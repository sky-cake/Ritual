import time
import traceback

import msgspec

import configs
from archive import Archive
from catalog import Catalog
from db.ritual import RitualDb
from enums import DeletionType
from fetcher import Fetcher
from state import State
from utils import ChanPost


class Posts:
    def __init__(self, db: RitualDb, fetcher: Fetcher, board: str, tid_2_thread: dict[int, dict], state: State, catalog: Catalog):
        self.db = db
        self.fetcher = fetcher
        self.board = board
        self.tid_2_thread = tid_2_thread
        self.state = state
        self.catalog = catalog
        self.tid_2_posts: dict[int, list[dict]] = dict()
        self.pid_2_post: dict[int, dict] = dict()


    def validate_posts(self, posts: list[dict]):
        for post in posts:
            msgspec.convert(post, ChanPost)


    def fetch_posts(self, archive: Archive):
        '''
        - detects threads missing from catalog (deleted, pruned, or archived)
        - fetches posts from api
        - validates posts from api
        - marks deleted posts as deleted in the database
        - uses catalog-based incremental updates when possible
        '''

        pids_deleted = []
        tids_deleted = []
        tids_archived = []
        catalog_update_count = 0
        full_fetch_count = 0

        # prefetch existing pids for each thread in one query
        all_tids = list(self.tid_2_thread.keys())
        tid_2_existing_pids = self.db.get_tid_2_existing_pids(self.board, all_tids)

        # detect threads that have disappeared from the catalog
        # unfiltered threads
        catalog_tids = set(self.catalog.tid_2_thread.keys())
        existing_tids = self.db.get_recently_active_thread_ids(self.board) | set(self.state.thread_meta.get(self.board, {}).keys())
        missing_tids = existing_tids - catalog_tids if catalog_tids else set()

        # - assumes threads don't disappear from the catalog, then return
        # - missing_tids get removed from self.state after db writes
        for tid in missing_tids:
            deletion_type = self.classify_missing_thread(tid, archive)

            if deletion_type == DeletionType.archived:
                tids_archived.append(tid)
            elif deletion_type == DeletionType.deleted:
                tids_deleted.append(tid)

        if tids_archived: configs.logger.info(f'[{self.board}] Threads archived: {tids_archived}')
        if tids_deleted: configs.logger.info(f'[{self.board}] Threads deleted by moderator: {tids_deleted}')

        if missing_tids:
            configs.logger.info(f'[{self.board}] {len(missing_tids)} thread(s) no longer in catalog')

        for tid in self.tid_2_thread:
            thread_data = self.tid_2_thread[tid]
            thread_stats = self.state.get_thread_stats(self.board, tid)
            last_replies = self.catalog.tid_2_last_replies.get(tid)

            if self.can_use_catalog_update(thread_data, thread_stats, last_replies):
                posts_to_add = self.process_catalog_update(tid, last_replies, thread_stats)
                if posts_to_add:
                    catalog_update_count += 1
                    existing_pids = tid_2_existing_pids.get(tid, set())
                    if tid in self.tid_2_posts:
                        existing_pids = existing_pids | {p['no'] for p in self.tid_2_posts[tid]}

                    if tid not in self.tid_2_posts:
                        self.tid_2_posts[tid] = []

                    for post in posts_to_add:
                        if post['no'] not in existing_pids:
                            self.tid_2_posts[tid].append(post)

                    if self.tid_2_posts[tid]:
                        most_recent_reply_no = max(p['no'] for p in self.tid_2_posts[tid])
                    else:
                        most_recent_reply_no = thread_stats.get('most_recent_reply_no') if thread_stats else None

                    self.state.set_thread_stats(
                        self.board, tid,
                        replies=thread_data.get('replies'),
                        images=thread_data.get('images'),
                        most_recent_reply_no=most_recent_reply_no
                    )
                    self.save_thread_stats(tid)
                    continue

            url = configs.url_thread.format(board=self.board, thread_id=tid)
            thread = self.fetcher.fetch_json(
                url,
                headers=configs.headers,
                request_cooldown_sec=configs.request_cooldown_sec,
                add_random=configs.add_random,
            )

            if not thread:
                # we already log the issue in the fetch_json() call
                continue

            full_fetch_count += 1
            configs.logger.info(f'[{self.board}] Found thread [{tid}]')

            self.validate_posts(thread['posts'])

            pids_found = {post['no'] for post in thread['posts']}
            pids_all = tid_2_existing_pids.get(tid, set())
            pids_deleted = []
            for pid in pids_all:
                if pid not in pids_found:
                    pids_deleted.append(pid)

            if pids_deleted: configs.logger.info(f'[{self.board}] [{tid}] Posts deleted: {pids_deleted}')

            self.tid_2_posts[tid] = thread['posts']

            most_recent_reply_no = max((post['no'] for post in thread['posts']), default=None)
            self.state.set_thread_stats(
                self.board, tid,
                replies=thread_data.get('replies'),
                images=thread_data.get('images'),
                most_recent_reply_no=most_recent_reply_no
            )
            self.save_thread_stats(tid)

        if catalog_update_count > 0:
            configs.logger.info(f'[{self.board}] Updated {catalog_update_count} thread(s) using catalog data')
        if full_fetch_count > 0:
            configs.logger.info(f'[{self.board}] Fetched {full_fetch_count} thread(s) fully')

        if pids_deleted:
            self.db.set_posts_deleted(self.board, pids_deleted)

        if tids_deleted:
            self.db.set_threads_deleted(self.board, tids_deleted)

        if tids_archived:
            self.db.set_threads_archived(self.board, tids_archived)

        # remove thread metadata only after db writes
        for tid in missing_tids:
            self.state.remove_thread_meta(self.board, tid)

        self.set_pid_2_post()

    def classify_missing_thread(self, tid: int, archive: Archive) -> DeletionType:
        """
        A 404'd thread could be deleted if all three of these are true:

            - could be deleted if bumped within N minutes  (`config.not_deleted_if_bump_age_exceeds_n_min`)
            - could be deleted if on early pages           (`config.not_deleted_if_page_n_reached`)
            - could be deleted if at least N replies        (`config.not_deleted_if_n_replies`)

        - if not probably deleted -> pruned
        - if in archive -> archived
        - else -> deleted

        Note: If any random tid (44, -2, 1e9) is passed into this function, it is inconclusive.
        """
        meta = self.state.get_thread_meta(self.board, tid)

        if not meta:
            return DeletionType.inconclusive

        page, bump_time = meta
        if not (page and bump_time):
            return DeletionType.inconclusive

        thread_got_recent_attention = False
        if bump_time:
            minutes_since_bump = (time.time() - bump_time) / 60
            thread_got_recent_attention = minutes_since_bump < configs.not_deleted_if_bump_age_exceeds_n_min

        thread_stats = self.state.get_thread_stats(self.board, tid)
        replies = thread_stats.get('replies', -1) if thread_stats else -1
        if replies == -1:
            return DeletionType.inconclusive

        on_early_page = page < configs.not_deleted_if_page_n_reached
        thread_is_popular = replies >= configs.not_deleted_if_n_replies

        # for threads missing from catalog,
        probably_deleted = thread_got_recent_attention and on_early_page and not thread_is_popular

        if not probably_deleted:
            return DeletionType.pruned

        if archive.is_archived(tid):
            return DeletionType.archived

        return DeletionType.deleted

    def can_use_catalog_update(self, thread_data: dict, thread_stats: dict | None, last_replies: list[dict] | None) -> bool:
        if not last_replies or not isinstance(last_replies, list) or len(last_replies) == 0:
            return False

        if not thread_stats:
            return False

        if thread_stats.get('most_recent_reply_no') is None:
            return False

        current_replies = thread_data.get('replies', 0)
        cached_replies = thread_stats.get('replies', 0)

        if current_replies <= cached_replies:
            return False

        reply_diff = current_replies - cached_replies
        if reply_diff > len(last_replies):
            return False

        last_seen = thread_stats.get('most_recent_reply_no')
        has_last_seen = any(reply.get('no') == last_seen for reply in last_replies)

        if not has_last_seen:
            return False

        new_replies = [r for r in last_replies if r.get('no', 0) > last_seen]
        if len(new_replies) != reply_diff:
            return False

        return True

    def process_catalog_update(self, tid: int, last_replies: list[dict], thread_stats: dict) -> list[dict]:
        last_seen = thread_stats.get('most_recent_reply_no')
        new_replies = [r for r in last_replies if r.get('no', 0) > last_seen]

        posts_to_add = []
        for reply in new_replies:
            try:
                msgspec.convert(reply, ChanPost)
                posts_to_add.append(reply)
            except msgspec.ValidationError as e:
                configs.logger.warning(f'[{self.board}] Invalid post in catalog update for thread [{tid}]: {e}')
                configs.logger.error(traceback.format_exc())
                raise e

        if posts_to_add:
            configs.logger.info(f'[{self.board}] Catalog update for thread [{tid}]: {len(posts_to_add)} new post(s)')

        return posts_to_add


    def set_pid_2_post(self):
        for posts in self.tid_2_posts.values():
            for post in posts:
                self.pid_2_post[post['no']] = post

    def save_thread_stats(self, tid: int):
        thread_stats = self.state.get_thread_stats(self.board, tid)
        if not thread_stats:
            return

        nreplies = thread_stats.get('replies')
        nimages = thread_stats.get('images')
        if nreplies is None and nimages is None:
            return

        thread_data = self.tid_2_thread.get(tid, {})
        time_op = thread_data.get('time', 0)
        time_last = time_op

        posts = self.tid_2_posts.get(tid, [])
        if posts:
            timestamps = [post.get('time', 0) for post in posts if post.get('time')]
            if timestamps:
                time_last = max(timestamps)

        d = {
            'thread_num': tid,
            'time_op': time_op,
            'time_last': time_last,
            'time_bump': time_last,
            'time_ghost': None,
            'time_ghost_bump': None,
            'time_last_modified': thread_data.get('last_modified', 0),
            'nreplies': nreplies if nreplies is not None else 0,
            'nimages': nimages if nimages is not None else 0,
            'sticky': 1 if thread_data.get('sticky', 0) else 0,
            'locked': 1 if thread_data.get('closed', 0) else 0,
        }
        self.db.upsert_thread_stats(self.board, d)

    def save_posts(self):
        self.db.upsert_posts(self.board, self.pid_2_post.values())
