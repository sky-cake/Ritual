import os
import re
import time
import traceback

import msgspec
from requests import Session, JSONDecodeError

import configs
from db_ritual import RitualDb, create_ritual_db
from enums import DeletionType, MediaType
from utils import (
    ChanPost,
    ChanThread,
    create_thumbnail,
    download_file,
    extract_text_from_html,
    fetch_and_save_boards_json,
    get_filename,
    get_filepath,
    get_fs_filename_full_media,
    get_fs_filename_thumbnail,
    get_url_and_filename,
    load_boards_with_archive,
    make_path,
    fullmatch_sub_and_com,
    post_has_file,
    read_json,
    sleep,
    test_deps,
    write_json_obj_to_file,
)


class Init:
    def __init__(self):
        if configs.make_thumbnails:
            test_deps(configs.logger)

        boards_json_path = make_path('cache', 'boards.json')
        if os.path.isfile(boards_json_path):
            boards_json = read_json(boards_json_path)
            configs.logger.info(f'Loaded boards.json from {boards_json_path}')
        else:
            boards_json = fetch_and_save_boards_json(boards_json_path, configs.url_boards, configs.logger)

        configs.boards_with_archive = load_boards_with_archive(boards_json)

        if not configs.boards_with_archive:
            raise ValueError(configs.boards_with_archive)

        configs.logger.info(f'{len(configs.boards_with_archive)} boards have archive support')


class Loop:
    '''Loop mechanisms, and stats.'''
    def __init__(self):
        self.loop_i: int = 1
        self.start_time: float | None = None
        self.board_2_duration: dict[str, float] = dict()
        configs.logger.info(f'Loop #{self.loop_i} Started')

    @property
    def is_first_loop(self) -> bool:
        return self.loop_i == 1

    def set_start_time(self):
        self.start_time = time.time()

    def get_duration_minutes(self) -> float:
        return round((time.time() - self.start_time) / 60, 2)

    def set_board_duration_minutes(self, board: str):
        self.board_2_duration[board] = self.get_duration_minutes()

    def log_board_durations(self):
        s = 'Duration for each board:\n'

        for board, duration in self.board_2_duration.items():
            s += f'    - {board:<4} {duration:.1f}m\n'

        total_duration = round(sum(self.board_2_duration.values()), 1)
        s += f'Total Duration: {total_duration}m\n'
        configs.logger.info(s)

    def increment_loop(self):
        configs.logger.info(f'Loop #{self.loop_i} Completed\n')
        self.loop_i += 1

    def sleep(self):
        configs.logger.info(f'Doing loop cooldown sleep for {configs.loop_cooldown_sec}s\n')
        time.sleep(configs.loop_cooldown_sec)


class State:
    def __init__(self, loop: Loop):
        """
        Manages persistent state across scraper runs.

        - thread_cache: Maps board -> thread_id -> last_modified timestamp from catalog JSON.
        - http_cache: Maps URL -> HTTP Last-Modified header string for conditional requests.
        - thread_stats: Maps board -> thread_id -> stats dict (replies, images, most_recent_reply_no).
        - thread_meta: Maps board -> thread_id -> (page, bump_time, hit_bump_limit) for deletion detection.
        """
        self.thread_cache_filepath = make_path('cache', 'thread_cache.json')
        self.thread_cache: dict[str, dict[int, float]] = dict()

        self.http_cache_filepath = make_path('cache', 'http_cache.json')
        self.http_cache: dict[str, str] = dict()

        self.thread_stats_filepath = make_path('cache', 'thread_stats.json')
        self.thread_stats: dict[str, dict[int, dict]] = dict()

        self.thread_meta_filepath = make_path('cache', 'thread_meta.json')
        self.thread_meta: dict[str, dict[int, list]] = dict()

        self.loop = loop

        self.read()

    @property
    def ignore_last_modified(self) -> bool:
        return self.loop.is_first_loop and configs.ignore_thread_cache

    def save(self):
        '''writes every cache'''
        try:
            write_json_obj_to_file(self.thread_cache_filepath, self.thread_cache)
            write_json_obj_to_file(self.http_cache_filepath, self.http_cache)
            write_json_obj_to_file(self.thread_stats_filepath, self.thread_stats)
            write_json_obj_to_file(self.thread_meta_filepath, self.thread_meta)
        except Exception as e:
            configs.logger.error(f'Failed to save state: {e}')
            configs.logger.error(traceback.format_exc())
            raise e

    def read(self):
        '''reads in every cache'''
        self.thread_cache = self.get_cached_thread_cache()
        self.http_cache = read_json(self.http_cache_filepath) or dict()
        self.thread_stats = self.get_cached_thread_stats()
        self.thread_meta = self.get_cached_thread_meta()

    def get_cached_thread_cache(self) -> dict[str, dict[int, float]]:
        """{g: {123: 1717755968, 124: 1717755999}, ck: {456: 1717755968}, ...}"""
        thread_cache = read_json(self.thread_cache_filepath)

        if not thread_cache:
            return dict()

        return {
            board: {int(tid): lm for tid, lm in tid_2_last_modified.items()}
            for board, tid_2_last_modified in thread_cache.items()
        }

    def get_cached_thread_stats(self) -> dict[str, dict[int, dict]]:
        """{g: {123: {replies: 10, images: 5, most_recent_reply_no: 456}, ...}, ...}"""
        thread_stats = read_json(self.thread_stats_filepath)

        if not thread_stats:
            return dict()

        return {
            board: {int(tid): stats for tid, stats in tid_2_stats.items()}
            for board, tid_2_stats in thread_stats.items()
        }

    def get_cached_thread_meta(self) -> dict[str, dict[int, list]]:
        """{g: {123: [page, bump_time, hit_bump_limit], ...}, ...}"""
        thread_meta = read_json(self.thread_meta_filepath)

        if not thread_meta:
            return dict()

        return {
            board: {int(tid): meta for tid, meta in tid_2_meta.items()}
            for board, tid_2_meta in thread_meta.items()
        }

    def prune_old_threads(self, board: str):
        # Don't let the dict grow over N entries per board.
        if board not in self.thread_cache:
            return

        N = 200
        board_cache = self.thread_cache[board]
        if (count := len(board_cache)) > N:
            # In case of multiple stickies, or similar special threads, we delete the extras, plus M oldest threads
            M = count - N + 10
            tid_timestamp_pairs = [(tid, board_cache.get(tid) or 0.0) for tid in board_cache.keys()]
            tid_timestamp_pairs.sort(key=self.get_timestamp_from_pair)
            for stale_id, _ in tid_timestamp_pairs[:M]:
                del board_cache[stale_id] # prune

    def get_timestamp_from_pair(self, pair: tuple[int, float]) -> float:
        return pair[1]

    def is_thread_modified_cache_update(self, board: str, thread: dict) -> bool:
        """
        `True` indicates we should download the thread.
        """

        tid = thread['no']
        thread_last_modified = thread.get('last_modified')

        if board not in self.thread_cache:
            self.thread_cache[board] = dict()

        # should come before entry pruning
        thread_last_modified_cached = self.thread_cache[board].get(tid)

        # last_modified changed
        if thread_last_modified and thread_last_modified_cached and thread_last_modified != thread_last_modified_cached:
            # Update the thread's last modified time in thread_cache.
            self.thread_cache[board][tid] = thread_last_modified
            return True

        # new thread
        if thread_last_modified_cached is None:
            # Update the thread's last modified time in thread_cache.
            self.thread_cache[board][tid] = thread_last_modified
            return True

        # Update the thread's last modified time even if unchanged
        self.thread_cache[board][tid] = thread_last_modified

        return False

    def get_thread_stats(self, board: str, tid: int) -> dict | None:
        if board not in self.thread_stats:
            return None
        return self.thread_stats[board].get(tid)

    def set_thread_stats(self, board: str, tid: int, replies: int | None, images: int | None, most_recent_reply_no: int | None):
        if board not in self.thread_stats:
            self.thread_stats[board] = dict()
        if tid not in self.thread_stats[board]:
            self.thread_stats[board][tid] = dict()
        if replies is not None:
            self.thread_stats[board][tid]['replies'] = replies
        if images is not None:
            self.thread_stats[board][tid]['images'] = images
        if most_recent_reply_no is not None:
            self.thread_stats[board][tid]['most_recent_reply_no'] = most_recent_reply_no

        self.prune_old_thread_stats(board)

    def prune_old_thread_stats(self, board: str):
        """don't let the dict grow over N entries per board."""
        if board not in self.thread_stats:
            return

        N = 200
        board_stats = self.thread_stats[board]
        if (count := len(board_stats)) > N:
            # delete the oldest threads based on most_recent_reply_no
            M = count - N + 10
            tid_reply_pairs = [(tid, stats.get('most_recent_reply_no', 0) or 0) for tid, stats in board_stats.items()]
            tid_reply_pairs.sort(key=lambda pair: pair[1])
            for stale_id, _ in tid_reply_pairs[:M]:
                del board_stats[stale_id]

    def get_http_last_modified(self, url: str) -> str | None:
        return self.http_cache.get(url)

    def set_http_last_modified(self, url: str, last_modified: str | None):
        if last_modified:
            self.http_cache[url] = last_modified
            if len(self.http_cache) > 500:
                oldest_key = next(iter(self.http_cache))
                del self.http_cache[oldest_key]
        elif url in self.http_cache:
            del self.http_cache[url]

    def get_thread_url_last_modified(self, board: str, tid: int) -> str | None:
        url = configs.url_thread.format(board=board, thread_id=tid)
        return self.get_http_last_modified(url)

    def update_thread_meta(self, board: str, tid_2_page: dict[int, int], tid_2_thread: dict[int, dict]):
        """update page positions, bump times, and bump limit status from catalog data."""
        if board not in self.thread_meta:
            self.thread_meta[board] = dict()

        for tid, page in tid_2_page.items():
            thread = tid_2_thread.get(tid, dict())
            bump_time = thread.get('last_modified', thread.get('time', 0))
            hit_bump_limit = bool(thread.get('bumplimit', 0))
            self.thread_meta[board][tid] = [page, bump_time, hit_bump_limit]

        self.prune_old_thread_meta(board)

    def prune_old_thread_meta(self, board: str):
        """don't let the dict grow over N entries per board."""
        if board not in self.thread_meta:
            return

        N = 200
        board_meta = self.thread_meta[board]
        if (count := len(board_meta)) > N:
            # delete the oldest threads based on bump_time (index 1 in the list)
            M = count - N + 10
            tid_bump_pairs = [(tid, meta[1] if meta and len(meta) > 1 else 0) for tid, meta in board_meta.items()]
            tid_bump_pairs.sort(key=lambda pair: pair[1])
            for stale_id, _ in tid_bump_pairs[:M]:
                del board_meta[stale_id]

    def get_thread_meta(self, board: str, tid: int) -> list | None:
        """returns [page, bump_time, hit_bump_limit] or None if not tracked."""
        if board not in self.thread_meta:
            return
        return self.thread_meta[board].get(tid)

    def remove_thread_meta(self, board: str, tid: int):
        """remove thread from tracking after deletion/archive/prune."""
        if board in self.thread_meta and tid in self.thread_meta[board]:
            del self.thread_meta[board][tid]


class Fetcher:
    def __init__(self, state: State | None = None):
        self.session: Session = Session()
        self.state = state

    def fetch_json(self, url, headers=None, request_cooldown_sec: float=None, add_random: bool=False) -> dict | None:
        request_headers = dict(headers) if headers else dict()

        if not configs.ignore_http_cache and self.state:
            last_modified = self.state.get_http_last_modified(url)
            if last_modified:
                request_headers['If-Modified-Since'] = last_modified

        resp = self.session.get(url, headers=request_headers, timeout=10)

        if request_cooldown_sec:
            sleep(request_cooldown_sec, add_random=add_random)

        if resp.status_code == 304:
            if not configs.ignore_http_cache and self.state:
                last_modified_header = resp.headers.get('Last-Modified')
                if last_modified_header:
                    self.state.set_http_last_modified(url, last_modified_header)
            configs.logger.warning(f'Not modified (304) {url}')
            return dict()

        if resp.status_code == 200:
            if not configs.ignore_http_cache and self.state:
                last_modified_header = resp.headers.get('Last-Modified')
                if last_modified_header:
                    self.state.set_http_last_modified(url, last_modified_header)
            try:
                return resp.json()
            except JSONDecodeError:
                configs.logger.warning(f'Failed to parse JSON (200) {url}')
                return dict()

        configs.logger.warning(f'Failed to get JSON ({resp.status_code}) {url}')
        return dict()

    def sleep(self):
        # Refresh session periodically to prevent stale connections
        # Also refresh if loop cooldown is long enough to warrant it
        if configs.loop_cooldown_sec >= 15.0:
            self.session.close()
            self.session = Session()


class Archive:
    def __init__(self, fetcher: Fetcher, board: str):
        self.fetcher = fetcher
        self.board = board
        self.has_archive: bool = board in configs.boards_with_archive
        self.archived_tids: set[int] | None = None
        self.fetching_archive_failed: bool = False

    def board_supports_archive(self) -> bool:
        return self.has_archive and not self.fetching_archive_failed

    def is_archived(self, tid: int) -> bool:
        """
        Lazy-fetches archive.json on first call and keeps results for other threads.
        """
        # avoid boards with no archive support
        if not self.board_supports_archive():
            return False

        if self.archived_tids is None:
            self.fetch_and_set_archive()

        if self.archived_tids is None:
            return False

        return tid in self.archived_tids

    def fetch_and_set_archive(self):
        url = configs.url_archive.format(board=self.board)
        configs.logger.info(f'[{self.board}] Fetching archive.json')
        data = self.fetcher.fetch_json(
            url,
            headers=configs.headers,
            request_cooldown_sec=configs.request_cooldown_sec,
            add_random=configs.add_random,
        )

        if not data or not isinstance(data, list):
            self.fetching_archive_failed = True
            configs.logger.info(f'[{self.board}] archive.json not available or empty')
            return

        self.archived_tids = set(data)
        configs.logger.info(f'[{self.board}] Loaded {len(self.archived_tids)} archived thread IDs')


class Catalog:
    def __init__(self, fetcher: Fetcher, board: str):
        self.fetcher = fetcher

        self.board: str = board
        self.catalog: list[dict] = []
        self.tid_2_thread: dict[int, dict] = dict()
        self.tid_2_page: dict[int, int] = dict()
        self.tid_2_last_replies: dict[int, list[dict]] = dict()


    def validate_threads(self):
        for thread in self.tid_2_thread.values():
            msgspec.convert(thread, ChanThread)


    def fetch_catalog(self) -> bool:
        '''
        - fetches catalog from api
        - validates catalog from api
        - returns `True` if successful
        '''
        self.catalog = self.fetcher.fetch_json(
            configs.url_catalog.format(board=self.board),
            headers=configs.headers,
            request_cooldown_sec=configs.request_cooldown_sec,
            add_random=configs.add_random,
        )

        configs.logger.info(f'[{self.board}] Downloaded catalog')

        if not self.catalog:
            configs.logger.warning(f'[{self.board}] Catalog empty {self.catalog}')
            return False

        self.set_tid_2_thread()
        self.validate_threads()

        self.set_tid_2_last_replies()

        return True


    def set_tid_2_thread(self):
        page_i = 1
        for page in self.catalog:
            page_num = page.get('page', page_i)
            page_i += 1
            for thread in page['threads']:
                tid = thread['no']
                self.tid_2_thread[tid] = thread
                self.tid_2_page[tid] = page_num


    def set_tid_2_last_replies(self):
        for page in self.catalog:
            for thread in page['threads']:
                if last_replies := thread.get('last_replies'):
                    self.tid_2_last_replies[thread['no']] = last_replies


class Posts:
    def __init__(self, db: RitualDb, fetcher: Fetcher, board: str, tid_2_thread: dict[int, dict], state: State, catalog: 'Catalog'):
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
        catalog_tids = set(self.tid_2_thread.keys())
        existing_tids = self.db.get_recently_active_thread_ids(self.board) | set(self.state.thread_meta.get(self.board, {}).keys())
        missing_tids = existing_tids - catalog_tids

        # assumes threads don't disappear from the catalog, then return
        # missing_tids get removed from self.state after db writes
        for tid in missing_tids:
            deletion_type = self.classify_missing_thread(tid, archive)

            if deletion_type == DeletionType.archived:
                configs.logger.info(f'[{self.board}] Thread [{tid}] archived')
                tids_archived.append(tid)
            elif deletion_type == DeletionType.deleted:
                configs.logger.info(f'[{self.board}] Thread [{tid}] deleted by moderator')
                tids_deleted.append(tid)
            elif deletion_type == DeletionType.pruned:
                # not marked as deleted, this is the natural lifespan of a thread
                configs.logger.info(f'[{self.board}] Thread [{tid}] pruned')
            else:
                configs.logger.info(f'[{self.board}] Thread [{tid}] deleted for unknown reason')

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
            for pid in pids_all:
                if pid not in pids_found:
                    pids_deleted.append(pid)
                    configs.logger.info(f'[{self.board}] Post Deleted [{tid}] [{pid}]')

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
        A missing thread is `probably_deleted` if it was,

            - recently bumped  (`config.thread_delete_bump_age_hours`)
            - on an early page (`config.thread_delete_page_threshold`)
            - has not hit bump limit 

        - if not `probably_deleted` -> pruned
        - if `probably_deleted` AND board has archive -> check archive.json
        - if in archive -> archived, else -> deleted
        """
        meta = self.state.get_thread_meta(self.board, tid)
        if not meta:
            return DeletionType.pruned

        page, bump_time, hit_bump_limit = meta

        recently_bumped = False
        if bump_time:
            hours_since_bump = (time.time() - bump_time) / 3600
            recently_bumped = hours_since_bump < configs.thread_delete_bump_age_hours

        # page is either None or gte 1
        on_early_page = page and page < configs.thread_delete_page_threshold
        probably_deleted = recently_bumped and on_early_page and not hit_bump_limit

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


class Filter:
    """
    - 'thread' are OP posts from the catalog, https://github.com/4chan/4chan-API/blob/master/pages/Catalog.md
    - 'post' are posts from threads, https://github.com/4chan/4chan-API/blob/master/pages/Threads.md
    """
    def __init__(self, fetcher: Fetcher, db: RitualDb, board: str, state: State):
        self.fetcher = fetcher
        self.db = db
        self.board = board
        self.state = state

        self.tid_2_thread: dict[int, dict] = dict()
        self.tid_2_posts: dict[int, list[dict]] = dict()

        self.full_pids: set[int] = set()
        self.thumb_pids: set[int] = set()

        self.md5_2_media_filename: dict[str, str] = dict()
        self.banned_hashes: set[str] = set()

    def set_tid_2_posts(self, tid_2_posts: dict[int, list[dict]]):
        self.tid_2_posts = tid_2_posts


    def filter_catalog(self, catalog: Catalog) -> dict[int, dict]:
        '''
        Filters based on,
        - title
        - comments
        - last modified time
        '''
        not_modified_thread_count = 0

        for page in catalog.catalog:
            for thread in page['threads']:
                tid = thread['no']

                subject = thread.get('sub', '')
                comment = thread.get('com', '')

                subject_text = extract_text_from_html(subject)
                comment_text = extract_text_from_html(comment)

                if not self.should_archive(subject_text, comment_text):
                    continue

                if self.state.ignore_last_modified:
                    self.tid_2_thread[tid] = thread
                    self.state.is_thread_modified_cache_update(self.board, thread)
                    continue

                if not self.state.is_thread_modified_cache_update(self.board, thread):
                    not_modified_thread_count += 1
                    continue

                self.tid_2_thread[tid] = thread

        self.state.prune_old_threads(self.board)

        msg = f'{not_modified_thread_count} thread(s) are unmodified. ' if not_modified_thread_count else ''
        if self.state.ignore_last_modified:
            msg = 'Ignoring last modified timestamps on first loop. '

        configs.logger.info(f'[{self.board}] {msg}{len(self.tid_2_thread)} thread(s) are modified and will be queued.')


    def should_archive(self, subject: str, comment: str, whitelist: str=None, blacklist: str=None):
        """
        - If a post is blacklisted and whitelisted, it will not be archived - blacklisted filters take precedence over whitelisted filters.
        - If only a blacklist is specified, skip blacklisted posts, and archive everything else.
        - If only a whitelist is specified, archive whitelisted posts, and skip everything else.
        - If no lists are specified, archive everything.
        """
        op_comment_min_chars = configs.boards[self.board].get('op_comment_min_chars')
        if op_comment_min_chars and len(comment) < op_comment_min_chars:
            return False

        op_comment_min_chars_unique = configs.boards[self.board].get('op_comment_min_chars_unique')
        if op_comment_min_chars_unique and len(set(comment)) < op_comment_min_chars_unique:
            return False

        blacklist_post_filter = configs.boards[self.board].get('blacklist') if blacklist is None else blacklist
        if blacklist_post_filter:
            if subject and re.search(blacklist_post_filter, subject, re.IGNORECASE):
                return False
            if comment and re.search(blacklist_post_filter, comment, re.IGNORECASE):
                return False

        whitelist_post_filter = configs.boards[self.board].get('whitelist') if whitelist is None else whitelist
        if whitelist_post_filter:
            if subject and re.search(whitelist_post_filter, subject, re.IGNORECASE):
                return True
            if comment and re.search(whitelist_post_filter, comment, re.IGNORECASE):
                return True
            return False

        return True
    

    def is_media_needed_simple(self, post: dict, pattern_or_bool: str | bool) -> bool:
        """Only based on config rules."""
        if isinstance(pattern_or_bool, bool):
            return pattern_or_bool

        if isinstance(pattern_or_bool, str):
            return fullmatch_sub_and_com(post, pattern_or_bool)

        return False


    def is_media_needed(self, post: dict, pattern_or_bool: str | bool, media_type: MediaType) -> bool:
        """
        - Post text does not match pattern    -> skip download
        - Hash banned                         -> skip download
        - Hash archived + stored file exists  -> skip download
        - Hash archived + stored file missing -> download
        - Hash not archived + file missing    -> download
        - Hash not archived + file exists     -> skip download
        """
        if not self.is_media_needed_simple(post, pattern_or_bool):
            return False

        if media_type == MediaType.full_media:
            media_hash = post.get('md5')
            if media_hash:
                if media_hash in self.banned_hashes:
                    return False

                if configs.skip_duplicate_files and media_hash in self.md5_2_media_filename:
                    stored_filepath = get_filepath(
                        configs.media_save_path,
                        self.board,
                        media_type,
                        self.md5_2_media_filename[media_hash], # stored filename
                    )
                    if os.path.isfile(stored_filepath):
                        return False

        filename = get_filename(post, media_type)
        filepath = get_filepath(configs.media_save_path, self.board, media_type, filename)

        if os.path.isfile(filepath):
            return False

        return True


    def get_pids_for_download(self):
        make_thumbnails = configs.make_thumbnails

        dl_fm_op = configs.boards[self.board].get('dl_fm_op')
        dl_fm_post = configs.boards[self.board].get('dl_fm_post')
        dl_fm_thread = configs.boards[self.board].get('dl_fm_thread')

        dl_th_op = configs.boards[self.board].get('dl_th_op')
        dl_th_post = configs.boards[self.board].get('dl_th_post')
        dl_th_thread = configs.boards[self.board].get('dl_th_thread')

        media_hashes = []
        for posts in self.tid_2_posts.values():
            for post in posts:
                if post_has_file(post) and post.get('md5'):
                    media_hashes.append(post['md5'])

        # query still needed for checking banned media hashes
        self.md5_2_media_filename, self.banned_hashes = self.db.get_media_hash_info(self.board, media_hashes)
        if not configs.skip_duplicate_files:
            self.md5_2_media_filename = dict()

        for tid, posts in self.tid_2_posts.items():
            should_dl_fm_thread = self.is_media_needed_simple(self.tid_2_thread[tid], dl_fm_thread)
            should_dl_th_thread = self.is_media_needed_simple(self.tid_2_thread[tid], dl_th_thread)

            for post in posts:
                if not post_has_file(post):
                    continue

                if tid == (pid := post['no']):
                    pattern_or_bool_full_media = dl_fm_op
                    pattern_or_bool_thumbs = dl_th_op
                else:
                    pattern_or_bool_full_media = dl_fm_post
                    pattern_or_bool_thumbs = dl_th_post

                if should_dl_fm_thread or self.is_media_needed(post, pattern_or_bool_full_media, MediaType.full_media):
                    self.full_pids.add(pid)

                if not make_thumbnails:
                    if should_dl_th_thread or self.is_media_needed(post, pattern_or_bool_thumbs, MediaType.thumbnail):
                        self.thumb_pids.add(pid)


    def download_media(self, tid_2_posts: dict[int, list[dict]], pid_2_post: dict[int, dict]):
        self.set_tid_2_posts(tid_2_posts)
        self.get_pids_for_download()

        for pid in self.full_pids:
            if pid not in pid_2_post:
                configs.logger.info(f'[{self.board}] Post {pid} not found in pid_2_post, skipping full media download')
                continue
            self.download_post_file(pid_2_post[pid], MediaType.full_media)

        for pid in self.thumb_pids:
            if pid not in pid_2_post:
                configs.logger.info(f'[{self.board}] Post {pid} not found in pid_2_post, skipping thumbnail download')
                continue
            self.download_post_file(pid_2_post[pid], MediaType.thumbnail)


    def download_post_file(self, post: dict, media_type: MediaType) -> bool:
        url, filename = get_url_and_filename(configs, self.board, post, media_type)
        filepath = get_filepath(configs.media_save_path, self.board, media_type, filename)

        if os.path.isfile(filepath):
            return True

        expected_size = post.get('fsize') if media_type == MediaType.full_media else None
        expected_md5 = post.get('md5') if media_type == MediaType.full_media else None

        is_success = download_file(
            url,
            filepath,
            video_cooldown_sec=configs.video_cooldown_sec,
            image_cooldown_sec=configs.image_cooldown_sec,
            headers=configs.headers,
            logger=configs.logger,
            session=self.fetcher.session,
            expected_size=expected_size,
            expected_md5=expected_md5,
            download_files_with_mismatched_md5=configs.download_files_with_mismatched_md5,
        )

        if not is_success:
            return False

        configs.logger.info(f'[{self.board}] Downloaded [{media_type.value}] {filepath}')

        if media_type == MediaType.full_media:
            media_hash = post.get('md5')
            if media_hash:
                media = get_fs_filename_full_media(post)
                self.db.upsert_image(self.board, media_hash, media)

            if configs.make_thumbnails:
                thumb_path = get_filepath(configs.media_save_path, self.board, MediaType.thumbnail, get_fs_filename_thumbnail(post))
                sleep(0.1, add_random=configs.add_random)
                create_thumbnail(post, filepath, thumb_path, logger=configs.logger)


def process_board(board: str, db: RitualDb, fetcher: Fetcher, loop: Loop, state: State):
    loop.set_start_time()

    catalog = Catalog(fetcher, board)
    if not catalog.fetch_catalog():
        return

    state.update_thread_meta(board, catalog.tid_2_page, catalog.tid_2_thread)

    # results in a max of one archive.json endpoint fetch per loop
    archive = Archive(fetcher, board)

    filter = Filter(fetcher, db, board, state)
    filter.filter_catalog(catalog)

    posts = Posts(db, fetcher, board, filter.tid_2_thread, state, catalog)
    posts.fetch_posts(archive)

    if configs.boards[board].get('thread_text') != False:
        posts.save_posts()

    filter.download_media(posts.tid_2_posts, posts.pid_2_post)

    loop.set_board_duration_minutes(board)


def save_on_error(state: State, db: RitualDb):
    configs.logger.info('Saving state...')
    state.save()
    configs.logger.info('Done')
    configs.logger.info('Saving database...')
    db.save_and_close()
    configs.logger.info('Done')


def main():
    Init()
    db = create_ritual_db()
    loop = Loop()
    state = State(loop)
    fetcher = Fetcher(state)

    critical_error_count = 0
    while True:
        try:
            for board in configs.boards:
                process_board(board, db, fetcher, loop, state)

            fetcher.sleep()
            state.save()

            loop.increment_loop()
            loop.log_board_durations()
            loop.sleep()

        except KeyboardInterrupt:
            configs.logger.info('Received interrupt signal')
            save_on_error(state, db)
            break

        except Exception as e:
            configs.logger.error(f'Critical error in main loop: {e}')
            configs.logger.error(traceback.format_exc())
            save_on_error(state, db)
            critical_error_count += 1
            n_critical_errors = 5
            if critical_error_count >= n_critical_errors:
                configs.logger.error(f'Critical error count reached {n_critical_errors}, exiting...')
                break

            sleep_for = critical_error_count * 60
            configs.logger.info(f'Sleeping for {sleep_for}s, maybe the issue will resolve itself by then...')
            sleep(sleep_for)

    configs.logger.info('Exited while loop, ending program.')


if __name__=='__main__':
    main()
