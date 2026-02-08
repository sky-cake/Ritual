import os
import re
import time
import traceback

from requests import Session, JSONDecodeError

import configs
from db_ritual import RitualDb, create_ritual_db
from enums import MediaType
from utils import (
    ChanPost,
    ChanThread,
    create_thumbnail,
    download_file,
    extract_text_from_html,
    get_filename,
    get_filepath,
    get_fs_filename_full_media,
    get_fs_filename_thumbnail,
    get_url_and_filename,
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
        """
        self.thread_cache_filepath = make_path('cache', 'thread_cache.json')
        self.thread_cache: dict[str, dict[int, float]] = dict()

        self.http_cache_filepath = make_path('cache', 'http_cache.json')
        self.http_cache: dict[str, str] = dict()

        self.thread_stats_filepath = make_path('cache', 'thread_stats.json')
        self.thread_stats: dict[str, dict[int, dict]] = dict()

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
        except Exception as e:
            configs.logger.error(f'Failed to save state: {e}')
            configs.logger.error(traceback.format_exc())
            raise e

    def read(self):
        '''reads in every cache'''
        self.thread_cache = self.get_cached_thread_cache()
        self.http_cache = read_json(self.http_cache_filepath) or dict()
        self.thread_stats = read_json(self.thread_stats_filepath) or dict()

    def get_cached_thread_cache(self):
        """{g: {123: 1717755968, 124: 1717755999}, ck: {456: 1717755968}, ...}"""
        thread_cache = read_json(self.thread_cache_filepath)

        if not thread_cache:
            return dict()

        return {
            board: {int(tid): lm for tid, lm in tid_2_last_modified.items()}
            for board, tid_2_last_modified in thread_cache.items()
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
            configs.logger.warning(f'Not modified (304)')
            return dict()

        if resp.status_code == 200:
            if not configs.ignore_http_cache and self.state:
                last_modified_header = resp.headers.get('Last-Modified')
                if last_modified_header:
                    self.state.set_http_last_modified(url, last_modified_header)
            try:
                return resp.json()
            except JSONDecodeError:
                configs.logger.warning(f'Failed to parse JSON from {url}')
                return dict()

        configs.logger.warning(f'Failed to get JSON {resp.status_code=}')
        return dict()

    def sleep(self):
        # Refresh session periodically to prevent stale connections
        # Also refresh if loop cooldown is long enough to warrant it
        if configs.loop_cooldown_sec >= 15.0:
            self.session.close()
            self.session = Session()


class Catalog:
    def __init__(self, fetcher: Fetcher, board: str):
        self.fetcher = fetcher

        self.board: str = board
        self.catalog: list[dict] = []
        self.tid_2_thread: dict[int, dict] = dict()
        self.tid_2_last_replies: dict[int, list[dict]] = dict()


    def validate_threads(self):
        for thread in self.tid_2_thread.values():
            ChanThread(**thread)


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
        for page in self.catalog:
            for thread in page['threads']:
                self.tid_2_thread[thread['no']] = thread


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
            ChanPost(**post)


    def fetch_posts(self):
        '''
        - fetches posts from api
        - validates posts from api
        - marks deleted posts as deleted in the database
        - uses catalog-based incremental updates when possible
        '''

        pids_deleted = []
        tids_deleted = []
        catalog_update_count = 0
        full_fetch_count = 0

        for tid in self.tid_2_thread:
            thread_data = self.tid_2_thread[tid]
            thread_stats = self.state.get_thread_stats(self.board, tid)
            last_replies = self.catalog.tid_2_last_replies.get(tid)

            if self.can_use_catalog_update(thread_data, thread_stats, last_replies):
                posts_to_add = self.process_catalog_update(tid, last_replies, thread_stats)
                if posts_to_add:
                    catalog_update_count += 1
                    existing_pids = self.get_existing_posts_for_thread(tid)

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

            if thread is None:
                configs.logger.info(f'[{self.board}] Thread [{tid}] not modified (304)')
                continue

            if not thread:
                configs.logger.info(f'[{self.board}] Lost Thread [{tid}]')
                tids_deleted.append(tid)
                continue

            full_fetch_count += 1
            configs.logger.info(f'[{self.board}] Found thread [{tid}]')

            self.validate_posts(thread['posts'])

            pids_found = {post['no'] for post in thread['posts']}
            pids_all = self.db.get_pids_by_tid(self.board, tid)
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

        self.set_pid_2_post()

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
                ChanPost(**reply)
                posts_to_add.append(reply)
            except Exception as e:
                configs.logger.warning(f'[{self.board}] Invalid post in catalog update for thread [{tid}]: {e}')
                configs.logger.error(traceback.format_exc())
                raise e

        if posts_to_add:
            configs.logger.info(f'[{self.board}] Catalog update for thread [{tid}]: {len(posts_to_add)} new post(s)')

        return posts_to_add

    def get_existing_posts_for_thread(self, tid: int) -> set[int]:
        if tid in self.tid_2_posts:
            return {p['no'] for p in self.tid_2_posts[tid]}
        return set(self.db.get_pids_by_tid(self.board, tid))


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

    filter = Filter(fetcher, db, board, state)
    filter.filter_catalog(catalog)

    posts = Posts(db, fetcher, board, filter.tid_2_thread, state, catalog)
    posts.fetch_posts()

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
