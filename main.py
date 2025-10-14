import html
import os
import re
import time

import tqdm
from requests import Session

import configs
from db_ritual import RitualDb
from enums import MediaType
from utils import (
    ChanPost,
    ChanThread,
    create_thumbnail,
    download_file,
    get_filename,
    get_filepath,
    get_fs_filename_thumbnail,
    get_url_and_filename,
    make_path,
    match_sub_and_com,
    post_has_file,
    post_is_sticky,
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
        self.last_modified_filepath = make_path('cache', 'd_last_modified.json')
        self.last_modified: dict[str, dict[int, float]] = dict()

        self.loop = loop

        self.read()


    @property
    def ignore_last_modified(self) -> bool:
        return self.loop.is_first_loop and configs.ignore_last_modified


    def save(self):
        '''writes every cache'''
        write_json_obj_to_file(self.last_modified_filepath, self.last_modified)


    def read(self):
        '''reads in every cache'''
        self.last_modified = self._get_cached_d_last_modified()


    def _get_cached_d_last_modified(self):
        """{g: {123: 1717755968, 124: 1717755999}, ck: {456: 1717755968}, ...}"""
        last_modified = read_json(self.last_modified_filepath)

        if not last_modified:
            return dict()

        return {
            board: {int(tid): lm for tid, lm in tid_2_last_modified.items()}
            for board, tid_2_last_modified in last_modified.items()
        }


    def is_thread_modified(self, board: str, thread: dict) -> bool:
        """
        `True` indicates we should download the thread.
        """

        tid = thread['no']
        thread_last_modified = thread.get('last_modified')

        if board not in self.last_modified:
            self.last_modified[board] = dict()

        # should come before entry pruning
        thread_last_modified_cached = self.last_modified[board].get(tid)

        # Update the thread's last modified time in d_last_modified.
        self.last_modified[board][tid] = thread_last_modified

        # Don't let the dict grow over N entries per board.
        N = 200
        if (count := len(self.last_modified[board])) > N:
            # In case of multiple stickies, or similar special threads, we delete the extras, plus M oldest threads
            M = count - N + 10
            temp_ids = sorted(self.last_modified[board], key=lambda tid: self.last_modified[board][tid])
            for stale_id in temp_ids[:M]:
                del self.last_modified[board][stale_id] # prune

        # last_modified changed
        if thread_last_modified and thread_last_modified_cached and thread_last_modified != thread_last_modified_cached:
            return True

        # new thread
        if thread_last_modified_cached is None:
            return True

        return False


class Fetcher:
    def __init__(self):
        self.session: Session = Session()

    def fetch_json(self, url, headers=None, request_cooldown_sec: float=None, add_random: bool=False) -> dict:
        resp = self.session.get(url, headers=headers)

        if request_cooldown_sec:
            sleep(request_cooldown_sec, add_random=add_random)

        if resp.status_code == 200:
            return resp.json()

        return dict()

    def sleep(self):
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
            configs.logger.info(f'[{self.board}] Catalog empty')
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
    def __init__(self, db: RitualDb, fetcher: Fetcher, board: str, tid_2_thread: dict[int, dict]):
        self.db = db
        self.fetcher = fetcher
        self.board = board
        self.tid_2_thread = tid_2_thread
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
        '''

        # TODO: add better support for marking OPs as deleted

        pids_deleted = []
        tids_deleted = []

        for tid in self.tid_2_thread:
            thread = self.fetcher.fetch_json(
                configs.url_thread.format(board=self.board, thread_id=tid),
                headers=configs.headers,
                request_cooldown_sec=configs.request_cooldown_sec,
                add_random=configs.add_random,
            )

            if not thread:
                configs.logger.info(f'[{self.board}] Lost Thread [{tid}]')
                tids_deleted.append(tid)
                continue

            configs.logger.info(f'[{self.board}] Found thread [{tid}]')

            self.validate_posts(thread['posts'])

            # TODO: one query versus len(self.tid_2_thread) queries

            pids_found = {post['no'] for post in thread['posts']}
            pids_all = self.db.get_pids_by_tid(self.board, tid)
            for pid in pids_all:
                if pid not in pids_found:
                    pids_deleted.append(pid)
                    configs.logger.info(f'[{self.board}] Post Deleted [{tid}] [{pid}]')

            self.tid_2_posts[tid] = thread['posts']

        if pids_deleted:
            self.db.set_posts_deleted(self.board, pids_deleted)

        if tids_deleted:
            self.db.set_threads_deleted(self.board, tids_deleted)

        self.set_pid_2_post()


    def set_pid_2_post(self):
        for posts in self.tid_2_posts.values():
            for post in posts:
                self.pid_2_post[post['no']] = post


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

    def set_tid_2_posts(self, tid_2_posts: dict[int, list[dict]]):
        self.tid_2_posts = tid_2_posts


    def filter_catalog(self, catalog: Catalog) -> dict[int, dict]:
        '''
        Fitlers based on,
        - sticky post
        - title
        - comments
        - last modified time
        '''
        not_modified_thread_count = 0

        for page in catalog.catalog:
            for thread in page['threads']:
                tid = thread['no']

                if post_is_sticky(thread):
                    continue

                subject = html.unescape(thread.get('sub', ''))
                comment = html.unescape(thread.get('com', ''))

                if not self.should_archive(subject, comment):
                    continue

                if self.state.ignore_last_modified:
                    self.tid_2_thread[tid] = thread
                    self.state.is_thread_modified(self.board, thread)
                    continue

                if not self.state.is_thread_modified(self.board, thread):
                    not_modified_thread_count += 1
                    continue

                self.tid_2_thread[tid] = thread

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


    def get_pids_for_download(self):
        make_thumbnails = configs.make_thumbnails
        dl_full_media = configs.boards[self.board].get('dl_full_media')
        dl_full_media_op = configs.boards[self.board].get('dl_full_media_op')
        dl_thumbs_op = configs.boards[self.board].get('dl_thumbs_op')
        dl_thumbs = configs.boards[self.board].get('dl_thumbs')

        def is_needed(post: dict, pattern: str, media_type: MediaType) -> bool:
            if isinstance(pattern, str) and not match_sub_and_com(post, pattern):
                return False

            filename = get_filename(post, media_type)
            filepath = get_filepath(configs.media_save_path, self.board, media_type, filename)

            # os.path.isfile is cheap, no need for a cache
            if os.path.isfile(filepath):
                return False
            
            return True

        for tid, posts in self.tid_2_posts.items():            
            for post in posts:
                if not post_has_file(post):
                    continue

                if tid == (pid := post['no']):
                    if dl_full_media_op:
                        if is_needed(post, dl_full_media_op, MediaType.full_media):
                            self.full_pids.add(pid)

                    # "if we're not making OP thumbs [with Convert or FFMPEG] from OP full media"
                    if not (make_thumbnails and dl_full_media_op):
                        if dl_thumbs_op:
                            if is_needed(post, dl_thumbs_op, MediaType.thumbnail):
                                self.thumb_pids.add(pid)
                else:
                    if dl_full_media:
                        if is_needed(post, dl_full_media, MediaType.full_media):
                            self.full_pids.add(pid)

                    # "if we're not making thumbs [with Convert or FFMPEG] from full media"
                    if not (make_thumbnails and dl_full_media):
                        if dl_thumbs:
                            if is_needed(post, dl_thumbs, MediaType.thumbnail):
                                self.thumb_pids.add(pid)


    def download_media(self, tid_2_posts: dict[int, list[dict]], pid_2_post: dict[int, dict]):
        self.set_tid_2_posts(tid_2_posts)
        self.get_pids_for_download()

        for pid in self.full_pids:
            self._download_post_file(pid_2_post[pid], MediaType.full_media)

        for pid in self.thumb_pids:
            self._download_post_file(pid_2_post[pid], MediaType.thumbnail)


    def _download_post_file(self, post: dict, media_type: MediaType) -> bool:
        url, filename = get_url_and_filename(configs, self.board, post, media_type)
        filepath = get_filepath(configs.media_save_path, self.board, media_type, filename)

        is_success = download_file(
            url,
            filepath,
            video_cooldown_sec=configs.video_cooldown_sec,
            image_cooldown_sec=configs.image_cooldown_sec,
            add_random=configs.add_random,
            headers=configs.headers,
            logger=configs.logger,
            session=self.fetcher.session,
        )

        if not is_success:
            configs.logger.info(f'[{self.board}] Failed to download [{media_type.value}] {filepath}')
            return False

        configs.logger.info(f'[{self.board}] Downloaded [{media_type.value}] {filepath}')
        if media_type == MediaType.full_media and configs.make_thumbnails:
            thumb_path = get_filepath(configs.media_save_path, self.board, MediaType.thumbnail, get_fs_filename_thumbnail(post))
            sleep(0.1, add_random=configs.add_random)
            create_thumbnail(post, filepath, thumb_path, logger=configs.logger)


def main():
    Init()
    db = RitualDb(configs.db_path)
    fetcher = Fetcher()
    loop = Loop()
    state = State(loop)

    while True:
        for board in tqdm.tqdm(configs.boards, disable=configs.disable_tqdm):
            loop.set_start_time()

            catalog = Catalog(fetcher, board)
            if not catalog.fetch_catalog():
                continue

            filter = Filter(fetcher, db, board, state)
            filter.filter_catalog(catalog)

            posts = Posts(db, fetcher, board, filter.tid_2_thread)
            posts.fetch_posts()
            posts.save_posts()

            filter.download_media(posts.tid_2_posts, posts.pid_2_post)

            loop.set_board_duration_minutes(board)

        fetcher.sleep()
        state.save()

        loop.increment_loop()
        loop.log_board_durations()
        loop.sleep()


if __name__=='__main__':
    main()
