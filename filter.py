import os
import re

import configs
from catalog import Catalog
from db.ritual import RitualDb
from enums import MediaType
from fetcher import Fetcher
from state import State
from utils import (
    create_thumbnail,
    download_file,
    extract_text_from_html,
    fullmatch_sub_and_com,
    get_filepath,
    get_asagi_value_media,
    get_url,
    post_has_file,
    sleep
)


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
        - Post text does not match pattern  -> skip download
        - Hash banned                       -> skip download
        - File missing                      -> download
        - File exists                       -> skip download
        """
        # post_has_file(post) is called in parent function, get_filepath() rules are followed

        if not self.is_media_needed_simple(post, pattern_or_bool):
            return False

        if media_type == MediaType.full_media:
            media_hash = post.get('md5')
            if media_hash:
                if media_hash in self.banned_hashes:
                    return False

        filepath = get_filepath(configs.media_save_path, self.board, media_type, post)
        return not os.path.isfile(filepath)


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

        self.banned_hashes = self.db.get_banned_hashes(self.board, media_hashes)

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
        url = get_url(configs, self.board, post, media_type)

        if not post_has_file(post):
            return False

        filepath = get_filepath(configs.media_save_path, self.board, media_type, post)

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
                media = get_asagi_value_media(post)
                self.db.upsert_image(self.board, media_hash, media)

            if configs.make_thumbnails:
                thumb_path = get_filepath(configs.media_save_path, self.board, MediaType.thumbnail, post)
                sleep(0.1, add_random=configs.add_random)
                create_thumbnail(post, filepath, thumb_path, logger=configs.logger)
