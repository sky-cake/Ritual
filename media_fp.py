import os
from abc import ABC, abstractmethod
from requests import Session

import configs
from enums import MediaType
from fetcher import Fetcher
from scanner.db_scanner import ScannerDb
from db.ritual import RitualDb
from utils import (
    create_thumbnail,
    fetch_media_bytes,
    get_media_url,
    sleep,
    get_md5_b64_hash,
    log_util,
    makedir_p,
)


def wrap_fetch_media_bytes(session: Session, url: str, ext: str) -> bytes | None:
    return fetch_media_bytes(
        url,
        ext,
        video_cooldown_sec=configs.video_cooldown_sec,
        image_cooldown_sec=configs.image_cooldown_sec,
        headers=configs.headers,
        logger=configs.logger,
        session=session,
        max_bytes=configs.fsize_upper_limit if (configs.enforce_fsize_upper_limit and configs.fsize_upper_limit) else None,
    )


class MediaFP(ABC):
    def __init__(self, fetcher: Fetcher, media_save_path: str, ritual_db: RitualDb):
        self.fetcher = fetcher
        self.media_save_path = media_save_path
        self.ritual_db = ritual_db
        self.ritual_queue = []


    def flush(self, board: str):
        if board:
            self.flush_ritual_db_images(board)


    def shutdown(self, board: str):
        """`flush(board)` and close any non-RitualDb connections."""
        if board:
            self.flush(board)


    def flush_ritual_db_images(self, board: str):
        """
        Writes to <board>_images table.
        """
        if not self.ritual_queue:
            return

        sql = f'''
        insert into `{board}_images`
            (media_hash, media, total, banned)
        values (?, ?, 1, 0) on conflict(media_hash) do update set
            total = total + 1,
            media = coalesce(media, excluded.media)
        ;'''
        self.ritual_db.db.run_query_many(sql, self.ritual_queue, commit=True)
        self.ritual_queue = []


    @abstractmethod
    def download_full_media(self, url: str, post: dict, board: str):
        pass


    @abstractmethod
    def get_dirpath_and_filename(self, board: str, media_type: MediaType, post: dict) -> tuple[str, str]:
        pass


    def save(self, post: dict, board: str, media_type: MediaType, content: bytes, dirpath: str=None, filename: str=None):
        """
        Overwrites any existing files.
        """
        if not (dirpath and filename):
            dirpath, filename, self.get_dirpath_and_filename(board, media_type, post)

        filepath = os.path.join(dirpath, filename)

        makedir_p(dirpath)

        with open(filepath, 'wb') as f:
            f.write(content)

        configs.logger.info(f'[{board}] Saved [{media_type.value}] {filepath}')

        if configs.make_thumbnails and media_type == MediaType.full_media:
            # allow time for the fs to "digest" newly written file
            sleep(0.1)
            if os.path.isfile(filepath):
                dirpath_thumb, filename_thumb = self.get_dirpath_and_filename(board, MediaType.thumbnail, post)
                filepath_thumb = os.path.join(dirpath_thumb, filename_thumb)
                create_thumbnail(
                    post,
                    filepath,
                    filepath_thumb,
                    logger=configs.logger,
                )


    def should_write_to_disk(
        self,
        url: str,
        post: dict,
        media_type: MediaType,
        content: bytes,
        fsize_computed: str=None,
        md5_computed: str=None,
    ) -> bool:
        if fsize_computed is None:
            fsize_computed = len(content)

        if not fsize_computed:
            return False

        if configs.enforce_fsize_lte and (fsize_api := post.get('fsize')):
            if fsize_computed > fsize_api:
                log_util(configs.logger, f'Not saving: {url=} - {fsize_computed=} > {fsize_api=}')
                return False

        if media_type == MediaType.full_media:
            if configs.enforce_md5_equality and (md5_api := post.get('md5')):

                if not md5_computed:
                    md5_computed = get_md5_b64_hash(content) # deferred md5 computation

                if md5_computed != md5_api:
                    log_util(configs.logger, f'Not saving: {url=} - {md5_api=} != {md5_computed=}')
                    return False

        return True


    def download_thumbnail(self, url: str, post: dict, board: str):
        filepath = self.get_dirpath_and_filename(board, MediaType.thumbnail, post)

        if os.path.isfile(filepath):
            return

        content = wrap_fetch_media_bytes(self.fetcher.session, url, post['ext'])
        if not content:
            return

        if not self.should_write_to_disk(url, post, MediaType.thumbnail, content):
            return

        self.save(post, board, MediaType.thumbnail, content)


    def download_media_for_ids(
        self,
        board: str,
        pid_2_post: dict[int, dict],
        full_pids: set[int],
        thumb_pids: set[int]
    ):
        for pid in full_pids:
            if pid not in pid_2_post:
                configs.logger.info(f'[{board}] Post {pid} not found in pid_2_post, skipping full media download')
                continue

            post = pid_2_post[pid]
            url = get_media_url(configs.url_full_media, board, post, MediaType.full_media)
            self.download_full_media(url, post, board)

        # thumb_pids should be empty in if this is True, but we can "double mitigate" downloading thumbnails
        if configs.make_thumbnails:
            return

        for pid in thumb_pids:
            if pid not in pid_2_post:
                configs.logger.info(f'[{board}] Post {pid} not found in pid_2_post, skipping thumbnail download')
                continue

            post = pid_2_post[pid]
            url = get_media_url(configs.url_thumbnail, board, post, MediaType.thumbnail)
            self.download_thumbnail(url, post, board)


class AsagiMediaFP(MediaFP):
    def __init__(self, fetcher: Fetcher, media_save_path: str, ritual_db: RitualDb):
        super().__init__(fetcher, media_save_path, ritual_db)


    def get_dirpath_and_filename(self, board: str, media_type: MediaType, post: dict) -> tuple[str, str]:
        # assume already validated post attributes via ChanPost
        if media_type == MediaType.full_media:
            filename = f'{post['tim']}{post['ext']}'
        elif media_type == MediaType.thumbnail:
            filename = f'{post['tim']}s.jpg'
        else:
            raise ValueError(media_type)

        dirpath = os.path.join(
            self.media_save_path,
            board,
            media_type.value,
            filename[:4],
            filename[4:6],
        )

        return dirpath, filename


    def download_full_media(self, url: str, post: dict, board: str):
        dirpath, filename = self.get_dirpath_and_filename(board, MediaType.full_media, post)
        filepath = os.path.join(dirpath, filename)

        if os.path.isfile(filepath):
            return

        content = wrap_fetch_media_bytes(self.fetcher.session, url, post['ext'])
        if not content:
            return

        if not self.should_write_to_disk(url, post, MediaType.full_media, content):
            return

        self.save(post, board, MediaType.full_media, content)

        if post.get('md5'):
            media = f"{post.get('tim')}{post.get('ext')}"
            self.ritual_queue.append((post['md5'], media))


class SutraMediaFP(MediaFP):
    def __init__(self, fetcher: Fetcher, media_save_path: str, ritual_db: RitualDb):
        super().__init__(fetcher, media_save_path, ritual_db)

        self.scanner_db = ScannerDb(configs.scanner_db_path)
        self.scanner_db.init_db()

        self.scanner_queue = []


    def download_media_for_ids(
        self,
        board: str,
        pid_2_post: dict[int, dict],
        full_pids: set[int],
        thumb_pids: set[int]
    ):
        # We can avoid downloading existing api md5 files with os.path.isfile(),
        # but to avoid downloading files with md5s that differ from the report api md5s, we do a scanner-db query

        md5_to_pid = {}
        for pid in full_pids:
            if pid in pid_2_post and (md5 := pid_2_post[pid]['md5']):
                md5_to_pid[md5] = pid

        existing_md5s = set()
        if md5_to_pid:
            placeholders = ','.join(['?'] * len(md5_to_pid))
            sql = f'select md5_computed from hashtab where md5_computed in ({placeholders}) and is_saved = 1;'
            params = tuple(md5_to_pid.keys())
            existing_md5s = {row[0] for row in self.scanner_db.run_query_tuple(sql, params)}

        remove_pids = {md5_to_pid[md5] for md5 in existing_md5s}

        if remove_pids:
            full_pids.difference_update(remove_pids)
            thumb_pids.difference_update(remove_pids)

            for pid in remove_pids:
                pid_2_post.pop(pid, None)  # avoid stale posts reaching downloader

        super().download_media_for_ids(board, pid_2_post, full_pids, thumb_pids)


    def flush(self, board: str):
        super().flush(board)
        self.flush_scanner()


    def shutdown(self, board: str):
        super().flush(board)
        self.scanner_db.save_and_close()


    def get_dirpath_and_filename(self, board: str, media_type: MediaType, post: dict) -> tuple[str, str]:
        if media_type == MediaType.full_media:
            filename = f'{post['filename_md5']}{post['ext']}'
        elif media_type == MediaType.thumbnail:
            filename = f'{post['filename_md5']}.jpg'
        else:
            raise ValueError(media_type)

        dirpath = os.path.join(
            self.media_save_path,
            media_type.value,
            filename[:2],
            filename[2:4],
            filename[4:6],
        )

        return dirpath, filename


    def download_full_media(self, url: str, post: dict, board: str):
        """
        md5 is cracked, meaning someone could upload files with specific md5 hashes with the hopes of
        erasing existing content in archives. We can avoid that by never overwriting existing files.
        """
        # first check if the api md5 exists on disk
        post['filename_md5'] = post['md5']
        dirpath, filename = self.get_dirpath_and_filename(board, MediaType.full_media, post)
        filepath = os.path.join(dirpath, filename)

        if os.path.isfile(filepath):
            return

        content = wrap_fetch_media_bytes(self.fetcher.session, url, post['ext'])
        if not content:
            return

        fsize_computed = len(content)
        md5_computed = get_md5_b64_hash(content)

        if not self.should_write_to_disk(url, post, MediaType.full_media, content, fsize_computed=fsize_computed, md5_computed=md5_computed):
            return

        # now check if the downloaded file md5 exists on disk
        post['filename_md5'] = md5_computed
        dirpath, filename = self.get_dirpath_and_filename(board, MediaType.full_media, post)
        filepath = os.path.join(dirpath, filename)

        if os.path.isfile(filepath):
            return

        self.save(post, board, MediaType.full_media, content, dirpath=dirpath, filename=filename)

        self.scanner_queue.append((
            dirpath,
            md5_computed,
            post['ext'],
            post['md5'],
            md5_computed,
            post['fsize'],
            fsize_computed,
            0,
            1,
            0,
        ))

        if post.get('md5'):
            media = f"{post.get('tim')}{post.get('ext')}"
            self.ritual_queue.append((post['md5'], media))


    def flush_scanner(self):
        if not self.scanner_queue:
            return

        sql_string = '''
        insert or ignore into hashtab_view
            (dirpath, filename_no_ext, ext, md5, md5_computed, fsize, fsize_computed, is_banned, is_saved)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ;'''

        self.scanner_db.run_query_many(sql_string, self.scanner_queue, commit=True)
        self.scanner_queue = []
