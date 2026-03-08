import os
from abc import ABC, abstractmethod
from requests import Session

import configs
from enums import MediaType
from fetcher import Fetcher
from scanner.db_scanner import ScannerDb
from utils import (
    create_thumbnail,
    fetch_media_bytes,
    get_media_url,
    sleep,
    get_md5_b64_hash,
    get_sha256_hash,
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
    def __init__(self, fetcher: Fetcher, media_save_path: str):
        self.fetcher = fetcher
        self.media_save_path = media_save_path


    def flush(self):
        pass


    def clean_up(self):
        pass


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
                configs.logger.info(
                    f'[{board}] Post {pid} not found in pid_2_post, skipping full media download'
                )
                continue

            post = pid_2_post[pid]
            url = get_media_url(configs.url_full_media, board, post, MediaType.full_media)
            self.download_full_media(url, post, board)

        # thumb_pids should be empty in if this is True, but we can "double mitigate" downloading thumbnails
        if configs.make_thumbnails:
            return

        for pid in thumb_pids:
            if pid not in pid_2_post:
                configs.logger.info(
                    f'[{board}] Post {pid} not found in pid_2_post, skipping thumbnail download'
                )
                continue

            post = pid_2_post[pid]
            url = get_media_url(configs.url_thumbnail, board, post, MediaType.thumbnail)
            self.download_thumbnail(url, post, board)


class AsagiMediaFP(MediaFP):
    def __init__(self, fetcher: Fetcher, media_save_path: str):
        super().__init__(fetcher, media_save_path)


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


class SutraMediaFP(MediaFP):
    def __init__(self, fetcher: Fetcher, media_save_path: str):
        super().__init__(fetcher, media_save_path, None)

        self.scanner_db = ScannerDb(configs.scanner_db_path)
        self.scanner_db.init_db()

        self.scanner_insert_queue = []


    def clean_up(self):
        self.flush()
        self.scanner_db.save_and_close()
        

    def get_dirpath_and_filename(self, board: str, media_type: MediaType, post: dict) -> tuple[str, str]:
        ext = post['ext']
        sha256 = post['sha256']

        if media_type == MediaType.full_media:
            filename = f'{sha256}{ext}'
        elif media_type == MediaType.thumbnail:
            filename = f'{sha256}.jpg'
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
        # The sequence of steps for Sutra are a bit different from Asagi
        # because we need to fetch the file content before creating the SHA256 hash / filename

        content = wrap_fetch_media_bytes(self.fetcher.session, url, post['ext'])
        if not content:
            return

        fsize_computed = len(content)
        md5_computed = get_md5_b64_hash(content)

        if not self.should_write_to_disk(url, post, MediaType.full_media, content, fsize_computed=fsize_computed, md5_computed=md5_computed):
            return

        sha256 = get_sha256_hash(content)
        post['sha256'] = sha256 # tack on sha256 for filename usage

        dirpath, filename = self.get_dirpath_and_filename(board, MediaType.full_media, post)
        filepath = os.path.join(dirpath, filename)

        if os.path.isfile(filepath):
            # TODO before we ever get here, we can mitigate downloading identical files using (md5, fsize, ext)
            return

        self.save(post, board, MediaType.full_media, content, dirpath=dirpath, filename=filename)

        self.scanner_insert_queue.append((
            dirpath,
            sha256,
            post['ext'],
            post['md5'],
            md5_computed,
            post['fsize'],
            fsize_computed,
            sha256,
            0,
            1,
            0,
        ))


    def flush(self):
        if not self.scanner_insert_queue:
            return

        sql_string = '''
        insert or ignore into hashtab_view
        (dirpath, filename_no_ext, ext, md5, md5_computed, fsize, fsize_computed, sha256, is_banned, is_saved, has_error)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ;'''

        self.scanner_db.run_query_many(sql_string, self.scanner_insert_queue, commit=True)
        self.scanner_insert_queue = []
