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
    get_asagi_value_media,
    get_asagi_value_preview,
    get_media_url,
    sleep,
    get_md5_b64_hash,
    get_sha256_hash,
    log_util,
    makedir_p,
)
from db.ritual import RitualDb


def wrap_fetch_media_bytes(session: Session, url: str, ext: str) -> bytes | None:
    return fetch_media_bytes(
        url,
        ext,
        video_cooldown_sec=configs.video_cooldown_sec,
        image_cooldown_sec=configs.image_cooldown_sec,
        headers=configs.headers,
        logger=configs.logger,
        session=session,
    )


def ensure_dot_ext(ext: str) -> str:
    if not ext:
        return ''
    return ext if ext.startswith('.') else f'.{ext}'


def build_content_meta(content: bytes) -> dict:
    return {
        'computed_bsize': len(content),
        'md5_b64_computed': get_md5_b64_hash(content)
    }


class MediaFP(ABC):
    def __init__(self, fetcher: Fetcher, media_save_path: str, ritual_db: RitualDb):
        self.fetcher = fetcher
        self.media_save_path = media_save_path
        self.ritual_db = ritual_db


    @abstractmethod
    def get_filefolder(self, post: dict, media_type: MediaType, board: str) -> str:
        pass


    @abstractmethod
    def get_filename(self, post: dict, media_type: MediaType, board: str) -> str:
        pass


    @abstractmethod
    def download_full_media(self, url: str, post: dict, board: str):
        pass


    def get_filepath(self, post: dict, media_type: MediaType, board: str) -> str:
        return os.path.join(
            self.get_filefolder(post, media_type, board),
            self.get_filename(post, media_type, board)
        )


    def save(self, post: dict, board: str, media_type: MediaType, content: bytes) -> str:
        filepath = self.get_filepath(post, media_type, board)
        filefolder = os.path.dirname(filepath)

        makedir_p(filefolder)

        with open(filepath, 'wb') as f:
            f.write(content)

        configs.logger.info(f'[{board}] Downloaded [{media_type.value}] {filepath}')

        if media_type == MediaType.full_media and configs.make_thumbnails:
            thumb_path = self.get_filepath(post, MediaType.thumbnail, board)

            # allow time for the fs to "digest" newly written file
            sleep(0.1)
            create_thumbnail(
                post,
                filepath,
                thumb_path,
                logger=configs.logger,
            )
        return filepath


    def get_max_size_for_media_type(self, media_type: MediaType) -> int | None:
        if media_type == MediaType.thumbnail:
            return getattr(configs, 'max_thumbnail_size_bytes', None)

        return getattr(configs, 'max_full_media_size_bytes', None)


    def should_write_to_disk(
        self,
        url: str,
        meta: dict,
        media_type: MediaType,
        api_bsize: int | None,
        api_md5_b64: str | None,
    ) -> bool:
        computed_bsize = meta['computed_bsize']
        computed_md5 = meta['md5_b64_computed']

        max_size = self.get_max_size_for_media_type(media_type)
        if max_size is not None and computed_bsize > max_size:
            log_util(
                configs.logger,
                f'Skipping download because file exceeds configured limit {url=} computed_bsize={computed_bsize} limit={max_size}'
            )
            return False

        if api_bsize and computed_bsize > api_bsize:
            log_util(
                configs.logger,
                f'File computed_bsize larger than API reported {url=} expected={api_bsize} got={computed_bsize}'
            )

        if api_md5_b64 and computed_md5 != api_md5_b64:
            if configs.download_files_with_mismatched_md5:
                log_util(
                    configs.logger,
                    f'Hashes differ but downloading anyway: {url} api={api_md5_b64} computed={computed_md5}'
                )
            else:
                log_util(
                    configs.logger,
                    f'Hashes differ, skipping download: {url} api={api_md5_b64} computed={computed_md5}'
                )
                return False

        return True


    def download_thumbnail(self, url: str, post: dict, board: str):
        filepath = self.get_filepath(post, MediaType.thumbnail, board)

        if os.path.isfile(filepath):
            return

        content = wrap_fetch_media_bytes(self.fetcher.session, url, post['ext'])
        if not content:
            return

        meta = build_content_meta(content)

        if not self.should_write_to_disk(
            url,
            meta,
            MediaType.thumbnail,
            None,
            None
        ):
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
    def __init__(self, fetcher: Fetcher, media_save_path: str, ritual_db):
        super().__init__(fetcher, media_save_path)
        self.ritual_db = ritual_db


    def get_filename(self, post: dict, media_type: MediaType, board: str) -> str:
        return get_asagi_value_media(post) if media_type == MediaType.full_media else get_asagi_value_preview(post)


    def get_filefolder(self, post: dict, media_type: MediaType, board: str) -> str:
        filename = self.get_filename(post, media_type, board)
        tim = filename.rsplit('.', maxsplit=1)[0]

        return os.path.join(
            self.media_save_path,
            board,
            media_type.value,
            tim[:4],
            tim[4:6],
        )


    def download_full_media(self, url: str, post: dict, board: str):
        filepath = self.get_filepath(post, MediaType.full_media, board)

        if os.path.isfile(filepath):
            return

        content = wrap_fetch_media_bytes(self.fetcher.session, url, post['ext'])
        if not content:
            return

        meta = build_content_meta(content)

        if not self.should_write_to_disk(
            url,
            meta,
            MediaType.full_media,
            post.get('fsize'),
            post.get('md5')
        ):
            return

        self.save(post, board, MediaType.full_media, content)


class SutraMediaFP(MediaFP):
    def __init__(self, fetcher: Fetcher, media_save_path: str, scanner_db: ScannerDb):
        super().__init__(fetcher, media_save_path)
        self.scanner_db = scanner_db


    def get_filename(self, post: dict, media_type: MediaType, board: str) -> str:
        sha256 = post['sha256']
        ext = ensure_dot_ext(post.get('ext', ''))

        if media_type == MediaType.full_media:
            return f'{sha256}{ext}'

        return f'{sha256}.jpg'


    def get_filefolder(self, post: dict, media_type: MediaType, board: str) -> str:
        sha256 = post['sha256']

        base = 'img' if media_type == MediaType.full_media else 'thb'

        return os.path.join(
            self.media_save_path,
            base,
            sha256[:2],
            sha256[2:4],
            sha256[4:6],
        )


    def download_full_media(self, url: str, post: dict, board: str):
        content = wrap_fetch_media_bytes(self.fetcher.session, url, post['ext'])
        if not content:
            return

        meta = build_content_meta(content)

        if not self.should_write_to_disk(
            url,
            meta,
            MediaType.full_media,
            post.get('fsize'),
            post.get('md5')
        ):
            return

        sha256 = get_sha256_hash(content)
        post['sha256'] = sha256

        filepath = self.save(post, board, MediaType.full_media, content)

        # TODO no double computing
        filename = os.path.basename(filepath)
        filename_no_ext, ext = filename.rsplit('.', 1)

        # TODO ext and directory table inserts
        # TODO batch inserts

        self.scanner_db.run_query_tuple(
            '''insert or ignore into hashtab 
               (dir_id, filename_no_ext, ext_id, md5_b64_given, sha256, md5_b64_computed, bsize)
               values (?, ?, ?, ?, ?, ?, ?)''',
            (
                None,
                filename_no_ext,
                ext,
                post['md5'],
                sha256,
                meta['md5_b64_computed'],
                meta['computed_bsize']
            ),
            commit=True
        )