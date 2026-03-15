import os
from abc import ABC, abstractmethod
from requests import Session

import configs
from enums import MediaType
from fetcher import Fetcher
from db.ritual import RitualDb
from utils import (
    create_thumbnail,
    fetch_media_bytes,
    get_media_url,
    sleep,
    get_md5_b64_hash,
    get_fs_safe_b64,
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
    def get_dirpath_and_filename(self, board: str, media_type: MediaType, post: dict) -> tuple[str, str]:
        pass


    def save(self, post: dict, board: str, media_type: MediaType, content: bytes, dirpath: str=None, filename: str=None):
        """
        Overwrites any existing files.
        """
        if not (dirpath and filename):
            dirpath, filename = self.get_dirpath_and_filename(board, media_type, post)

        filepath = os.path.join(dirpath, filename)

        makedir_p(dirpath)

        with open(filepath, 'wb') as f:
            f.write(content)

        configs.logger.info(f'[{board}] Saved [{media_type.value}] {filepath}')

        if configs.make_thumbnails and media_type == MediaType.full_media:
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
        fsize_computed: int | None=None,
        md5_computed: str | None=None,
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
            if configs.enforce_md5_equality and (md5_api := post['md5']):

                if not md5_computed:
                    md5_computed = get_md5_b64_hash(content)

                if md5_computed != md5_api:
                    log_util(configs.logger, f'Not saving: {url=} - {md5_api=} != {md5_computed=}')
                    return False

        return True


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

        if post['md5']:
            media = f"{post.get('tim')}{post.get('ext')}"
            self.ritual_queue.append((post['md5'], media))


    def download_thumbnail(self, url: str, post: dict, board: str):
        dirpath, filename = self.get_dirpath_and_filename(board, MediaType.thumbnail, post)
        filepath = os.path.join(dirpath, filename)

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
        if media_type == MediaType.full_media:
            filename = f'{post["tim"]}{post["ext"]}'
        elif media_type == MediaType.thumbnail:
            filename = f'{post["tim"]}s.jpg'
        else:
            raise ValueError(media_type)

        dirpath = os.path.join(
            self.media_save_path,
            board,
            'image' if media_type == MediaType.full_media else 'thumb',
            filename[:4],
            filename[4:6],
        )

        return dirpath, filename


class SutraMediaFP(MediaFP):
    def __init__(self, fetcher: Fetcher, media_save_path: str, ritual_db: RitualDb):
        super().__init__(fetcher, media_save_path, ritual_db)


    def get_dirpath_and_filename(self, board: str, media_type: MediaType, post: dict) -> tuple[str, str]:
        if media_type == MediaType.full_media:
            filename = f'{get_fs_safe_b64(post["md5"])}{post["ext"]}'
        elif media_type == MediaType.thumbnail:
            filename = f'{get_fs_safe_b64(post["md5"])}.jpg'
        else:
            raise ValueError(media_type)

        dirpath = os.path.join(
            self.media_save_path,
            'img' if media_type == MediaType.full_media else 'thb',
            filename[:2],
            filename[2:4],
            filename[4:6],
        )

        return dirpath, filename
