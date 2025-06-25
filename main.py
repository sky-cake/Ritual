import html
import os
import re
import time
from collections import defaultdict
from sqlite3 import Cursor

import requests
import tqdm

import configs
from asagi import (
    create_thumbnail,
    get_filepath,
    get_fs_filename_full_media,
    get_fs_filename_thumbnail,
    post_has_file,
    post_is_sticky,
    get_op_threads,
    get_d_board, 
    get_d_image,
    get_d_thread
)
from db import get_connection
from enums import MediaType
from utils import (
    fetch_json,
    is_image_path,
    is_video_path,
    make_path,
    sleep,
    test_deps
)


def download_file(url: str, filename: str):
    if is_video_path(filename):
        ts = [configs.video_cooldown_sec, 4.0, 6.0, 10.0]
    elif is_image_path(filename):
        ts = [configs.image_cooldown_sec, 4.0, 6.0, 10.0]
    else:
        raise ValueError(filename)

    for i, t in enumerate(ts):
        resp = requests.get(url, headers=configs.headers)
        if i > 0:
            configs.logger.warning(f'Incrementing 1 sec: {configs.video_cooldown_sec=} {configs.image_cooldown_sec=}')
            configs.video_cooldown_sec += 1.0
            configs.image_cooldown_sec += 1.0
            configs.logger.warning(f'Incremented 1 sec: {configs.video_cooldown_sec=} {configs.image_cooldown_sec=}')

        sleep(t, add_random=configs.add_random)

        if resp.status_code != 200:
            configs.logger.warning(f'{url=} {resp.status_code=}')
            return

        if resp.status_code == 200 and resp.content:
            with open(filename, 'wb') as f:
                f.write(resp.content)
            return True
        
        configs.logger.warning(f'No content {url=} {filename=} {resp.content=}')
    configs.logger.warning(f'Max retries exceeded {url=} {filename=}')


def get_catalog(board) -> dict:
    catalog = fetch_json(
        configs.url_catalog.format(board=board),
        headers=configs.headers,
        request_cooldown_sec=configs.request_cooldown_sec,
        add_random=configs.add_random
    )
    configs.logger.info(f'[{board}] Downloaded catalog')
    if catalog:
        return catalog


def should_archive(board: str, subject: str, comment: str, whitelist: str =None, blacklist: str=None):
    """
    - If a post is blacklisted and whitelisted, it will not be archived - blacklisted filters take precedence over whitelisted filters.
    - If only a blacklist is specified, skip blacklisted posts, and archive everything else.
    - If only a whitelist is specified, archive whitelisted posts, and skip everything else.
    - If no lists are specified, archive everything.
    """
    op_comment_min_chars = configs.boards[board].get('op_comment_min_chars')
    if op_comment_min_chars and len(comment) < op_comment_min_chars:
        return False

    op_comment_min_chars_unique = configs.boards[board].get('op_comment_min_chars_unique')
    if op_comment_min_chars_unique and len(set(comment)) < op_comment_min_chars_unique:
        return False

    blacklist_post_filter = configs.boards[board].get('blacklist') if blacklist is None else blacklist
    if blacklist_post_filter:
        if subject:
            if re.fullmatch(blacklist_post_filter, subject, re.IGNORECASE) is not None:
                return False

        if comment:
            if re.fullmatch(blacklist_post_filter, comment, re.IGNORECASE) is not None:
                return False
    
    whitelist_post_filter = configs.boards[board].get('whitelist') if whitelist is None else whitelist
    if whitelist_post_filter:
        if subject:
            if re.fullmatch(whitelist_post_filter, subject, re.IGNORECASE) is not None:
                return True

        if comment:
            if re.fullmatch(whitelist_post_filter, comment, re.IGNORECASE) is not None:
                return True
            
        return False
    
    return True


def thread_modified(board: str, thread: dict, d_last_modified: dict) -> bool:
    """`True` indicates we should download the thread. Also handles the `d_last_modified` cache."""
    no = thread.get('no')
    thread_last_modified = thread.get('last_modified')

    if board not in d_last_modified:
        d_last_modified[board] = {}
    if no not in d_last_modified[board]:
        d_last_modified[board][no] = {}

    thread_last_modified_cached = d_last_modified[board][no].get('last_modified')

    # Update the thread's last modified time in d_last_modified.
    d_last_modified[board][no]['last_modified'] = thread_last_modified

    # Don't let the dict grow over N entries per board.
    N = 200
    if len(d_last_modified[board]) > N:
        # In case of multiple stickies, or similar special threads, we delete M oldest threads
        M = 10
        tmp_nos = sorted(d_last_modified[board], key=lambda no: d_last_modified[board][no]['last_modified'])
        for stale_no in tmp_nos[:M]:
            del d_last_modified[board][stale_no]

    # last_modified changed
    if thread_last_modified and thread_last_modified_cached and thread_last_modified != thread_last_modified_cached:
        return True

    # new thread
    if thread_last_modified_cached is None:
        return True

    return False


def filter_catalog(board: str, catalog: dict, d_last_modified: dict, is_first_loop: bool) -> list[int]:
    thread_ids = []
    not_modified_thread_count = 0
    reinitializing = False
    for page in catalog:
        for thread in page['threads']:

            if post_is_sticky(thread):
                continue

            subject = html.unescape(thread.get('sub', ''))
            comment = html.unescape(thread.get('com', ''))
            if not should_archive(board, subject, comment):
                continue

            if is_first_loop and configs.reinitialize:
                reinitializing = True
                thread_ids.append(thread.get('no'))
                thread_modified(board, thread, d_last_modified) # populate `d_last_modified`
                continue

            if not thread_modified(board, thread, d_last_modified):
                not_modified_thread_count += 1
                continue

            thread_ids.append(thread.get('no'))

    m = f'{not_modified_thread_count} threads not modified'
    if reinitializing:
        m = 'Ignoring last modified timestamps on first loop'

    configs.logger.info(f'[{board}] {m}. Queuing {len(thread_ids)} threads.')

    return thread_ids


def get_non_deleted_post_ids_for_thread_nums(cursor: Cursor, board: str, thread_ids: list[int]) -> dict[int, list[int]]:
    if not thread_ids:
        return {}

    placeholders = ','.join(['?'] * len(thread_ids))
    sql = f"""select thread_num, num from `{board}` where thread_num in ({placeholders}) and deleted = 0;"""
    cursor.execute(sql, thread_ids)
    results = cursor.fetchall()
    d = defaultdict(list)
    for r in results:
        d[r['thread_num']].append(r['num'])
    return d


def set_posts_deleted(cursor: Cursor, board: str, post_ids: list[int]) -> None:
    if not post_ids:
        return

    placeholders = ','.join(['?'] * len(post_ids))
    sql = f"update `{board}` set deleted = 1 where num in ({placeholders});"
    cursor.execute(sql, post_ids)


def get_post_ids_from_thread(thread: dict) -> set[int]:
    return {t['no'] for t in thread['posts']}


def get_threads(cursor: Cursor, board: str, thread_ids: list[int]) -> list[dict]:
    threads = []
    deleted_post_ids = []

    # one query, rather than one per thread
    thread_num_2_prev_post_nums = get_non_deleted_post_ids_for_thread_nums(cursor, board, thread_ids)

    for thread_id in thread_ids:
        thread = fetch_json(
            configs.url_thread.format(board=board, thread_id=thread_id),
            headers=configs.headers,
            request_cooldown_sec=configs.request_cooldown_sec,
            add_random=configs.add_random
        )
        if thread:
            configs.logger.info(f'[{board}] Found thread [{thread_id}]')
            
            current_post_ids = get_post_ids_from_thread(thread)
            previous_post_ids = thread_num_2_prev_post_nums.get(thread_id, [])
            for post_id in previous_post_ids:
                if post_id not in current_post_ids:
                    deleted_post_ids.append(post_id)
                    configs.logger.info(f'[{board}] Post Deleted [{thread_id}] [{post_id}]')

            threads.append(thread)
        else:
            configs.logger.info(f'[{board}] Lost Thread [{thread_id}]')

    if deleted_post_ids:
        set_posts_deleted(cursor, board, deleted_post_ids)
    return threads


def do_upsert(cursor: Cursor, table: str, d: dict, conflict_col: str, returning: str):
    sql_cols = ', '.join(d)
    sql_placeholders = ', '.join(['?'] * len(d))
    sql_conflict = ', '.join([f'{col}=?' for col in d])

    sql = f"""insert into `{table}` ({sql_cols}) values ({sql_placeholders}) on conflict({conflict_col}) do update set {sql_conflict} returning {returning};"""
    values = list(d.values())
    parameters = values + values
    cursor.execute(sql, parameters)
    result = cursor.fetchone()
    return result[returning]


def do_upsert_many(cursor: Cursor, table: str, rows: list[dict], conflict_col: str, batch_size: int = 155):
    if not rows:
        return

    keys = list(rows[0])
    placeholder = '(' + ', '.join(['?'] * len(keys)) + ')'
    sql_cols = ', '.join(keys)
    sql_conflict = ', '.join([f'{k}=excluded.{k}' for k in keys])

    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        placeholders = ', '.join([placeholder] * len(chunk))
        sql = f"""insert into `{table}` ({sql_cols}) values {placeholders} on conflict({conflict_col}) do update set {sql_conflict};"""
        flat_values = [v for row in chunk for v in row.values()]
        cursor.execute(sql, flat_values)


def upsert_threads(cursor, board: str, threads: list[dict]):
    post_rows = []
    thread_rows = []

    for thread in threads:
        d_thread = get_d_thread(thread)
        thread_rows.append(d_thread)

        is_op = True
        for post in thread['posts']:
            if post_is_sticky(post):
                is_op = False
                continue

            media_id = None
            if post_has_file(post):
                d_image = get_d_image(post, is_op)
                media_id = do_upsert(cursor, f'{board}_images', d_image, 'media_hash', 'media_id')
                assert media_id

            d_board = get_d_board(post, media_id, unescape_data_b4_db_write=configs.unescape_data_b4_db_write)
            post_rows.append(d_board)
            is_op = False

    do_upsert_many(cursor, board, post_rows, 'num')
    do_upsert_many(cursor, f'{board}_threads', thread_rows, 'thread_num')


def match_sub_and_com(post_op: dict, pattern: str):
    sub = post_op.get('sub')
    com = post_op.get('com')

    if sub and re.fullmatch(pattern, sub, re.IGNORECASE):
        return True
    
    if com and re.fullmatch(pattern, com, re.IGNORECASE):
        return True
    
    return False


def download_thread_media(board: str, threads: list[dict], media_type: MediaType):
    for thread in threads:
        for post in thread['posts']:
            if post_has_file(post):
                tim = post.get('tim') if post.get('tim') else time.time()
                ext = post.get('ext')
                assert tim
                assert ext

                if media_type == MediaType.thumbnail:
                    board_thumb_pattern = configs.boards[board].get('dl_thumbs')
                    if isinstance(board_thumb_pattern, str) and not match_sub_and_com(thread['posts'][0], board_thumb_pattern):
                        continue

                    if configs.url_thumbnail is None:
                        configs.logger.info('Warning: this site does not support thumbnail downloads.')

                    url = configs.url_thumbnail.format(board=board, image_id=tim)
                    filename = get_fs_filename_thumbnail(post)
                    filepath = get_filepath(configs.media_save_path, board, MediaType.thumbnail.value, filename)

                elif media_type == MediaType.full_media:
                    board_full_media_pattern = configs.boards[board].get('dl_full_media')
                    if isinstance(board_full_media_pattern, str) and not match_sub_and_com(thread['posts'][0], board_full_media_pattern):
                        continue

                    url = configs.url_full_media.format(board=board, image_id=tim, ext=ext)
                    filename = get_fs_filename_full_media(post)
                    filepath = get_filepath(configs.media_save_path, board, MediaType.full_media.value, filename)

                else:
                    raise ValueError(media_type)

                # os.path.isfile is cheap, no need for a cache
                if not os.path.isfile(filepath):
                    result = download_file(url, filepath)
                    if result:
                        configs.logger.info(f"[{board}] Downloaded [{media_type.value}] {filepath}")
                        if media_type == MediaType.full_media and configs.make_thumbnails:
                            thumb_path = get_filepath(configs.media_save_path, board, MediaType.thumbnail.value, get_fs_filename_thumbnail(post))
                            sleep(0.1, add_random=configs.add_random)
                            create_thumbnail(post, filepath, thumb_path, logger=configs.logger)


def create_non_existing_tables():
    conn = get_connection()
    cursor = conn.cursor()
    for board in configs.boards:
        try:
            sql = f'SELECT * FROM `{board}` LIMIT 1;'
            conn.execute(sql)
            configs.logger.info(f'[{board}] Tables already exist.')
        except Exception:
            configs.logger.info(f'[{board}] Creating tables.')
            with open(make_path('schema.sql')) as f:
                sql = f.read()
            sqls = sql.replace('%%BOARD%%', board).split(';')
            for sql in sqls:
                conn.execute(sql)
            conn.commit()
    cursor.close()
    conn.close()


def main():
    test_deps()

    create_non_existing_tables()

    d_last_modified = dict() # {g: {123: 1717755968, 124: 1717755999}}

    is_first_loop = True

    while True:
        conn = get_connection()
        cursor = conn.cursor()

        configs.logger.info(f'Loop Started')
        times = {}
        for board in tqdm.tqdm(configs.boards, disable=configs.disable_tqdm):
            start = time.time()

            catalog = get_catalog(board)
            if not catalog:
                configs.logger.info(f"Catalog returned {catalog}")
                continue

            thread_ids = filter_catalog(board, catalog, d_last_modified, is_first_loop)

            threads = get_threads(cursor, board, thread_ids)

            if configs.boards[board].get('thread_text'):
                upsert_threads(cursor, board, threads)
                conn.commit()

            if configs.boards[board].get('dl_full_media_op'):
                op_threads = get_op_threads(threads)
                download_thread_media(board, op_threads, MediaType.full_media)

            if configs.boards[board].get('dl_full_media'):
                download_thread_media(board, threads, MediaType.full_media)

            # only dl thumbs if we are not instructed to make them
            if configs.boards[board].get('dl_thumbs_op') and not (configs.make_thumbnails and configs.boards[board].get('dl_full_media_op') and configs.boards[board].get('dl_full_media')):
                op_threads = get_op_threads(threads)
                download_thread_media(board, op_threads, MediaType.thumbnail)

            if configs.boards[board].get('dl_thumbs') and not (configs.make_thumbnails and configs.boards[board].get('dl_full_media_op') and configs.boards[board].get('dl_full_media')):
                download_thread_media(board, threads, MediaType.thumbnail)

            times[board] = round((time.time() - start) / 60, 2) # minutes

        is_first_loop = False

        cursor.close()
        conn.close()

        configs.logger.info("Loop Completed")
        configs.logger.info("Duration for each board:")

        for board, duration in times.items():
            configs.logger.info(f' {board:<4} {duration:.1f}m')

        total_duration = round(sum(times.values()), 1)
        configs.logger.info(f"Total Duration: {total_duration}m")
        configs.logger.info(f"Going to sleep for {configs.loop_cooldown_sec}s")

        time.sleep(configs.loop_cooldown_sec)


if __name__=='__main__':
    main()