import html
import json
import os
import random
import re
import time
from collections import OrderedDict
from sqlite3 import Cursor

import requests

import configs
from db import get_connection
from defs import URL4chan, URLlainchan, MediaType, h
from utils import convert_to_asagi_capcode, convert_to_asagi_comment, make_path


URL = URL4chan
if configs.site == 'lainchan':
    URL = URLlainchan


class MaxQueue:
    def __init__(self, boards, max_items_per_board=150*151):
        """
        threads_per_catalog = 150
        images_per_thread = 151
        """

        # use OrderedDict for lookup speed, rather than a list
        self.items = {b: OrderedDict() for b in boards}

        self.max_items_per_board = max_items_per_board

    def add(self, board: str, filepath: str):
        board_items = self.items[board]

        if filepath in board_items:
            return

        while len(board_items) >= self.max_items_per_board:
            board_items.popitem(last=False)

        board_items[filepath] = 0

    def __contains__(self, filepath: str) -> bool:
        return any(filepath in board_items for board_items in self.items.values())

    def __getitem__(self, board: str) -> OrderedDict:
        return self.items[board]


DOWNLOADED_MEDIA = MaxQueue(configs.boards)


def get_headers():
    headers = None
    if configs.user_agent:
        headers = {'User-Agent', configs.user_agent}
    return headers


def get_cookies():
    return {}


def sleep(t=None):
    if t:
        time.sleep(t)
        return

    s = configs.request_cooldown_sec
    if configs.add_random:
        s += random.uniform(0.0, 1.0)
    time.sleep(s)


def fetch_json(url) -> dict:
    try:
        resp = requests.get(url, headers=h)
        sleep()

        if resp.status_code == 200:
            return resp.json()
    except Exception:
        sleep(20)


def fetch_file(url):
    try:
        resp = requests.get(url, headers=h)
        sleep()

        if resp.status_code == 200:
            return resp.content
    except Exception:
        sleep(20)


def get_catalog(board) -> dict:
    catalog = fetch_json(URL.catalog.value.format(board=board))
    configs.logger.info(f'[{board}] Downloaded catalog')
    if catalog:
        return catalog


def should_archive(board, subject, comment, whitelist=None, blacklist=None):
    """
    - If a post is blacklisted and whitelisted, it will not be archived - blacklisted filters take precedence over whitelisted filters.
    - If only a blacklist is specified, skip blacklisted posts, and archive everything else.
    - If only a whitelist is specified, archive whitelisted posts, and skip everything else.
    - If no lists are specified, archive everything.
    """
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
    last_modified = thread.get('last_modified')

    if board not in d_last_modified:
        d_last_modified[board] = {}
    if no not in d_last_modified[board]:
        d_last_modified[board][no] = {}

    last_modified_d = d_last_modified[board][no].get('last_modified')

    # Update the thread's last modified time in d_last_modified.
    d_last_modified[board][no]['last_modified'] = last_modified

    # Don't let the dict grow over N entries per board.
    N = 200
    if len(d_last_modified[board]) > N:
        # In case of multiple stickies, or similar special threads, we delete M oldest threads
        M = 10
        tmp_nos = sorted(d_last_modified[board], key=lambda no: d_last_modified[board][no]['last_modified'])
        for stale_no in tmp_nos[:M]:
            del d_last_modified[board][stale_no]

    # last_modified changed
    if last_modified and last_modified_d and last_modified != last_modified_d:
        return True

    # new thread
    if last_modified_d is None:
        return True

    return False


def filter_catalog(board: str, catalog: dict, d_last_modified: dict) -> list[int]:
    thread_ids = []
    not_modified_thread_count = 0
    for page in catalog:
        for thread in page['threads']:

            if post_is_sticky(thread):
                continue

            subject = html.unescape(thread.get('sub', ''))
            comment = html.unescape(thread.get('com', ''))
            if not should_archive(board, subject, comment):
                continue

            if not configs.reinitialize and not thread_modified(board, thread, d_last_modified):
                not_modified_thread_count += 1
                continue

            thread_ids.append(thread.get('no'))

    if not_modified_thread_count > 0:
        configs.logger.info(f'[{board}] {not_modified_thread_count} threads not modified.')

    return thread_ids


def get_non_deleted_post_ids_for_thread_num(cursor: Cursor, board: str, thread_id: int) -> list[int]:
    sql = f"""select num from {board} where thread_num = ? and deleted = 0;"""
    parameters = [thread_id]
    cursor.execute(sql, parameters)
    results = cursor.fetchall()
    post_ids = [r['num'] for r in results]
    return post_ids


def set_posts_deleted(cursor: Cursor, board: str, post_ids: list[int]) -> list[int]:
    sql = f"""update {board} set deleted = 1 where num = ?;"""
    cursor.executemany(sql, [(p,) for p in post_ids])


def get_post_ids_from_thread(thread: dict) -> list[int]:
    return [t['no'] for t in thread['posts']]


def get_threads(cursor: Cursor, board: str, thread_ids: list[int]) -> list[dict]:
    threads = []
    deleted_post_ids = []
    for thread_id in thread_ids:
        thread = fetch_json(URL.thread.value.format(board=board, thread_id=thread_id))
        if thread:
            configs.logger.info(f'[{board}] Found thread [{thread_id}]')

            previous_post_ids = get_non_deleted_post_ids_for_thread_num(cursor, board, thread_id)
            if previous_post_ids:
                current_post_ids = get_post_ids_from_thread(thread)
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


def post_has_file(post: dict) -> bool:
    return post.get('tim') and post.get('ext') and post.get('md5')


def get_fs_filename_full_media(post: dict) -> str:
    if post_has_file(post):
        return f"{post.get('tim')}{post.get('ext')}"


def get_fs_filename_thumbnail(post: dict) -> str:
    if post_has_file(post):
        return f"{post.get('tim')}s.jpg"


def post_is_sticky(post: dict) -> bool:
    return post.get('sticky') == 1


def upsert_thread(cursor: Cursor, board: str, thread: dict):
    for i, post in enumerate(thread['posts']):
        if post_is_sticky(post):
            continue

        media_id = None
        if post_has_file(post):
            d_image = {
                # 'media_id': post.get('media_id'), # autoincremented
                'media_hash': post.get('md5'),
                'media': get_fs_filename_full_media(post),
                'preview_op': get_fs_filename_thumbnail(post) if i == 0 else None,
                'preview_reply': get_fs_filename_thumbnail(post) if i != 0 else None,
                'total': 0,
                'banned': 0,
            }
            media_id = do_upsert(cursor, f'{board}_images', d_image, 'media_hash', 'media_id')
            assert media_id

        d_board = {
            # 'doc_id': post.get('doc_id'), # autoincremented
            'media_id': media_id or 0,
            'poster_ip': post.get('poster_ip', '0'),
            'num': post.get('no', 0),
            'subnum': post.get('subnum', 0),
            'thread_num': post.get('no') if post.get('resto') == 0 else post.get('resto'),
            'op': 1 if post.get('resto') == 0 else 0,
            'timestamp': post.get('time', 0),
            'timestamp_expired': post.get('archived_on', 0),
            'preview_orig': get_fs_filename_thumbnail(post),
            'preview_w': post.get('tn_w', 0),
            'preview_h': post.get('tn_h', 0),
            'media_filename': html.unescape(f"{post.get('filename')}{post.get('ext')}") if post.get('filename') and post.get('ext') else None,
            'media_w': post.get('w', 0),
            'media_h': post.get('h', 0),
            'media_size': post.get('fsize', 0),
            'media_hash': post.get('md5'),
            'media_orig': get_fs_filename_full_media(post),
            'spoiler': post.get('spoiler', 0),
            'deleted': post.get('filedeleted', 0),
            'capcode': convert_to_asagi_capcode(post.get('capcode')),
            'email': post.get('email'),
            'name': html.unescape(post.get('name')) if post.get('name') else None,
            'trip': post.get('trip'),
            'title': html.unescape(post.get('sub')) if post.get('sub') else None,
            'comment': convert_to_asagi_comment(post.get('com')),
            'delpass': post.get('delpass'),
            'sticky': post.get('sticky', 0),
            'locked': post.get('closed', 0),
            'poster_hash': post.get('id'),
            'poster_country': post.get('country_name'),
            'exif': json.dumps({'uniqueIps': int(post.get('unique_ips'))}) if post.get('unique_ips') else None,
        }
        do_upsert(cursor, board, d_board, 'num', 'num')

    d_thread = {
        'thread_num': thread['posts'][0]['no'],
        'time_op': thread['posts'][0]['time'],
        'time_last': thread['posts'][-1]['time'],
        'time_bump': thread['posts'][-1]['time'],
        'time_ghost': None,
        'time_ghost_bump': None,
        'time_last_modified': 0,
        'nreplies': len(thread['posts']) - 1,
        'nimages': len([None for post in thread['posts'] if post_has_file(post)]),
        'sticky': 0,
        'locked': 0,
    }
    do_upsert(cursor, f'{board}_threads', d_thread, 'thread_num', 'thread_num')


def do_upsert(cursor: Cursor, table: str, d: dict, conflict_col: str, returning: str):
    sql_cols = ', '.join(d)
    sql_placeholders = ', '.join(['?'] * len(d))
    sql_conflict = ', '.join([f'{col}=?' for col in d])

    sql = f"""INSERT INTO `{table}` ({sql_cols}) VALUES ({sql_placeholders}) ON CONFLICT({conflict_col}) DO UPDATE SET {sql_conflict} RETURNING {returning};"""
    values = list(d.values())
    parameters = values + values
    cursor.execute(sql, parameters)
    result = cursor.fetchone()
    return result[returning]


def upsert_threads(cursor, board: str, threads: list[dict]):
    for thread in threads:
        upsert_thread(cursor, board, thread)


def download_file(url: str, filename: str):
    content = fetch_file(url)
    if content:
        with open(filename, 'wb') as f:
            f.write(content)
    else:
        raise ValueError(f'No content {url=} {filename=}')


def get_filepath(board: str, media_type, filename: str) -> str:
    tim = filename.split('.')[0]
    assert len(tim) >= 6 and tim[:6].isdigit()
    dir_path = make_path(configs.media_save_path, board, media_type, filename[:4], filename[4:6])
    os.makedirs(dir_path, mode=775, exist_ok=True)
    os.chmod(dir_path, 0o775)
    return os.path.join(dir_path, filename)


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

                    if URL.thumbnail.value is None:
                        configs.logger.info('Warning: this site does not support thumbnail downloads.')

                    url = URL.thumbnail.value.format(board=board, image_id=tim)
                    filename = get_fs_filename_thumbnail(post)
                    filepath = get_filepath(board, MediaType.thumbnail.value, filename)

                elif media_type == MediaType.full_media:
                    board_full_media_pattern = configs.boards[board].get('dl_full_media')
                    if isinstance(board_full_media_pattern, str) and not match_sub_and_com(thread['posts'][0], board_full_media_pattern):
                        continue

                    url = URL.full_media.value.format(board=board, image_id=tim, ext=ext)
                    filename = get_fs_filename_full_media(post)
                    filepath = get_filepath(board, MediaType.full_media.value, filename)

                else:
                    raise ValueError(media_type)

                if (filepath not in DOWNLOADED_MEDIA[board]) and (not os.path.isfile(filepath)):
                    download_file(url, filepath)
                    configs.logger.info(f"[{board}] Downloaded [{media_type.value}] {filepath}")

                DOWNLOADED_MEDIA.add(board, filepath)


def create_non_existing_tables():
    conn = get_connection()
    cursor = conn.cursor()
    for board in configs.boards:
        try:
            sql = f'SELECT * FROM {board} LIMIT 1;'
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


def get_op_threads(threads):
    if not threads:
        return []

    op_threads = []
    for thread in threads:
        for post in thread['posts']:
            if post['resto'] == 0:
                op_threads.append({'posts': [post]})

    return op_threads


def main():
    create_non_existing_tables()

    d_last_modified = dict() # {g: {123: 1717755968, 124: 1717755999}}

    while True:
        conn = get_connection()
        cursor = conn.cursor()

        configs.logger.info(f'Loop Started')
        times = {}
        for board in configs.boards:
            start = time.time()

            catalog = get_catalog(board)
            if not catalog:
                continue

            thread_ids = filter_catalog(board, catalog, d_last_modified)

            threads = get_threads(cursor, board, thread_ids)

            if configs.boards[board].get('thread_text'):
                upsert_threads(cursor, board, threads)
                conn.commit()

            if configs.boards[board].get('dl_full_media_op'):
                op_threads = get_op_threads(threads)
                download_thread_media(board, op_threads, MediaType.full_media)

            if configs.boards[board].get('dl_thumbs_op'):
                op_threads = get_op_threads(threads)
                download_thread_media(board, op_threads, MediaType.thumbnail)

            if configs.boards[board].get('dl_full_media'):
                download_thread_media(board, threads, MediaType.full_media)

            if configs.boards[board].get('dl_thumbs'):
                download_thread_media(board, threads, MediaType.thumbnail)

            times[board] = round((time.time() - start) / 60, 2) # minutes
            configs.reinitialize = True

        cursor.close()
        conn.close()

        configs.logger.info("Loop Completed")
        configs.logger.info("Duration for each board:")

        for board, duration in times.items():
            configs.logger.info(f' {board:<4} {duration:.1f}m')

        total_duration = round(sum(times.values()), 1)
        configs.logger.info(f"Total Duration: {total_duration}m")
        configs.logger.info(f"Going to sleep for {configs.catalog_cooldown_sec}s")

        time.sleep(configs.catalog_cooldown_sec)


if __name__=='__main__':
    main()