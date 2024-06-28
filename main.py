import html
import json
import os
import random
import re
import time
from sqlite3 import Cursor

import requests

import configs
from db import get_connection
from defs import URL, MediaType
from utils import convert_to_asagi_capcode, convert_to_asagi_comment, make_path

DOWNLOADED_MEDIA = set()


def get_headers():
    headers = None
    if configs.user_agent:
        headers = {'User-Agent', configs.user_agent}
    return headers


def sleep(t=None):
    if t:
        time.sleep(t)
        return

    s = configs.request_cooldown_sec
    if configs.add_random:
        s += random.uniform(0.0, 1.0)
    time.sleep(s)


def fetch_json(url):
    try:
        resp = requests.get(url, headers=get_headers())
        sleep()

        if resp.status_code == 200:
            return resp.json()
    except Exception:
        sleep(20)


def fetch_file(url):
    try:
        resp = requests.get(url, headers=get_headers())
        sleep()

        if resp.status_code == 200:
            return resp.content
    except Exception:
        sleep(20)


def get_catalog(board):
    catalog = fetch_json(URL.catalog.value.format(board=board))
    configs.logger.info(f'Fetch catalog [{board}]')
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


def filter_catalog(board, catalog):
    thread_ids = []
    for page in catalog:
        for thread in page['threads']:

            if post_is_sticky(thread):
                continue

            subject = html.unescape(thread.get('sub', ''))
            comment = html.unescape(thread.get('com', ''))
            if not should_archive(board, subject, comment):
                continue

            thread_ids.append(thread.get('no'))
    return thread_ids


def get_non_deleted_post_ids_for_thread_num(cursor: Cursor, board, thread_id):
    sql = f"""select num from {board} where thread_num = ? and (deleted is null or deleted = 0);"""
    parameters = [thread_id]
    cursor.execute(sql, parameters)
    results = cursor.fetchall()
    post_ids = [r['num'] for r in results]
    return post_ids


def set_posts_deleted(cursor: Cursor, board, post_ids):
    sql = f"""update {board} set deleted = 1 where num = ?;"""
    cursor.executemany(sql, [(p,) for p in post_ids])


def get_post_ids_from_thread(thread):
    return [t['no'] for t in thread['posts']]


def get_threads(cursor, board, thread_ids):
    threads = []
    deleted_post_ids = []
    for thread_id in thread_ids[30:50]:
        thread = fetch_json(URL.thread.value.format(board=board, thread_id=thread_id))
        if thread:
            configs.logger.info(f'Fetched thread [{board}] [{thread_id}]')

            previous_post_ids = get_non_deleted_post_ids_for_thread_num(cursor, board, thread_id)
            if previous_post_ids:
                current_post_ids = get_post_ids_from_thread(thread)
                for post_id in previous_post_ids:
                    if post_id not in current_post_ids:
                        deleted_post_ids.append(post_id)

            threads.append(thread)
        else:
            configs.logger.info(f'Lost Thread [{board}] [{thread_id}]')

    if deleted_post_ids:
        set_posts_deleted(cursor, board, deleted_post_ids)
    return threads


def post_has_file(post):
    return post.get('tim') and post.get('ext') and post.get('md5')


def get_fs_filename_full_media(post):
    if post_has_file(post):
        return f"{post.get('tim')}{post.get('ext')}"


def get_fs_filename_thumbnail(post):
    if post_has_file(post):
        return f"{post.get('tim')}s.jpg"


def post_is_sticky(post):
    return post.get('sticky') == 1


def upsert_thread(cursor, board, thread):
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
                # 'total': post.get('total'), # archive attribute
                # 'banned': post.get('banned'), # archive attribute
            }
            media_id = do_upsert(cursor, f'{board}_images', d_image, 'media_hash', 'media_id')
            assert media_id

        d_board = {
            # 'doc_id': post.get('doc_id'), # autoincremented
            'media_id': media_id,
            'poster_ip': post.get('poster_ip'),
            'num': post.get('no'),
            'subnum': post.get('subnum'),
            'thread_num': post.get('no') if post.get('resto') == 0 else post.get('resto'),
            'op': 1 if post.get('resto') == 0 else 0,
            'timestamp': post.get('time'),
            'timestamp_expired': post.get('archived_on'),
            'preview_orig': get_fs_filename_thumbnail(post),
            'preview_w': post.get('tn_w'),
            'preview_h': post.get('tn_h'),
            'media_filename': html.unescape(f"{post.get('filename')}{post.get('ext')}") if post.get('filename') and post.get('ext') else None,
            'media_w': post.get('w'),
            'media_h': post.get('h'),
            'media_size': post.get('fsize'),
            'media_hash': post.get('md5'),
            'media_orig': get_fs_filename_full_media(post),
            'spoiler': post.get('spoiler'),
            'deleted': post.get('filedeleted'),
            'capcode': convert_to_asagi_capcode(post.get('capcode')),
            'email': post.get('email'),
            'name': html.unescape(post.get('name')) if post.get('name') else None,
            'trip': post.get('trip'),
            'title': html.unescape(post.get('sub')) if post.get('sub') else None,
            'comment': convert_to_asagi_comment(post.get('com')),
            'delpass': post.get('delpass'),
            'sticky': post.get('sticky'),
            'locked': post.get('closed'),
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
        'time_last_modified': None,
        'nreplies': len(thread['posts']) - 1,
        'nimages': len([None for post in thread['posts'] if post_has_file(post)]),
        'sticky': None,
        'locked': None,
    }
    do_upsert(cursor, f'{board}_threads', d_thread, 'thread_num', 'thread_num')


def do_upsert(cursor, table, d, conflict_col, returning):
    sql_cols = ', '.join(d)
    sql_placeholders = ', '.join(['?'] * len(d))
    sql_conflict = ', '.join([f'{col}=?' for col in d])

    sql = f"""INSERT INTO `{table}` ({sql_cols}) VALUES ({sql_placeholders}) ON CONFLICT({conflict_col}) DO UPDATE SET {sql_conflict} RETURNING {returning};"""
    values = list(d.values())
    parameters = values + values
    cursor.execute(sql, parameters)
    result = cursor.fetchone()
    return result[returning]


def upsert_threads(cursor, board, threads):
    for thread in threads:
        upsert_thread(cursor, board, thread)


def download_file(url, filename):
    content = fetch_file(url)
    if content:
        with open(filename, 'wb') as f:
            f.write(content)


def get_filepath(board, media_type, filename):
    tim = filename.split('.')[0]
    assert len(tim) >= 6 and tim[:6].isdigit()
    dir_path = make_path(configs.media_save_path, board, media_type, filename[:4], filename[4:6])
    os.makedirs(dir_path, mode=775, exist_ok=True)
    os.chmod(dir_path, 0o775)
    return os.path.join(dir_path, filename)


def match_sub_and_com(post_op, pattern):
    sub = post_op.get('sub')
    com = post_op.get('com')

    if sub and re.fullmatch(pattern, sub, re.IGNORECASE):
        return True
    
    if com and re.fullmatch(pattern, com, re.IGNORECASE):
        return True
    
    return False


def download_thread_media(board, threads, media_type):
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

                if (filepath not in DOWNLOADED_MEDIA) and (not os.path.isfile(filepath)):
                    download_file(url, filepath)
                    configs.logger.info(f"Downloaded [{board}] [{post.get('no')}] {media_type.value} - {filepath}")

                DOWNLOADED_MEDIA.add(filepath)


def create_non_existing_tables():
    conn = get_connection()
    cursor = conn.cursor()
    for board in configs.boards:
        try:
            sql = f'SELECT * FROM {board} LIMIT 1;'
            conn.execute(sql)
            configs.logger.info(f'{board} tables already exist.')
        except Exception:
            configs.logger.info(f'Creating tables for {board}.')
            with open(make_path('schema.sql')) as f:
                sql = f.read()
            sqls = sql.replace('%%BOARD%%', board).split(';')
            for sql in sqls:
                conn.execute(sql)
            conn.commit()
    cursor.close()
    conn.close()


def main():
    create_non_existing_tables()

    while True:
        start = time.time()
        conn = get_connection()
        cursor = conn.cursor()

        for board in configs.boards:
            catalog = get_catalog(board)
            if not catalog:
                continue

            thread_ids = filter_catalog(board, catalog)

            threads = get_threads(cursor, board, thread_ids)

            if configs.boards[board].get('thread_text'):
                upsert_threads(cursor, board, threads)
                conn.commit()

            if configs.boards[board].get('dl_full_media'):
                download_thread_media(board, threads, MediaType.full_media)

            if configs.boards[board].get('dl_thumbs'):
                download_thread_media(board, threads, MediaType.thumbnail)

        cursor.close()
        conn.close()

        configs.logger.info(f'Loop duration: {time.time() - start}s')

        configs.logger.info(f'Sleepy time for {configs.catalog_cooldown_sec}s')
        time.sleep(configs.catalog_cooldown_sec)


if __name__=='__main__':
    main()