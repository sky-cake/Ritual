import html
import os
import re
import time
from collections import defaultdict
from sqlite3 import Cursor

import tqdm
from requests import Session

import configs
from asagi import (
    create_thumbnail,
    get_filepath,
    get_fs_filename_full_media,
    get_fs_filename_thumbnail,
    post_has_file,
    post_is_sticky,
    get_d_board, 
    get_d_image,
    get_thread_id_2_last_replies
)
from db import get_connection
from enums import MediaType
from utils import (
    download_file,
    fetch_json,
    make_path,
    sleep,
    test_deps,
    read_json,
    write_json,
)


def get_catalog_from_api(board, session: Session=None) -> dict:
    catalog = fetch_json(
        configs.url_catalog.format(board=board),
        headers=configs.headers,
        request_cooldown_sec=configs.request_cooldown_sec,
        add_random=configs.add_random,
        session=session,
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
    thread_id = thread['no']
    thread_last_modified = thread.get('last_modified')

    if board not in d_last_modified:
        d_last_modified[board] = {}

    thread_last_modified_cached = d_last_modified[board].get(thread_id)

    # Update the thread's last modified time in d_last_modified.
    d_last_modified[board][thread_id] = thread_last_modified

    # Don't let the dict grow over N entries per board.
    N = 200
    if len(d_last_modified[board]) > N:
        # In case of multiple stickies, or similar special threads, we delete M oldest threads
        M = 10
        tmp_nos = sorted(d_last_modified[board], key=lambda no: d_last_modified[board][no])
        for stale_no in tmp_nos[:M]:
            del d_last_modified[board][stale_no]

    # last_modified changed
    if thread_last_modified and thread_last_modified_cached and thread_last_modified != thread_last_modified_cached:
        return True

    # new thread
    if thread_last_modified_cached is None:
        return True

    return False


def filter_catalog(board: str, catalog: dict, d_last_modified: dict, is_first_loop: bool) -> dict[int, dict]:
    thread_id_2_thread = dict()
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
                thread_id_2_thread[thread['no']] = thread
                thread_modified(board, thread, d_last_modified) # populate `d_last_modified`
                continue

            if not thread_modified(board, thread, d_last_modified):
                not_modified_thread_count += 1
                continue

            thread_id_2_thread[thread['no']] = thread

    m = f'{not_modified_thread_count} thread(s) are unmodified. ' if not_modified_thread_count else ''
    if reinitializing:
        m = 'Ignoring last modified timestamps on first loop. '

    configs.logger.info(f'[{board}] {m}{len(thread_id_2_thread.keys())} thread(s) are modified and will be queued.')

    return thread_id_2_thread


def get_thread_nums_2_post_ids_from_db(cursor: Cursor, board: str, thread_ids: list[int]) -> tuple[dict, dict, dict]:
    if not thread_ids:
        return {}, {}, {}

    placeholders = ','.join(['?'] * len(thread_ids))
    sql = f"""select thread_num, num, deleted, media_orig from `{board}` where thread_num in ({placeholders});"""
    cursor.execute(sql, thread_ids)
    posts = cursor.fetchall()

    d_all = defaultdict(set)
    d_not_deleted = defaultdict(set)
    d_not_deleted_media = defaultdict(list)

    for post in posts:
        thread_num = post['thread_num']
        d_all[thread_num].add(post['num'])

        if not post['deleted']:
            d_not_deleted[thread_num].add(post['num'])

            if post.get('media_orig'):
                post['tim'], post['ext'] = post['media_orig'].split('.')
                post['ext'] = '.' + post['ext']

                # use API key names
                d_not_deleted_media[thread_num].append({'no': post['num'], 'tim': post['tim'], 'ext': post['ext']})

    return d_all, d_not_deleted, d_not_deleted_media


def set_posts_deleted(cursor: Cursor, board: str, post_ids: list[int]) -> None:
    if not post_ids:
        return

    placeholders = ','.join(['?'] * len(post_ids))
    sql = f"update `{board}` set deleted = 1 where num in ({placeholders});"
    cursor.execute(sql, post_ids)


def get_post_ids_from_thread(thread: dict) -> set[int]:
    return {t['no'] for t in thread}


def get_threads_nums_2_posts_from_api(cursor: Cursor, board: str, thread_ids: list[int], thread_num_2_prev_post_nums_not_deleted: dict[int, list[int]], session: Session=None) -> dict[int, list[dict]]:
    threads_nums_2_posts = dict()
    newly_deleted_post_ids = []

    for thread_id in thread_ids:
        thread = fetch_json(
            configs.url_thread.format(board=board, thread_id=thread_id),
            headers=configs.headers,
            request_cooldown_sec=configs.request_cooldown_sec,
            add_random=configs.add_random,
            session=session,
        )
        if thread:
            configs.logger.info(f'[{board}] Found thread [{thread_id}]')

            current_post_ids = get_post_ids_from_thread(thread['posts'])
            previous_post_ids = thread_num_2_prev_post_nums_not_deleted.get(thread_id, [])
            for post_id in previous_post_ids:
                if post_id not in current_post_ids:
                    newly_deleted_post_ids.append(post_id)
                    configs.logger.info(f'[{board}] Post Deleted [{thread_id}] [{post_id}]')

            threads_nums_2_posts[thread_id] = thread['posts']
        else:
            configs.logger.info(f'[{board}] Lost Thread [{thread_id}]')

    if newly_deleted_post_ids:
        set_posts_deleted(cursor, board, newly_deleted_post_ids)
    return threads_nums_2_posts


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


def upsert_thread_num_2_posts(cursor, board: str, thread_num_2_posts: dict[int, list[dict]], thread_num_2_stats: dict[int, dict]):
    thread_rows = []
    post_rows = []

    for thread_num, posts in thread_num_2_posts.items():
        d_thread = {
            'thread_num': thread_num,
            'time_op': thread_num_2_stats[thread_num]['time_op'],
            'time_last': posts[-1]['time'],
            'time_bump': posts[-1]['time'],
            'time_ghost': None,
            'time_ghost_bump': None,
            'time_last_modified': 0,
            'nreplies': thread_num_2_stats[thread_num]['nreplies'],
            'nimages': thread_num_2_stats[thread_num]['nimages'],
            'sticky': 0,
            'locked': 0,
        }
        thread_rows.append(d_thread)

        for post in posts:
            if post_is_sticky(post):
                continue

            media_id = None
            if post_has_file(post):
                is_op = post['resto'] == 0
                d_image = get_d_image(post, is_op)
                media_id = do_upsert(cursor, f'{board}_images', d_image, 'media_hash', 'media_id')
                assert media_id

            d_board = get_d_board(post, media_id, unescape_data_b4_db_write=configs.unescape_data_b4_db_write)
            post_rows.append(d_board)

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


def download_thread_media_for_post(board: str, thread_op_post: dict, post: dict, media_type: MediaType, session: Session=None):
    """`post` only required `tim` and `ext` keys."""
    if post_has_file(post):
        tim = post['tim']
        ext = post['ext']

        if media_type == MediaType.thumbnail:
            board_thumb_pattern = configs.boards[board].get('dl_thumbs')
            if isinstance(board_thumb_pattern, str) and not match_sub_and_com(thread_op_post, board_thumb_pattern):
                return

            if configs.url_thumbnail is None:
                configs.logger.info('Warning: this site does not support thumbnail downloads.')

            url = configs.url_thumbnail.format(board=board, image_id=tim)
            filename = get_fs_filename_thumbnail(post)
            filepath = get_filepath(configs.media_save_path, board, MediaType.thumbnail.value, filename)

        elif media_type == MediaType.full_media:
            board_full_media_pattern = configs.boards[board].get('dl_full_media')
            if isinstance(board_full_media_pattern, str) and not match_sub_and_com(thread_op_post, board_full_media_pattern):
                return

            url = configs.url_full_media.format(board=board, image_id=tim, ext=ext)
            filename = get_fs_filename_full_media(post)
            filepath = get_filepath(configs.media_save_path, board, MediaType.full_media.value, filename)

        else:
            raise ValueError(media_type)

        # os.path.isfile is cheap, no need for a cache
        if not os.path.isfile(filepath):
            result = download_file(
                url,
                filepath,
                video_cooldown_sec=configs.video_cooldown_sec,
                image_cooldown_sec=configs.image_cooldown_sec,
                add_random=configs.add_random,
                headers=configs.headers,
                logger=configs.logger,
                session=session,
            )
            if result:
                configs.logger.info(f"[{board}] Downloaded [{media_type.value}] {filepath}")
                if media_type == MediaType.full_media and configs.make_thumbnails:
                    thumb_path = get_filepath(configs.media_save_path, board, MediaType.thumbnail.value, get_fs_filename_thumbnail(post))
                    sleep(0.1, add_random=configs.add_random)
                    create_thumbnail(post, filepath, thumb_path, logger=configs.logger)


def download_thread_media_for_op(board: str, thread_num_2_op: dict[int, dict], media_type: MediaType, session: Session=None):
    for thread_num, op_post in thread_num_2_op.items():
        download_thread_media_for_post(board, op_post, op_post, media_type, session=session)


def download_thread_media_for_posts(board: str, thread_num_2_posts: dict[int, dict], thread_num_2_op: dict[int, dict], media_type: MediaType, session: Session=None):
    for thread_num, posts in thread_num_2_posts.items():
        thread_op_post = thread_num_2_op[thread_num]
        for post in posts:
            download_thread_media_for_post(board, thread_op_post, post, media_type, session=session)


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


def get_new_posts_map_and_latest_stats_map_and_not_deleted_media_posts(cursor, board, thread_num_2_op, thread_id_2_catalog_last_replies, session: Session=None) -> tuple[dict, dict, dict]:
    """
    In previous versions of Ritual, we would just grab entire threads, as toss them at sqlite like it wasn't our problem.
    Now, we carefully try to select posts we have not yet archived, and gently hand them to sqlite.

    - existing posts: already archived posts, marks as not deleted
    - new posts: posts we just fetched from the api
    - stats: the latest stats for a thread, fetched from the api
    """
    # query the database to see which of threads we already have, if any
    ti = time.perf_counter()
    thread_ids = list(thread_num_2_op.keys())
    if not thread_ids:
        return {}, {}, {}

    thread_num_2_prev_post_nums_all, thread_num_2_prev_post_nums_not_deleted, thread_num_2_prev_posts_not_deleted_media = get_thread_nums_2_post_ids_from_db(cursor, board, thread_ids)

    tf = time.perf_counter()
    configs.logger.info(f'[{board}] Queried database for {len(thread_ids)} threads in {tf-ti:.4f}s')

    thread_num_2_new_posts = dict()
    thread_num_2_stats = dict()
    thread_ids_to_fetch = []

    for thread_id in thread_ids:

        stats = thread_num_2_op[thread_id]
        thread_num_2_stats[thread_id] = {
            'time_op': stats['time'],
            'nreplies': stats['replies'],
            'nimages': stats['images'],
        }

        catalog_last_replies = thread_id_2_catalog_last_replies.get(thread_id, [])
        catalog_last_reply_nums = set((r['no'] for r in catalog_last_replies))

        archive_nums = thread_num_2_prev_post_nums_all.get(thread_id, set())

        # catalog-last-replies not supported by API, or new thread, or something else - just fetch it
        if not catalog_last_reply_nums or not archive_nums:
            thread_ids_to_fetch.append(thread_id)
            continue

        # all matched, nothing new
        if catalog_last_reply_nums.issubset(archive_nums):
            continue

        # any matched
        if catalog_last_reply_nums & archive_nums:
            nums_not_in_archive = catalog_last_reply_nums - archive_nums
            thread_num_2_new_posts[thread_id] = [r for r in catalog_last_replies if r['no'] in nums_not_in_archive]
            continue

        # none matched
        thread_ids_to_fetch.append(thread_id)

    threads_nums_2_posts = get_threads_nums_2_posts_from_api(
        cursor,
        board,
        thread_ids_to_fetch,
        thread_num_2_prev_post_nums_not_deleted=thread_num_2_prev_post_nums_not_deleted,
        session=session,
    )

    for thread_id, posts in threads_nums_2_posts.items():
        fetched_post_ids = {p['no'] for p in posts}
        new_posts_ids = fetched_post_ids - thread_num_2_prev_post_nums_all.get(thread_id, set())
        if new_posts_ids:
            thread_num_2_new_posts[thread_id] = [p for p in posts if p['no'] in new_posts_ids]

    return thread_num_2_new_posts, thread_num_2_stats, thread_num_2_prev_posts_not_deleted_media


def write_new_posts_and_stats(cursor: Cursor, board: str, thread_num_2_new_posts: dict[int, dict], thread_num_2_stats: dict[int, dict]):
    try:
        upsert_thread_num_2_posts(cursor, board, thread_num_2_new_posts, thread_num_2_stats)
        cursor.connection.commit()
    except Exception as e:
        configs.logger.error(f'Rolling back and retrying execute()s and commit() due to:\n{e}')
        cursor.connection.rollback()
        sleep(0.25)
        upsert_thread_num_2_posts(cursor, board, thread_num_2_new_posts, thread_num_2_stats)
        cursor.connection.commit()


def get_thread_nums_2_media_posts(board: str, thread_num_2_new_posts: dict, thread_num_2_prev_posts_not_deleted_media: dict) -> dict:
    if not configs.ensure_all_files_downloaded:
        # download media for the posts we just fetched
        return thread_num_2_new_posts
    
    if not configs.boards[board].get('dl_full_media') and not configs.boards[board].get('dl_thumbs'):
        # We don't need to download any media for posts.
        # OP-only specified with dl_full_media_op and dl_thumbs_op are handled separately.
        return {}

    # Download media for the posts we just fetched,
    # but also add already archived posts-with-media, not marked as deleted, for possible downloading.
    thread_nums_2_media_posts = defaultdict(list)
    thread_nums_2_media_posts |= thread_num_2_new_posts
    _thread_num_post_num_pairs = set()
    for thread_id, posts in thread_num_2_new_posts.items():
        for post in posts:
            if post_has_file(post):
                _thread_num_post_num_pairs.add((thread_id, post['no']))

    media_count_i = len(_thread_num_post_num_pairs)
    configs.logger.info(f'[{board}] Media count from new posts: {media_count_i}. Post filters not applied yet.')

    for thread_id, posts in thread_num_2_prev_posts_not_deleted_media.items():
        for post in posts:
            if post_has_file(post) and ((thread_id, post['no']) not in _thread_num_post_num_pairs):
                thread_nums_2_media_posts[thread_id].append(post)
                _thread_num_post_num_pairs.add((thread_id, post['no']))

    media_count_f = len(_thread_num_post_num_pairs)
    configs.logger.info(f'[{board}] Media count from existing posts: {media_count_f - media_count_i}. Post filters not applied yet.')

    return thread_nums_2_media_posts


def main():
    if configs.make_thumbnails:
        test_deps(configs.logger)

    create_non_existing_tables()

    # {g: {123: 1717755968, 124: 1717755999}}
    fpath_d_last_modified = make_path('cache', 'd_last_modified')
    d_last_modified = read_json(fpath_d_last_modified)
    if not d_last_modified:
        d_last_modified = dict()

    is_first_loop = True
    loop_i = 1

    session = Session()

    while True:
        conn = get_connection()
        cursor = conn.cursor()

        configs.logger.info(f'\nLoop #{loop_i} Started')
        times = {}
        for board in tqdm.tqdm(configs.boards, disable=configs.disable_tqdm):
            start = time.time()

            catalog = get_catalog_from_api(board, session=session)
            if not catalog:
                configs.logger.info(f"Catalog returned {catalog}")
                continue

            # 5 lastest replies per thread are free, at least when archiving 4chan and vichan archives
            thread_id_2_catalog_last_replies = get_thread_id_2_last_replies(catalog)

            # these are the threads we want to archive
            thread_num_2_op = filter_catalog(board, catalog, d_last_modified, is_first_loop)
            if not thread_num_2_op:
                continue


            thread_num_2_new_posts, thread_num_2_stats, thread_num_2_prev_posts_not_deleted_media = get_new_posts_map_and_latest_stats_map_and_not_deleted_media_posts(cursor, board, thread_num_2_op, thread_id_2_catalog_last_replies, session=session)

            # write the new records to the database
            if thread_num_2_new_posts and configs.boards[board].get('thread_text'):
                write_new_posts_and_stats(cursor, board, thread_num_2_new_posts, thread_num_2_stats)


            if not thread_num_2_new_posts and not configs.ensure_all_files_downloaded:
                configs.logger.info(f'[{board}] No new posts found.')
                continue


            thread_nums_2_media_posts = get_thread_nums_2_media_posts(board, thread_num_2_new_posts, thread_num_2_prev_posts_not_deleted_media)

            if configs.boards[board].get('dl_full_media_op'):
                download_thread_media_for_op(board, thread_num_2_op, MediaType.full_media, session=session)

            if configs.boards[board].get('dl_full_media'):
                download_thread_media_for_posts(board, thread_nums_2_media_posts, thread_num_2_op, MediaType.full_media, session=session)

            # only dl thumbs if we are not instructed to generate them with Convert or FFMPEG
            if configs.boards[board].get('dl_thumbs_op') and not (configs.make_thumbnails and configs.boards[board].get('dl_full_media_op') and configs.boards[board].get('dl_full_media')):
                download_thread_media_for_op(board, thread_num_2_op, MediaType.thumbnail, session=session)

            if configs.boards[board].get('dl_thumbs') and not (configs.make_thumbnails and configs.boards[board].get('dl_full_media_op') and configs.boards[board].get('dl_full_media')):
                download_thread_media_for_posts(board, thread_nums_2_media_posts, thread_num_2_op, MediaType.thumbnail, session=session)

            times[board] = round((time.time() - start) / 60, 2) # minutes

        is_first_loop = False

        cursor.close()
        conn.close()

        configs.logger.info(f"\nLoop #{loop_i} Completed")
        configs.logger.info("Duration for each board:")

        for board, duration in times.items():
            configs.logger.info(f'    - {board:<4} {duration:.1f}m')

        total_duration = round(sum(times.values()), 1)
        configs.logger.info(f"Total Duration: {total_duration}m")
        configs.logger.info(f"Doing loop cooldown sleep for {configs.loop_cooldown_sec}s")

        write_json(fpath_d_last_modified, d_last_modified)

        if configs.loop_cooldown_sec >= 15.0:
            session.close()

        loop_i += 1
        time.sleep(configs.loop_cooldown_sec)


if __name__=='__main__':
    main()
