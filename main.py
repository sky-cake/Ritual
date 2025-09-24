import html
import os
import re
import time
from collections import defaultdict

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


def get_catalog_from_api(board) -> dict:
    catalog = fetch_json(
        configs.url_catalog.format(board=board),
        headers=configs.headers,
        request_cooldown_sec=configs.request_cooldown_sec,
        add_random=configs.add_random,
        session=configs._session,
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


def get_thread_nums_2_post_ids_from_db(board: str, thread_ids: list[int]) -> tuple[dict, dict, dict]:
    if not thread_ids:
        return {}, {}, {}

    placeholders = get_placeholders(thread_ids)
    sql = f"""select thread_num, num, deleted, media_orig, media_hash from `{board}` where thread_num in ({placeholders});"""
    configs._cursor.execute(sql, thread_ids)
    posts = configs._cursor.fetchall()

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
                d_not_deleted_media[thread_num].append({'no': post['num'], 'tim': post['tim'], 'ext': post['ext'], 'md5': post['media_hash']})

    return d_all, d_not_deleted, d_not_deleted_media


def set_posts_deleted(board: str, post_ids: list[int]) -> None:
    if not post_ids:
        return

    placeholders = get_placeholders(post_ids)
    sql = f"update `{board}` set deleted = 1 where num in ({placeholders});"
    configs._cursor.execute(sql, post_ids)


def get_post_ids_from_thread(thread: dict) -> set[int]:
    return {t['no'] for t in thread}


def get_threads_nums_2_posts_from_api(board: str, thread_ids: list[int], thread_num_2_prev_post_nums_not_deleted: dict[int, list[int]]) -> dict[int, list[dict]]:
    threads_nums_2_posts = dict()
    newly_deleted_post_ids = []

    for thread_id in thread_ids:
        thread = fetch_json(
            configs.url_thread.format(board=board, thread_id=thread_id),
            headers=configs.headers,
            request_cooldown_sec=configs.request_cooldown_sec,
            add_random=configs.add_random,
            session=configs._session,
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
        set_posts_deleted(board, newly_deleted_post_ids)
    return threads_nums_2_posts


def get_placeholders(collection: list) -> str:
    return ','.join(['?'] * len(collection))


def do_upsert(table: str, d: dict, conflict_col: str, returning: str):
    sql_cols = ', '.join(d)
    sql_conflict = ', '.join([f'{col}=?' for col in d])

    sql = f"""insert into `{table}` ({sql_cols}) values ({get_placeholders(d)}) on conflict({conflict_col}) do update set {sql_conflict} returning {returning};"""
    values = list(d.values())
    parameters = values + values
    configs._cursor.execute(sql, parameters)
    result = configs._cursor.fetchone()
    return result[returning]


def do_upsert_many(table: str, rows: list[dict], conflict_col: str, batch_size: int = 155):
    if not rows:
        return

    keys = list(rows[0])
    placeholder = '(' + get_placeholders(keys) + ')'
    sql_cols = ', '.join(keys)
    sql_conflict = ', '.join([f'{k}=excluded.{k}' for k in keys])

    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        placeholders = ', '.join([placeholder] * len(chunk))
        sql = f"""insert into `{table}` ({sql_cols}) values {placeholders} on conflict({conflict_col}) do update set {sql_conflict};"""
        flat_values = [v for row in chunk for v in row.values()]
        configs._cursor.execute(sql, flat_values)


def upsert_thread_num_2_posts(board: str, thread_num_2_posts: dict[int, list[dict]]):
    post_rows = []

    for thread_num, posts in thread_num_2_posts.items():
        for post in posts:
            if post_is_sticky(post):
                continue

            d_board = get_d_board(post, unescape_data_b4_db_write=configs.unescape_data_b4_db_write)
            post_rows.append(d_board)

    do_upsert_many(board, post_rows, 'num')


def match_sub_and_com(post_op: dict, pattern: str):
    sub = post_op.get('sub')
    com = post_op.get('com')

    if sub and re.fullmatch(pattern, sub, re.IGNORECASE):
        return True

    if com and re.fullmatch(pattern, com, re.IGNORECASE):
        return True

    return False


def get_url_and_filename(board: str, post: dict, media_type: MediaType):
    # TODO split out into 2 get()s

    if media_type == MediaType.thumbnail:
        url = configs.url_thumbnail.format(board=board, image_id=post['tim']) # ext is always .jpg
        filename = get_fs_filename_thumbnail(post)

    elif media_type == MediaType.full_media:
        url = configs.url_full_media.format(board=board, image_id=post['tim'], ext=post['ext'])
        filename = get_fs_filename_full_media(post)

    else:
        raise ValueError(media_type)
    
    return url, filename


def download_thread_media_for_post(board: str, thread_op_post: dict, post: dict, media_type: MediaType, qualifier: str):
    """`post` only required `tim` and `ext` keys."""
    if not post_has_file(post):
        return

    # TODO create an enum
    if qualifier not in ('dl_full_media', 'dl_full_media_op', 'dl_thumbs', 'dl_thumbs_op'):
        raise ValueError(qualifier)
    
    qualifier_pattern = configs.boards[board].get(qualifier)

    # the config is-set, and config True test
    if not qualifier_pattern:
        return

    # the config regex test
    if isinstance(qualifier_pattern, str) and not match_sub_and_com(thread_op_post, qualifier_pattern):
        return

    url, filename = get_url_and_filename(board, post, media_type)
    filepath = get_filepath(configs.media_save_path, board, media_type, filename)

    # os.path.isfile is cheap, no need for a cache
    if os.path.isfile(filepath):
        return

    result = download_file(
        url,
        filepath,
        video_cooldown_sec=configs.video_cooldown_sec,
        image_cooldown_sec=configs.image_cooldown_sec,
        add_random=configs.add_random,
        headers=configs.headers,
        logger=configs.logger,
        session=configs._session,
    )

    if not result:
        return

    configs.logger.info(f"[{board}] Downloaded [{media_type.value}] {filepath}")
    if media_type == MediaType.full_media and configs.make_thumbnails:
        thumb_path = get_filepath(configs.media_save_path, board, MediaType.thumbnail, get_fs_filename_thumbnail(post))
        sleep(0.1, add_random=configs.add_random)
        create_thumbnail(post, filepath, thumb_path, logger=configs.logger)


def download_thread_media_for_op(board: str, thread_num_2_op: dict[int, dict], media_type: MediaType, qualifier: str):
    for thread_num, op_post in thread_num_2_op.items():
        download_thread_media_for_post(board, op_post, op_post, media_type, qualifier)


def download_thread_media_for_posts(board: str, thread_num_2_posts: dict[int, dict], thread_num_2_op: dict[int, dict], media_type: MediaType, qualifier: str):
    for thread_num, p in thread_num_2_posts.items():
        thread_op_post = thread_num_2_op[thread_num]
    
        if isinstance(p, list):
            for post in p:
                download_thread_media_for_post(board, thread_op_post, post, media_type, qualifier)

        elif isinstance(p, dict):
            download_thread_media_for_post(board, thread_op_post, p, media_type, qualifier)


def create_non_existing_tables():
    conn = get_connection()
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
    conn.close()


def get_new_posts_map_and_not_deleted_media_posts(board, thread_num_2_op, thread_id_2_catalog_last_replies) -> tuple[dict, dict]:
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
        return {}, {}

    thread_num_2_prev_post_nums_all, thread_num_2_prev_post_nums_not_deleted, thread_num_2_prev_posts_not_deleted_media = get_thread_nums_2_post_ids_from_db(board, thread_ids)

    tf = time.perf_counter()
    configs.logger.info(f'[{board}] Queried database for {len(thread_ids)} threads in {tf-ti:.4f}s')

    thread_num_2_new_posts = dict()
    thread_ids_to_fetch = []

    for thread_id in thread_ids:

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
        board,
        thread_ids_to_fetch,
        thread_num_2_prev_post_nums_not_deleted=thread_num_2_prev_post_nums_not_deleted,
    )

    for thread_id, posts in threads_nums_2_posts.items():
        fetched_post_ids = {p['no'] for p in posts}
        new_posts_ids = fetched_post_ids - thread_num_2_prev_post_nums_all.get(thread_id, set())
        if new_posts_ids:
            thread_num_2_new_posts[thread_id] = [p for p in posts if p['no'] in new_posts_ids]

    return thread_num_2_new_posts, thread_num_2_prev_posts_not_deleted_media


def write_new_posts_and_stats(board: str, thread_num_2_new_posts: dict[int, dict]):
    try:
        upsert_thread_num_2_posts(board, thread_num_2_new_posts)
        configs._cursor.connection.commit()
    except Exception as e:
        configs.logger.error(f'Rolling back and retrying execute()s and commit() in 0.25s due to:\n{e}')
        configs._cursor.connection.rollback()
        sleep(0.25)
        upsert_thread_num_2_posts(board, thread_num_2_new_posts)
        configs._cursor.connection.commit()


def get_thread_nums_2_media_posts(board: str, thread_num_2_new_posts: dict, thread_num_2_prev_posts_not_deleted_media: dict) -> dict:
    # Only mark the posts we just fetched for possible downloading
    if not configs.ensure_all_files_downloaded:
        return thread_num_2_new_posts

    # Media for posts we just fetched, marked for possible downloading
    thread_nums_2_media_posts = defaultdict(list)
    thread_nums_2_media_posts |= thread_num_2_new_posts
    _thread_num_post_num_pairs = set()
    for thread_id, posts in thread_num_2_new_posts.items():
        for post in posts:
            if post_has_file(post):
                _thread_num_post_num_pairs.add((thread_id, post['no']))

    media_count_i = len(_thread_num_post_num_pairs)
    configs.logger.info(f'[{board}] Media count from new posts: {media_count_i}. Post filters not applied yet.')

    # Media for posts we've already archived, marked for possible downloading
    for thread_id, posts in thread_num_2_prev_posts_not_deleted_media.items():
        for post in posts:
            if post_has_file(post) and ((thread_id, post['no']) not in _thread_num_post_num_pairs):
                thread_nums_2_media_posts[thread_id].append(post)
                _thread_num_post_num_pairs.add((thread_id, post['no']))

    media_count_f = len(_thread_num_post_num_pairs)
    configs.logger.info(f'[{board}] Media count from existing posts: {media_count_f - media_count_i}. Post filters not applied yet.')

    return thread_nums_2_media_posts


def get_cached_d_last_modified(fpath_d_last_modified: str):
    """{g: {123: 1717755968, 124: 1717755999}, ck: {456: 1717755968}, ...}"""
    d_last_modified = read_json(fpath_d_last_modified)

    if not d_last_modified:
        return dict()
    
    # convert keys to ints
    return {
        k_board_name: {
            int(k_thread_num): v_last_modified
            for k_thread_num, v_last_modified in thread_num_2_last_modified.items()
        }
        for k_board_name, thread_num_2_last_modified in d_last_modified.items()
    }


def remove_images_already_downloaded(board: str, thread_num_2_media_posts: dict) -> dict:
    """
    Each (board, media_hash, is_op) state gets at most 1 filesystem path.
    This query helps get us that fs path, as well as avoid banned media hashes.
    """
    if not thread_num_2_media_posts:
        return thread_num_2_media_posts

    media_hashes = set()
    for p in thread_num_2_media_posts.values():
        if isinstance(p, list):
            for post in p:
                if media_hash := post.get('md5'):
                    media_hashes.add(media_hash)
        elif isinstance(p, dict):
            if media_hash := p.get('md5'):
                media_hashes.add(media_hash)
    
    media_hashes = tuple(media_hashes)

    sql_string = f'''
    select
        `media_hash`, `media`, `preview_op`, `preview_reply`, `total`, `banned`
    from `{board}_images`
    where
        media_hash in ({get_placeholders(media_hashes)})
    '''
    rows = configs._cursor.execute(sql_string, media_hashes).fetchall()

    hash_2_image_row = {row['media_hash']: row for row in rows}

    d = dict()
    for thread_num, post in thread_num_2_media_posts.items():
        if isinstance(post, dict):
            if not (media_hash := post.get('md5')):
                continue

            image_row = hash_2_image_row.get(media_hash, {})

            # banned hash, skip it
            if image_row['banned'] != 0:
                continue

            # don't have this hash, mark for possible downloading
            if media_hash not in hash_2_image_row:
                d[thread_num] = post
                continue

            # found hash in the db, but no file in the fs, mark for possible downloading
            full_filename = get_fs_filename_full_media(post)
            full_path = get_filepath(configs.media_save_path, board, MediaType.full_media, full_filename)
            if not os.path.isfile(full_path):
                d[thread_num] = post
                continue

            # found hash in the db, but no file in the fs, mark for possible downloading
            thumb_filename = get_fs_filename_thumbnail(post)
            thumb_path = get_filepath(configs.media_save_path, board, MediaType.thumbnail, thumb_filename)
            if not os.path.isfile(thumb_path):
                d[thread_num] = post
                continue

        elif isinstance(post, list):
            for p in post:
                if not (media_hash := p.get('md5')):
                    continue

                image_row = hash_2_image_row.get(media_hash, {})

                # banned hash, skip it
                if image_row['banned'] != 0:
                    continue

                # don't have this hash, mark for possible downloading
                if media_hash not in hash_2_image_row:
                    d[thread_num] = p
                    continue

                # found hash in the db, but no file in the fs, mark for possible downloading
                full_filename = get_fs_filename_full_media(p)
                full_path = get_filepath(configs.media_save_path, board, MediaType.full_media, full_filename)
                if not os.path.isfile(full_path):
                    d[thread_num] = p
                    continue

                # found hash in the db, but no file in the fs, mark for possible downloading
                thumb_filename = get_fs_filename_thumbnail(p)
                thumb_path = get_filepath(configs.media_save_path, board, MediaType.thumbnail, thumb_filename)
                if not os.path.isfile(thumb_path):
                    d[thread_num] = p
                    continue

    return d


def main():
    if configs.make_thumbnails:
        test_deps(configs.logger)

    create_non_existing_tables()

    fpath_d_last_modified = make_path('cache', 'd_last_modified.json')
    d_last_modified = get_cached_d_last_modified(fpath_d_last_modified)

    is_first_loop = True
    loop_i = 1

    configs._session = Session()

    while True:
        configs._conn = get_connection()
        configs._cursor = configs._conn.cursor()

        configs.logger.info(f'\nLoop #{loop_i} Started')
        times = {}
        for board in tqdm.tqdm(configs.boards, disable=configs.disable_tqdm):
            start = time.time()

            catalog = get_catalog_from_api(board)
            if not catalog:
                configs.logger.info(f"Catalog returned {catalog}")
                continue

            # 5 lastest replies per thread are free, at least when archiving 4chan and vichan archives
            thread_id_2_catalog_last_replies = get_thread_id_2_last_replies(catalog)

            # these are the threads we want to archive
            thread_num_2_op = filter_catalog(board, catalog, d_last_modified, is_first_loop)
            if not thread_num_2_op:
                continue


            thread_num_2_new_posts, thread_num_2_prev_posts_not_deleted_media = get_new_posts_map_and_not_deleted_media_posts(board, thread_num_2_op, thread_id_2_catalog_last_replies)

            # write the new records to the database
            if thread_num_2_new_posts and configs.boards[board].get('thread_text'):
                write_new_posts_and_stats(board, thread_num_2_new_posts)


            if not thread_num_2_new_posts and not configs.ensure_all_files_downloaded:
                configs.logger.info(f'[{board}] No new posts found.')
                continue


            thread_nums_2_media_posts = get_thread_nums_2_media_posts(board, thread_num_2_new_posts, thread_num_2_prev_posts_not_deleted_media)


            thread_num_2_op           = remove_images_already_downloaded(board, thread_num_2_op)
            thread_nums_2_media_posts = remove_images_already_downloaded(board, thread_nums_2_media_posts)


            if configs.boards[board].get('dl_full_media_op'):
                download_thread_media_for_op(board, thread_num_2_op, MediaType.full_media, 'dl_full_media_op')

            if configs.boards[board].get('dl_full_media'):
                download_thread_media_for_posts(board, thread_nums_2_media_posts, thread_num_2_op, MediaType.full_media, 'dl_full_media')

            # "if we're not making OP thumbs [with Convert or FFMPEG] from OP full media"
            if not (configs.make_thumbnails and configs.boards[board].get('dl_full_media_op')):
                if configs.boards[board].get('dl_thumbs_op'):
                    download_thread_media_for_op(board, thread_num_2_op, MediaType.thumbnail, 'dl_thumbs_op')

            # "if we're not making thumbs [with Convert or FFMPEG] from full media"
            if not (configs.make_thumbnails and configs.boards[board].get('dl_full_media')):
                if configs.boards[board].get('dl_thumbs'):
                    download_thread_media_for_posts(board, thread_nums_2_media_posts, thread_num_2_op, MediaType.thumbnail, 'dl_thumbs')

            times[board] = round((time.time() - start) / 60, 2) # minutes

        is_first_loop = False

        configs._cursor.close()
        configs._conn.close()

        configs.logger.info(f"\nLoop #{loop_i} Completed")
        configs.logger.info("Duration for each board:")

        for board, duration in times.items():
            configs.logger.info(f'    - {board:<4} {duration:.1f}m')

        total_duration = round(sum(times.values()), 1)
        configs.logger.info(f"Total Duration: {total_duration}m")
        configs.logger.info(f"Doing loop cooldown sleep for {configs.loop_cooldown_sec}s")

        write_json(fpath_d_last_modified, d_last_modified)

        if configs.loop_cooldown_sec >= 15.0:
            configs._session.close()

        loop_i += 1
        time.sleep(configs.loop_cooldown_sec)


if __name__=='__main__':
    main()
