import os
import traceback

import configs
from archive import Archive
from catalog import Catalog
from db.ritual import RitualDb, create_ritual_db
from fetcher import Fetcher
from filter import Filter
from loop import Loop
from media_fp import AsagiMediaFP, SutraMediaFP, MediaFP
from posts import Posts
from state import State
from utils import (
    fetch_and_save_boards_json,
    load_boards_with_archive,
    make_path,
    read_json,
    sleep,
    assert_thumbnail_deps
)


class Init:
    def __init__(self):
        if configs.make_thumbnails:
            assert_thumbnail_deps(configs.logger)

        boards_json_path = make_path('cache', 'boards.json')
        if os.path.isfile(boards_json_path):
            boards_json = read_json(boards_json_path)
            configs.logger.info(f'Loaded boards.json from {boards_json_path}')
        else:
            boards_json = fetch_and_save_boards_json(boards_json_path, configs.url_boards, configs.logger)

        configs.boards_with_archive = load_boards_with_archive(boards_json)

        if not configs.boards_with_archive:
            raise ValueError(configs.boards_with_archive)

        configs.logger.info(f'{len(configs.boards_with_archive)} boards have archive support')


def process_board(board: str, db: RitualDb, fetcher: Fetcher, loop: Loop, state: State, media_fp: MediaFP):
    loop.set_start_time()

    catalog = Catalog(fetcher, board)
    if not catalog.fetch_catalog():
        return

    state.update_thread_meta(board, catalog.tid_2_page, catalog.tid_2_thread)

    # results in a max of one archive.json endpoint fetch per loop
    archive = Archive(fetcher, board)

    filter = Filter(fetcher, db, board, state)
    filter.filter_catalog(catalog)

    posts = Posts(db, fetcher, board, filter.tid_2_thread, state, catalog)
    posts.fetch_posts(archive)

    if configs.boards[board].get('thread_text') != False:
        posts.save_posts()

    filter.set_tid_2_posts(posts.tid_2_posts)
    filter.get_pids_for_download()

    media_fp.download_media_for_ids(board, posts.pid_2_post, filter.full_pids, filter.thumb_pids)
    media_fp.flush(board)

    loop.set_board_duration_minutes(board)


def save_on_error(state: State, db: RitualDb, media_fp: MediaFP, board: str):
    configs.logger.info('Saving State...')
    state.save()
    configs.logger.info('  Done')

    configs.logger.info('Shutting down MediaFP...')
    media_fp.shutdown(board)
    configs.logger.info('  Done')

    configs.logger.info('Shutting down RitualDb...')
    db.save_and_close()
    configs.logger.info('  Done')


def get_media_fp(fetcher: Fetcher, db: RitualDb) -> MediaFP:
    if configs.media_fp == 'sutra':
        return SutraMediaFP(fetcher, configs.media_save_path, db)

    if configs.media_fp == 'asagi':
        return AsagiMediaFP(fetcher, configs.media_save_path, db)

    raise ValueError(configs.media_fp)


def main():
    Init()
    db = create_ritual_db()
    media_fp = get_media_fp(fetcher, db)
    loop = Loop()
    state = State(loop)
    fetcher = Fetcher(state)

    critical_error_count = 0
    board = ''
    while True:
        try:
            for board in configs.boards:
                process_board(board, db, fetcher, loop, state, media_fp)

            fetcher.sleep()
            state.save()

            loop.increment_loop()
            loop.log_board_durations()
            loop.sleep()

        except KeyboardInterrupt:
            configs.logger.info('Received interrupt signal')
            save_on_error(state, db, media_fp, board)
            break

        except Exception as e:
            configs.logger.error(f'Critical error in main loop: {e}')
            configs.logger.error(traceback.format_exc())
            save_on_error(state, db, media_fp, board)
            critical_error_count += 1
            n_critical_errors = 5
            if critical_error_count >= n_critical_errors:
                configs.logger.error(f'Critical error count reached {n_critical_errors}, exiting...')
                break

            sleep_for = critical_error_count * 60
            configs.logger.info(f'Sleeping for {sleep_for}s, maybe the issue will resolve itself by then...')
            sleep(sleep_for)

    configs.logger.info('Exited while loop, ending program.')


if __name__=='__main__':
    main()
