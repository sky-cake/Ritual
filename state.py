import traceback

import configs
from loop import Loop
from utils import make_path, read_json, write_json_obj_to_file


class State:
    def __init__(self, loop: Loop):
        """
        Manages persistent state across scraper runs.

        - thread_cache: Maps board -> thread_id -> last_modified timestamp from catalog JSON.
        - http_cache: Maps URL -> HTTP Last-Modified header string for conditional requests.
        - thread_stats: Maps board -> thread_id -> stats dict (replies, images, most_recent_reply_no).
        - thread_meta: Maps board -> thread_id -> (page, bump_time) for deletion detection.
        """
        self.thread_cache_filepath = make_path('cache', 'thread_cache.json')
        self.thread_cache: dict[str, dict[int, float]] = dict()

        self.http_cache_filepath = make_path('cache', 'http_cache.json')
        self.http_cache: dict[str, str] = dict()

        self.thread_stats_filepath = make_path('cache', 'thread_stats.json')
        self.thread_stats: dict[str, dict[int, dict]] = dict()

        self.thread_meta_filepath = make_path('cache', 'thread_meta.json')
        self.thread_meta: dict[str, dict[int, list]] = dict()

        self.loop = loop

        self.read()

    @property
    def ignore_last_modified(self) -> bool:
        return self.loop.is_first_loop and configs.ignore_thread_cache

    def save(self):
        '''writes every cache'''
        try:
            write_json_obj_to_file(self.thread_cache_filepath, self.thread_cache)
            write_json_obj_to_file(self.http_cache_filepath, self.http_cache)
            write_json_obj_to_file(self.thread_stats_filepath, self.thread_stats)
            write_json_obj_to_file(self.thread_meta_filepath, self.thread_meta)
        except Exception as e:
            configs.logger.error(f'Failed to save state: {e}')
            configs.logger.error(traceback.format_exc())
            raise e

    def read(self):
        '''reads in every cache'''
        self.thread_cache = self.get_cached_thread_cache()
        self.http_cache = read_json(self.http_cache_filepath) or dict()
        self.thread_stats = self.get_cached_thread_stats()
        self.thread_meta = self.get_cached_thread_meta()

    def get_cached_thread_cache(self) -> dict[str, dict[int, float]]:
        """{g: {123: 1717755968, 124: 1717755999}, ck: {456: 1717755968}, ...}"""
        thread_cache = read_json(self.thread_cache_filepath)

        if not thread_cache:
            return dict()

        return {
            board: {int(tid): lm for tid, lm in tid_2_last_modified.items()}
            for board, tid_2_last_modified in thread_cache.items()
        }

    def get_cached_thread_stats(self) -> dict[str, dict[int, dict]]:
        """{g: {123: {replies: 10, images: 5, most_recent_reply_no: 456}, ...}, ...}"""
        thread_stats = read_json(self.thread_stats_filepath)

        if not thread_stats:
            return dict()

        return {
            board: {int(tid): stats for tid, stats in tid_2_stats.items()}
            for board, tid_2_stats in thread_stats.items()
        }

    def get_cached_thread_meta(self) -> dict[str, dict[int, list]]:
        """{g: {123: [page, bump_time], ...}, ...}"""
        thread_meta = read_json(self.thread_meta_filepath)

        if not thread_meta:
            return dict()

        return {
            board: {int(tid): meta for tid, meta in tid_2_meta.items()}
            for board, tid_2_meta in thread_meta.items()
        }

    def prune_old_threads(self, board: str):
        # Don't let the dict grow over N entries per board.
        if board not in self.thread_cache:
            return

        N = 200
        board_cache = self.thread_cache[board]
        if (count := len(board_cache)) > N:
            # In case of multiple stickies, or similar special threads, we delete the extras, plus M oldest threads
            M = count - N + 10
            tid_timestamp_pairs = [(tid, board_cache.get(tid) or 0.0) for tid in board_cache.keys()]
            tid_timestamp_pairs.sort(key=self.get_timestamp_from_pair)
            for stale_id, _ in tid_timestamp_pairs[:M]:
                del board_cache[stale_id] # prune

    def get_timestamp_from_pair(self, pair: tuple[int, float]) -> float:
        return pair[1]

    def is_thread_modified_cache_update(self, board: str, thread: dict) -> bool:
        """
        `True` indicates we should download the thread.
        """

        tid = thread['no']
        thread_last_modified = thread.get('last_modified')

        if board not in self.thread_cache:
            self.thread_cache[board] = dict()

        # should come before entry pruning
        thread_last_modified_cached = self.thread_cache[board].get(tid)

        # last_modified changed
        if thread_last_modified and thread_last_modified_cached and thread_last_modified != thread_last_modified_cached:
            # Update the thread's last modified time in thread_cache.
            self.thread_cache[board][tid] = thread_last_modified
            return True

        # new thread
        if thread_last_modified_cached is None:
            # Update the thread's last modified time in thread_cache.
            self.thread_cache[board][tid] = thread_last_modified
            return True

        # Update the thread's last modified time even if unchanged
        self.thread_cache[board][tid] = thread_last_modified

        return False

    def get_thread_stats(self, board: str, tid: int) -> dict | None:
        if board not in self.thread_stats:
            return None
        return self.thread_stats[board].get(tid)

    def set_thread_stats(self, board: str, tid: int, replies: int | None, images: int | None, most_recent_reply_no: int | None):
        if board not in self.thread_stats:
            self.thread_stats[board] = dict()
        if tid not in self.thread_stats[board]:
            self.thread_stats[board][tid] = dict()
        if replies is not None:
            self.thread_stats[board][tid]['replies'] = replies
        if images is not None:
            self.thread_stats[board][tid]['images'] = images
        if most_recent_reply_no is not None:
            self.thread_stats[board][tid]['most_recent_reply_no'] = most_recent_reply_no

        self.prune_old_thread_stats(board)

    def prune_old_thread_stats(self, board: str):
        """don't let the dict grow over N entries per board."""
        if board not in self.thread_stats:
            return

        N = 200
        board_stats = self.thread_stats[board]
        if (count := len(board_stats)) > N:
            # delete the oldest threads based on most_recent_reply_no
            M = count - N + 10
            tid_reply_pairs = [(tid, stats.get('most_recent_reply_no', 0) or 0) for tid, stats in board_stats.items()]
            tid_reply_pairs.sort(key=lambda pair: pair[1])
            for stale_id, _ in tid_reply_pairs[:M]:
                del board_stats[stale_id]

    def get_http_last_modified(self, url: str) -> str | None:
        return self.http_cache.get(url)

    def set_http_last_modified(self, url: str, last_modified: str | None):
        if last_modified:
            self.http_cache[url] = last_modified
            if len(self.http_cache) > 500:
                oldest_key = next(iter(self.http_cache))
                del self.http_cache[oldest_key]
        elif url in self.http_cache:
            del self.http_cache[url]

    def get_thread_url_last_modified(self, board: str, tid: int) -> str | None:
        url = configs.url_thread.format(board=board, thread_id=tid)
        return self.get_http_last_modified(url)

    def update_thread_meta(self, board: str, tid_2_page: dict[int, int], tid_2_thread: dict[int, dict]):
        """update page positions and bump times from catalog data."""
        if board not in self.thread_meta:
            self.thread_meta[board] = dict()

        for tid, page in tid_2_page.items():
            thread = tid_2_thread.get(tid, dict())
            # last reply time, op time, 0
            # if 0, no harm done - thread deletion logic still relies on page number, n replies, and not in archive
            bump_time = thread.get('last_modified', thread.get('time', 0))
            self.thread_meta[board][tid] = [page, bump_time]

        self.prune_old_thread_meta(board)

    def prune_old_thread_meta(self, board: str):
        """don't let the dict grow over N entries per board."""
        if board not in self.thread_meta:
            return

        N = 200
        board_meta = self.thread_meta[board]
        if (count := len(board_meta)) > N:
            # delete the oldest threads based on bump_time (index 1 in the list)
            M = count - N + 10
            tid_bump_pairs = [(tid, meta[1] if meta and len(meta) > 1 else 0) for tid, meta in board_meta.items()]
            tid_bump_pairs.sort(key=lambda pair: pair[1])
            for stale_id, _ in tid_bump_pairs[:M]:
                del board_meta[stale_id]

    def get_thread_meta(self, board: str, tid: int) -> list | None:
        """returns [page, bump_time] or None if not tracked."""
        if board not in self.thread_meta:
            return
        return self.thread_meta[board].get(tid)

    def remove_thread_meta(self, board: str, tid: int):
        """remove thread from tracking after deletion/archive/prune."""
        if board in self.thread_meta and tid in self.thread_meta[board]:
            del self.thread_meta[board][tid]
