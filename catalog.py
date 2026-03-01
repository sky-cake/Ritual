import msgspec

import configs
from fetcher import Fetcher
from utils import ChanThread


class Catalog:
    def __init__(self, fetcher: Fetcher, board: str):
        self.fetcher = fetcher

        self.board: str = board
        self.catalog: list[dict] = []
        self.tid_2_thread: dict[int, dict] = dict()
        self.tid_2_page: dict[int, int] = dict()
        self.tid_2_last_replies: dict[int, list[dict]] = dict()


    def validate_threads(self):
        for thread in self.tid_2_thread.values():
            msgspec.convert(thread, ChanThread)


    def fetch_catalog(self) -> bool:
        '''
        - fetches catalog from api
        - validates catalog from api
        - returns `True` if successful
        '''
        self.catalog = self.fetcher.fetch_json(
            configs.url_catalog.format(board=self.board),
            headers=configs.headers,
            request_cooldown_sec=configs.request_cooldown_sec,
            add_random=configs.add_random,
        )

        configs.logger.info(f'[{self.board}] Downloaded catalog')

        if not self.catalog:
            configs.logger.warning(f'[{self.board}] Catalog empty {self.catalog}')
            return False

        self.set_tid_2_thread()
        self.validate_threads()

        self.set_tid_2_last_replies()

        return True


    def set_tid_2_thread(self):
        page_i = 1
        for page in self.catalog:
            page_num = page.get('page', page_i)
            page_i += 1
            for thread in page['threads']:
                tid = thread['no']
                self.tid_2_thread[tid] = thread
                self.tid_2_page[tid] = page_num


    def set_tid_2_last_replies(self):
        for page in self.catalog:
            for thread in page['threads']:
                if last_replies := thread.get('last_replies'):
                    self.tid_2_last_replies[thread['no']] = last_replies

