import configs
from fetcher import Fetcher


class Archive:
    def __init__(self, fetcher: Fetcher, board: str):
        self.fetcher = fetcher
        self.board = board
        self.has_archive: bool = board in configs.boards_with_archive
        self.archived_tids: set[int] | None = None

    def board_supports_archive(self) -> bool:
        return self.has_archive

    def is_archived(self, tid: int) -> bool:
        """
        Lazy-fetches archive.json on first call and keeps results for other threads.
        """
        # avoid boards with no archive support
        if not self.board_supports_archive():
            return False

        if self.archived_tids is None:
            self.fetch_and_set_archive()

        if self.archived_tids is None:
            return False

        return tid in self.archived_tids

    def fetch_and_set_archive(self):
        url = configs.url_archive.format(board=self.board)
        configs.logger.info(f'[{self.board}] Fetching archive.json')
        data = self.fetcher.fetch_json(
            url,
            headers=configs.headers,
            request_cooldown_sec=configs.request_cooldown_sec,
            add_random=configs.add_random,
        )

        if not data:
            return

        self.archived_tids = set(data)
        configs.logger.info(f'[{self.board}] Loaded {len(self.archived_tids)} archived tids')

