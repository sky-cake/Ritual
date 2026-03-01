from requests import JSONDecodeError, Session

import configs
from state import State
from utils import sleep


class Fetcher:
    def __init__(self, state: State | None = None):
        self.session: Session = Session()
        self.state = state

    def fetch_json(self, url, headers=None, request_cooldown_sec: float=None, add_random: bool=False) -> dict | None:
        request_headers = dict(headers) if headers else dict()

        if not configs.ignore_http_cache and self.state:
            last_modified = self.state.get_http_last_modified(url)
            if last_modified:
                request_headers['If-Modified-Since'] = last_modified

        resp = self.session.get(url, headers=request_headers, timeout=10)

        if request_cooldown_sec:
            sleep(request_cooldown_sec, add_random=add_random)

        if resp.status_code == 304:
            if not configs.ignore_http_cache and self.state:
                last_modified_header = resp.headers.get('Last-Modified')
                if last_modified_header:
                    self.state.set_http_last_modified(url, last_modified_header)
            configs.logger.warning(f'Not modified (304) {url}')
            return dict()

        if resp.status_code == 200:
            if not configs.ignore_http_cache and self.state:
                last_modified_header = resp.headers.get('Last-Modified')
                if last_modified_header:
                    self.state.set_http_last_modified(url, last_modified_header)
            try:
                return resp.json()
            except JSONDecodeError:
                configs.logger.warning(f'Failed to parse JSON (200) {url}')
                return dict()

        configs.logger.warning(f'Failed to get JSON ({resp.status_code}) {url}')
        return dict()

    def sleep(self):
        # Refresh session periodically to prevent stale connections
        # Also refresh if loop cooldown is long enough to warrant it
        if configs.loop_cooldown_sec >= 15.0:
            self.session.close()
            self.session = Session()
