import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from requests.exceptions import ChunkedEncodingError

from catalog import Catalog
from db.ritual import RitualDb
from enums import MediaType
from fetcher import Fetcher
from filter import Filter
from loop import Loop
from media_fp import AsagiMediaFP
from posts import Posts
from state import State
from tests.conftest import create_test_sqlite_db
from utils import db_row_to_media_post, fetch_media_bytes


@pytest.fixture
def mock_configs(monkeypatch, tmp_path):
    cfg = SimpleNamespace(
        url_catalog='https://a.4cdn.org/{board}/catalog.json',
        url_thread='https://a.4cdn.org/{board}/thread/{thread_id}.json',
        headers={},
        request_cooldown_sec=0,
        add_random=False,
        logger=SimpleNamespace(info=lambda s: None, warning=lambda s: None, error=lambda s: None),
        boards={'po': {'thread_text': True}},
        boards_with_archive=[],
        use_http_cache_first_loop=False,
        ensure_media_downloaded=True,
        make_thumbnails=False,
        media_save_path=str(tmp_path),
        db_type='sqlite',
        db_path=':memory:',
        unescape_data_b4_db_write=True,
        loop_cooldown_sec=0,
    )
    monkeypatch.setattr('main.configs', cfg)
    monkeypatch.setattr('db.ritual.configs', cfg)
    monkeypatch.setattr('filter.configs', cfg)
    monkeypatch.setattr('media_fp.configs', cfg)
    monkeypatch.setattr('posts.configs', cfg)
    monkeypatch.setattr('archive.configs', cfg)
    monkeypatch.setattr('fetcher.configs', cfg)
    monkeypatch.setattr('catalog.configs', cfg)
    monkeypatch.setattr('loop.configs', cfg)
    monkeypatch.setattr('state.configs', cfg)

    return cfg


@pytest.fixture
def mock_fetcher(catalog_json, thread_json, state):
    fetcher = Fetcher(state)
    
    def fetch_json(url, **kwargs):
        if 'catalog.json' in url:
            return catalog_json
        elif 'thread' in url:
            return thread_json
        return {}
    
    fetcher.fetch_json = Mock(side_effect=fetch_json)
    return fetcher


@pytest.fixture
def loop(mock_configs):
    return Loop()


@pytest.fixture
def state(loop, tmp_path, monkeypatch):
    monkeypatch.setattr('state.make_path', lambda *parts: str(tmp_path.joinpath(*parts)))
    return State(loop)


@pytest.fixture
def db(mock_configs, monkeypatch):
    # Mock the async table creation to avoid asagi_tables dependency in tests
    monkeypatch.setattr('db.ritual.execute_action', AsyncMock())
    monkeypatch.setattr('db.ritual.asagi_close_pool', AsyncMock())
    
    sqlite_db = create_test_sqlite_db('po')
    return RitualDb(sqlite_db)


class TestCatalog:
    def test_fetch_catalog_success(self, mock_fetcher, mock_configs):
        catalog = Catalog(mock_fetcher, 'po')
        result = catalog.fetch_catalog()
        
        assert result is True
        assert len(catalog.catalog) > 0
        assert len(catalog.tid_2_thread) > 0

    def test_fetch_catalog_empty(self, mock_configs, state):
        fetcher = Fetcher(state)
        fetcher.fetch_json = Mock(return_value={})
        
        catalog = Catalog(fetcher, 'po')
        result = catalog.fetch_catalog()
        
        assert result is False
        assert len(catalog.catalog) == 0

    def test_set_tid_2_thread(self, mock_fetcher, catalog_json, mock_configs):
        catalog = Catalog(mock_fetcher, 'po')
        catalog.catalog = catalog_json
        catalog.set_tid_2_thread()
        
        assert len(catalog.tid_2_thread) > 0
        for page in catalog_json:
            for thread in page['threads']:
                assert thread['no'] in catalog.tid_2_thread

    def test_set_tid_2_last_replies(self, mock_fetcher, catalog_json, mock_configs):
        catalog = Catalog(mock_fetcher, 'po')
        catalog.catalog = catalog_json
        catalog.set_tid_2_last_replies()
        
        has_replies = False
        for page in catalog_json:
            for thread in page['threads']:
                if thread.get('last_replies'):
                    has_replies = True
                    assert thread['no'] in catalog.tid_2_last_replies
        
        if has_replies:
            assert len(catalog.tid_2_last_replies) > 0


class TestShouldUseHttpCache:
    def test_first_loop_bypasses_cache(self, state, mock_configs):
        mock_configs.use_http_cache_first_loop = False
        fetcher = Fetcher(state)

        assert fetcher.should_use_http_cache() is False

    def test_first_loop_honors_cache_when_enabled(self, state, mock_configs):
        mock_configs.use_http_cache_first_loop = True
        fetcher = Fetcher(state)

        assert fetcher.should_use_http_cache() is True

    def test_later_loop_always_honors_cache(self, state, mock_configs):
        mock_configs.use_http_cache_first_loop = False
        state.loop.increment_loop()
        fetcher = Fetcher(state)

        assert fetcher.should_use_http_cache() is True

    def test_no_state_bypasses_cache(self, mock_configs):
        fetcher = Fetcher(None)

        assert fetcher.should_use_http_cache() is False


class TestState:
    def test_is_thread_modified_new_thread(self, state):
        thread = {'no': 1, 'last_modified': 100}
        result = state.is_thread_modified_cache_update('po', thread)
        
        assert result is True
        assert state.thread_cache['po'][1] == 100

    def test_is_thread_modified_changed(self, state):
        state.thread_cache['po'] = {1: 100}
        thread = {'no': 1, 'last_modified': 200}
        result = state.is_thread_modified_cache_update('po', thread)
        
        assert result is True
        assert state.thread_cache['po'][1] == 200

    def test_is_thread_modified_unchanged(self, state):
        state.thread_cache['po'] = {1: 100}
        thread = {'no': 1, 'last_modified': 100}
        result = state.is_thread_modified_cache_update('po', thread)
        
        assert result is False
        assert state.thread_cache['po'][1] == 100

    def test_is_thread_modified_none_value(self, state):
        thread = {'no': 1, 'last_modified': None}
        result = state.is_thread_modified_cache_update('po', thread)
        
        assert result is True
        assert state.thread_cache['po'][1] is None

    def test_prune_old_threads(self, state):
        board = 'po'
        state.thread_cache[board] = {i: i for i in range(210)}
        state.prune_old_threads(board)
        
        assert len(state.thread_cache[board]) <= 200

    def test_get_cached_thread_cache(self, state, tmp_path, monkeypatch):
        cache_file = tmp_path / 'cache.json'
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        test_data = {'po': {'1': 100, '2': 200}}
        with open(cache_file, 'w') as f:
            json.dump(test_data, f)
        
        state.thread_cache_filepath = str(cache_file)
        result = state.get_cached_thread_cache()
        
        assert result['po'][1] == 100
        assert result['po'][2] == 200

    def test_http_cache_get_set(self, state):
        url = 'https://a.4cdn.org/po/thread/1.json'
        last_modified = 'Wed, 21 Oct 2015 07:28:00 GMT'
        
        state.set_http_last_modified(url, last_modified)
        assert state.get_http_last_modified(url) == last_modified

    def test_http_cache_prune(self, state):
        for i in range(510):
            url = f'https://a.4cdn.org/po/thread/{i}.json'
            state.set_http_last_modified(url, 'Wed, 21 Oct 2015 07:28:00 GMT')
        
        assert len(state.http_cache) <= 500

    def test_thread_stats(self, state):
        state.set_thread_stats('po', 1, replies=10, images=5, most_recent_reply_no=100)
        stats = state.get_thread_stats('po', 1)
        
        assert stats['replies'] == 10
        assert stats['images'] == 5
        assert stats['most_recent_reply_no'] == 100


class TestFilter:
    def test_should_archive_whitelist(self, mock_fetcher, db, state, mock_configs):
        mock_configs.boards['po'] = {'whitelist': 'origami|paper'}
        filter_obj = Filter(mock_fetcher, db, 'po', state)
        
        assert filter_obj.should_archive('origami discussion', '')
        assert filter_obj.should_archive('', 'paper craft')
        assert not filter_obj.should_archive('random', 'topic')

    def test_should_archive_blacklist(self, mock_fetcher, db, state, mock_configs):
        mock_configs.boards['po'] = {'blacklist': 'spam'}
        filter_obj = Filter(mock_fetcher, db, 'po', state)
        
        assert not filter_obj.should_archive('spam thread', '')
        assert not filter_obj.should_archive('', 'this is spam')
        assert filter_obj.should_archive('legitimate', 'topic')

    def test_should_archive_blacklist_overrides_whitelist(self, mock_fetcher, db, state, mock_configs):
        mock_configs.boards['po'] = {
            'blacklist': 'spam',
            'whitelist': 'origami'
        }
        filter_obj = Filter(mock_fetcher, db, 'po', state)
        
        assert not filter_obj.should_archive('spam origami', '')

    def test_should_archive_min_chars(self, mock_fetcher, db, state, mock_configs):
        mock_configs.boards['po'] = {'op_comment_min_chars': 10}
        filter_obj = Filter(mock_fetcher, db, 'po', state)
        
        assert not filter_obj.should_archive('', 'short')
        assert filter_obj.should_archive('', 'this is long enough')

    def test_should_archive_min_unique_chars(self, mock_fetcher, db, state, mock_configs):
        mock_configs.boards['po'] = {'op_comment_min_chars_unique': 5}
        filter_obj = Filter(mock_fetcher, db, 'po', state)
        
        assert not filter_obj.should_archive('', 'aaaa')
        assert filter_obj.should_archive('', 'abcde')

    def test_is_media_needed_conf_pattern_match(self, mock_fetcher, db, state, mock_configs):
        filter_obj = Filter(mock_fetcher, db, 'po', state)

        post = {'no': 1, 'tim': 123456, 'ext': '.jpg', 'sub': 'test', 'com': 'wireguard'}
        pattern = '.*wireguard.*'

        assert filter_obj.is_media_needed_conf(post, pattern) is True

    def test_is_media_needed_conf_bool(self, mock_fetcher, db, state, mock_configs):
        filter_obj = Filter(mock_fetcher, db, 'po', state)

        assert filter_obj.is_media_needed_conf({}, True) is True
        assert filter_obj.is_media_needed_conf({}, False) is False
        assert filter_obj.is_media_needed_conf({}, None) is False


class TestDownloadSkipsExisting:
    def test_download_full_media_skips_existing_file(self, mock_fetcher, db, mock_configs, tmp_path, monkeypatch):
        media_fp = AsagiMediaFP(mock_fetcher, str(tmp_path), db, None)
        post = {'tim': 123456, 'ext': '.jpg', 'md5': 'h1'}

        dirpath, filename = media_fp.get_dirpath_and_filename('po', MediaType.full_media, post)
        os.makedirs(dirpath, exist_ok=True)
        with open(os.path.join(dirpath, filename), 'wb') as f:
            f.write(b'data')

        fetch = Mock()
        monkeypatch.setattr('media_fp.wrap_fetch_media_bytes', fetch)

        media_fp.download_full_media('http://example.com/123456.jpg', post, 'po')

        fetch.assert_not_called()

    def test_download_thumbnail_skips_existing_file(self, mock_fetcher, db, mock_configs, tmp_path, monkeypatch):
        media_fp = AsagiMediaFP(mock_fetcher, str(tmp_path), db, None)
        post = {'tim': 123456, 'ext': '.jpg', 'md5': 'h1'}

        dirpath, filename = media_fp.get_dirpath_and_filename('po', MediaType.thumbnail, post)
        os.makedirs(dirpath, exist_ok=True)
        with open(os.path.join(dirpath, filename), 'wb') as f:
            f.write(b'data')

        fetch = Mock()
        monkeypatch.setattr('media_fp.wrap_fetch_media_bytes', fetch)

        media_fp.download_thumbnail('http://example.com/123456s.jpg', post, 'po')

        fetch.assert_not_called()


class TestPosts:
    def test_fetch_posts_success(self, mock_fetcher, db, thread_json, mock_configs, state, catalog_json):
        from main import Archive
        tid_2_thread = {628117: {'no': 628117, 'last_modified': 100, 'replies': 3, 'images': 0}}
        catalog = Catalog(mock_fetcher, 'po')
        catalog.catalog = catalog_json
        catalog.set_tid_2_thread()
        catalog.set_tid_2_last_replies()
        posts = Posts(db, mock_fetcher, 'po', tid_2_thread, state, catalog)
        archive = Archive(mock_fetcher, 'po')
        posts.fetch_posts(archive)
        
        assert 628117 in posts.tid_2_posts
        assert len(posts.pid_2_post) > 0

    def test_fetch_posts_missing_thread(self, mock_fetcher, db, mock_configs, state, catalog_json):
        from main import Archive
        def fetch_json(url, **kwargs):
            if 'thread' in url:
                return {}
            return {}
        
        mock_fetcher.fetch_json = Mock(side_effect=fetch_json)
        
        tid_2_thread = {999999: {'no': 999999, 'last_modified': 100, 'replies': 0, 'images': 0}}
        catalog = Catalog(mock_fetcher, 'po')
        catalog.catalog = []
        catalog.set_tid_2_thread()
        catalog.set_tid_2_last_replies()
        posts = Posts(db, mock_fetcher, 'po', tid_2_thread, state, catalog)
        archive = Archive(mock_fetcher, 'po')
        posts.fetch_posts(archive)
        
        assert 999999 not in posts.tid_2_posts

    def test_fetch_posts_304_not_modified(self, mock_fetcher, db, mock_configs, state, catalog_json):
        from main import Archive
        def fetch_json(url, **kwargs):
            if 'thread' in url:
                return None
            return {}
        
        mock_fetcher.fetch_json = Mock(side_effect=fetch_json)
        
        tid_2_thread = {628117: {'no': 628117, 'last_modified': 100, 'replies': 0, 'images': 0}}
        catalog = Catalog(mock_fetcher, 'po')
        catalog.catalog = []
        catalog.set_tid_2_thread()
        catalog.set_tid_2_last_replies()
        posts = Posts(db, mock_fetcher, 'po', tid_2_thread, state, catalog)
        archive = Archive(mock_fetcher, 'po')
        posts.fetch_posts(archive)
        
        assert 628117 not in posts.tid_2_posts

    def test_set_pid_2_post(self, db, mock_fetcher, thread_json, mock_configs, state, catalog_json):
        tid_2_thread = {628117: {'no': 628117}}
        catalog = Catalog(mock_fetcher, 'po')
        catalog.catalog = []
        catalog.set_tid_2_thread()
        catalog.set_tid_2_last_replies()
        posts = Posts(db, mock_fetcher, 'po', tid_2_thread, state, catalog)
        posts.tid_2_posts = {628117: thread_json['posts']}
        posts.set_pid_2_post()
        
        for post in thread_json['posts']:
            assert post['no'] in posts.pid_2_post

    def test_save_posts(self, db, mock_fetcher, thread_json, mock_configs, state, catalog_json):
        tid_2_thread = {628117: {'no': 628117}}
        catalog = Catalog(mock_fetcher, 'po')
        catalog.catalog = []
        catalog.set_tid_2_thread()
        catalog.set_tid_2_last_replies()
        posts = Posts(db, mock_fetcher, 'po', tid_2_thread, state, catalog)
        posts.tid_2_posts = {628117: thread_json['posts']}
        posts.set_pid_2_post()
        
        posts.save_posts()
        
        rows = db.db.run_query_tuple(f'select num from `po` where num = ?', params=(628117,))
        assert len(rows) > 0


class TestLoop:
    def test_is_first_loop(self, loop):
        assert loop.is_first_loop is True
        loop.increment_loop()
        assert loop.is_first_loop is False

    def test_set_start_time(self, loop):
        loop.set_start_time()
        assert loop.start_time is not None

    def test_get_duration_minutes(self, loop):
        loop.set_start_time()
        import time
        time.sleep(0.1)
        duration = loop.get_duration_minutes()
        assert duration >= 0

    def test_set_board_duration_minutes(self, loop):
        loop.set_start_time()
        loop.set_board_duration_minutes('po')
        assert 'po' in loop.board_2_duration

    def test_increment_loop(self, loop):
        initial = loop.loop_i
        loop.increment_loop()
        assert loop.loop_i == initial + 1


class TestIntegration:
    def test_full_flow_no_api_calls(self, mock_fetcher, db, state, loop, catalog_json, thread_json, mock_configs):
        mock_configs.boards['po'] = {'thread_text': True}
        
        catalog = Catalog(mock_fetcher, 'po')
        catalog.catalog = catalog_json
        catalog.set_tid_2_thread()
        catalog.set_tid_2_last_replies()
        
        filter_obj = Filter(mock_fetcher, db, 'po', state)
        filter_obj.filter_catalog(catalog)
        
        posts = Posts(db, mock_fetcher, 'po', filter_obj.tid_2_thread, state, catalog)
        posts.tid_2_posts = {628117: thread_json['posts']}
        posts.set_pid_2_post()
        posts.save_posts()
        
        assert len(posts.pid_2_post) > 0
        assert len(filter_obj.tid_2_thread) > 0


def insert_media_post(db, board, num, thread_num, op, media_hash, media_orig, media_size=100, deleted=0):
    db.db.conn.execute(
        f'insert into `{board}` (num, thread_num, subnum, op, title, comment, media_hash, media_orig, media_size, deleted) '
        f'values (?, ?, 0, ?, ?, ?, ?, ?, ?, ?)',
        (num, thread_num, op, 'title', 'comment', media_hash, media_orig, media_size, deleted),
    )
    db.db.conn.commit()


class TestDbRowToMediaPost:
    def test_reconstructs_post(self):
        row = {
            'num': 5,
            'thread_num': 1,
            'op': 0,
            'title': 'subject',
            'comment': 'body',
            'media_hash': 'hashhashhashhashhashhash',
            'media_orig': '1234567890.jpg',
            'media_size': 42,
        }
        post = db_row_to_media_post(row)

        assert post['no'] == 5
        assert post['resto'] == 1
        assert post['sub'] == 'subject'
        assert post['com'] == 'body'
        assert post['md5'] == 'hashhashhashhashhashhash'
        assert post['tim'] == '1234567890'
        assert post['ext'] == '.jpg'
        assert post['fsize'] == 42

    def test_non_media_returns_none(self):
        assert db_row_to_media_post({'media_orig': 'deleted'}) is None
        assert db_row_to_media_post({'media_orig': None}) is None


class TestGetPostsForTids:
    def test_groups_by_thread_and_excludes_deleted(self, db):
        insert_media_post(db, 'po', 1, 1, 1, 'h1', '111.jpg')
        insert_media_post(db, 'po', 2, 1, 0, 'h2', '222.png')
        insert_media_post(db, 'po', 3, 2, 1, 'h3', '333.gif')
        insert_media_post(db, 'po', 4, 2, 0, 'h4', '444.jpg', deleted=1)

        result = db.get_media_posts_for_tids('po', [1, 2])

        assert {row['num'] for row in result[1]} == {1, 2}
        assert {row['num'] for row in result[2]} == {3}

    def test_empty_tids(self, db):
        assert db.get_media_posts_for_tids('po', []) == {}


class TestGetPidsForDownload:
    def test_all_full_media(self, mock_fetcher, db, state, mock_configs):
        mock_configs.boards['po'] = {'dl_fm_op': True, 'dl_fm_post': True}
        filter_obj = Filter(mock_fetcher, db, 'po', state)

        tid_2_posts = {
            1: [
                {'no': 1, 'tim': '111', 'ext': '.jpg', 'md5': 'h1', 'sub': '', 'com': ''},
                {'no': 2, 'tim': '222', 'ext': '.png', 'md5': 'h2', 'sub': '', 'com': ''},
            ]
        }
        tid_2_thread = {1: {'no': 1}}

        full_pids, thumb_pids = filter_obj.get_pids_for_download(tid_2_posts, tid_2_thread)

        assert full_pids == {1, 2}
        assert thumb_pids == set()

    def test_missing_thread_entry_is_skipped(self, mock_fetcher, db, state, mock_configs):
        mock_configs.boards['po'] = {'dl_fm_post': True}
        filter_obj = Filter(mock_fetcher, db, 'po', state)

        tid_2_posts = {1: [{'no': 2, 'tim': '222', 'ext': '.png', 'md5': 'h2', 'sub': '', 'com': ''}]}

        full_pids, _ = filter_obj.get_pids_for_download(tid_2_posts, {})

        assert full_pids == set()


class TestEnsureThumbnail:
    def test_missing_thumb_created(self, mock_fetcher, db, mock_configs, tmp_path, monkeypatch):
        mock_configs.make_thumbnails = True
        media_fp = AsagiMediaFP(mock_fetcher, str(tmp_path), db, None)
        post = {'tim': 123456, 'ext': '.jpg', 'md5': 'h1'}

        dirpath, filename = media_fp.get_dirpath_and_filename('po', MediaType.full_media, post)
        os.makedirs(dirpath, exist_ok=True)
        with open(os.path.join(dirpath, filename), 'wb'):
            pass

        calls = []
        monkeypatch.setattr('media_fp.create_thumbnail', lambda *a, **k: calls.append((a, k)))

        media_fp.ensure_thumbnail(post, 'po')

        assert len(calls) == 1
        _, full_path, thumb_path = calls[0][0]
        assert os.path.isfile(full_path)
        assert thumb_path.endswith('123456s.jpg')

    def test_unsupported_media_skipped(self, mock_fetcher, db, mock_configs, tmp_path, monkeypatch):
        mock_configs.make_thumbnails = True
        media_fp = AsagiMediaFP(mock_fetcher, str(tmp_path), db, None)
        post = {'tim': 123456, 'ext': '.pdf', 'md5': 'h1'}

        dirpath, filename = media_fp.get_dirpath_and_filename('po', MediaType.full_media, post)
        os.makedirs(dirpath, exist_ok=True)
        with open(os.path.join(dirpath, filename), 'wb'):
            pass

        calls = []
        monkeypatch.setattr('media_fp.create_thumbnail', lambda *a, **k: calls.append((a, k)))

        media_fp.ensure_thumbnail(post, 'po')

        assert calls == []


class TestEnsureMediaDownloaded:
    def test_downloads_missing_media_from_db(self, mock_fetcher, db, state, mock_configs):
        mock_configs.boards['po'] = {'dl_fm_op': True, 'dl_fm_post': True}
        insert_media_post(db, 'po', 1, 1, 1, 'h1', '111.jpg')
        insert_media_post(db, 'po', 2, 1, 0, 'h2', '222.png')

        catalog = Catalog(mock_fetcher, 'po')
        catalog.tid_2_thread = {1: {'no': 1, 'sub': 'x', 'com': 'y'}}

        filter_obj = Filter(mock_fetcher, db, 'po', state)
        media_fp = Mock()

        filter_obj.ensure_media_downloaded(media_fp, catalog)

        media_fp.download_media_for_ids.assert_called_once()
        board, pid_2_post, full_pids, thumb_pids = media_fp.download_media_for_ids.call_args.args
        assert board == 'po'
        assert full_pids == {1, 2}
        assert thumb_pids == set()
        assert set(pid_2_post) == {1, 2}

    def test_deleted_posts_are_skipped(self, mock_fetcher, db, state, mock_configs):
        mock_configs.boards['po'] = {'dl_fm_op': True}
        insert_media_post(db, 'po', 1, 1, 1, 'h1', '111.jpg', deleted=1)

        catalog = Catalog(mock_fetcher, 'po')
        catalog.tid_2_thread = {1: {'no': 1, 'sub': 'x', 'com': 'y'}}

        filter_obj = Filter(mock_fetcher, db, 'po', state)
        media_fp = Mock()

        filter_obj.ensure_media_downloaded(media_fp, catalog)

        media_fp.download_media_for_ids.assert_not_called()

    def test_filtered_threads_are_skipped(self, mock_fetcher, db, state, mock_configs):
        mock_configs.boards['po'] = {'blacklist': 'skipme', 'dl_fm_op': True}
        insert_media_post(db, 'po', 1, 1, 1, 'h1', '111.jpg')

        catalog = Catalog(mock_fetcher, 'po')
        catalog.tid_2_thread = {1: {'no': 1, 'sub': 'skipme', 'com': ''}}

        filter_obj = Filter(mock_fetcher, db, 'po', state)
        media_fp = Mock()

        filter_obj.ensure_media_downloaded(media_fp, catalog)

        media_fp.download_media_for_ids.assert_not_called()


class FakeResponse:
    def __init__(self, chunks=None, raise_exc=None):
        self.status_code = 200
        self.headers = {}
        self._chunks = chunks or []
        self._raise_exc = raise_exc

    def iter_content(self, chunk_size):
        if self._raise_exc:
            raise self._raise_exc
        yield from self._chunks

    def close(self):
        pass


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, url, headers=None, stream=True):
        response = self.responses[self.calls]
        self.calls += 1
        return response


class TestFetchMediaBytesRetry:
    def test_retries_after_chunked_encoding_error(self, monkeypatch):
        waits = []
        monkeypatch.setattr('utils.sleep', lambda t, add_random=False: waits.append(t))

        session = FakeSession([
            FakeResponse(raise_exc=ChunkedEncodingError('incomplete read')),
            FakeResponse(chunks=[b'data']),
        ])

        result = fetch_media_bytes('http://example.com/x.jpg', '.jpg', session=session)

        assert result == b'data'
        assert session.calls == 2
        assert waits == [5.0, 2.2]  # backoff, then image cooldown

    def test_gives_up_after_max_retries(self, monkeypatch):
        waits = []
        monkeypatch.setattr('utils.sleep', lambda t, add_random=False: waits.append(t))

        session = FakeSession([FakeResponse(raise_exc=ChunkedEncodingError('incomplete read')) for _ in range(3)])

        result = fetch_media_bytes('http://example.com/x.jpg', '.jpg', session=session)

        assert result is None
        assert session.calls == 3
        assert waits == [5, 10]

    def test_success_sleeps_cooldown_only(self, monkeypatch):
        waits = []
        monkeypatch.setattr('utils.sleep', lambda t, add_random=False: waits.append(t))

        session = FakeSession([FakeResponse(chunks=[b'data'])])

        result = fetch_media_bytes('http://example.com/x.jpg', '.jpg', session=session)

        assert result == b'data'
        assert waits == [2.2]
