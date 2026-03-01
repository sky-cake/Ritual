import json
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from catalog import Catalog
from db.ritual import RitualDb
from enums import MediaType
from fetcher import Fetcher
from filter import Filter
from loop import Loop
from posts import Posts
from state import State
from tests.conftest import create_test_sqlite_db


@pytest.fixture
def mock_configs(monkeypatch):
    cfg = SimpleNamespace(
        url_catalog='https://a.4cdn.org/{board}/catalog.json',
        url_thread='https://a.4cdn.org/{board}/thread/{thread_id}.json',
        headers={},
        request_cooldown_sec=0,
        add_random=False,
        logger=SimpleNamespace(info=lambda s: None, warning=lambda s: None, error=lambda s: None),
        boards={'po': {'thread_text': True}},
        boards_with_archive=[],
        ignore_last_modified=False,
        ignore_thread_cache=False,
        ignore_http_cache=False,
        make_thumbnails=False,
        media_save_path=tempfile.mkdtemp(),
        db_path=':memory:',
        unescape_data_b4_db_write=True,
        loop_cooldown_sec=0,
    )
    monkeypatch.setattr('main.configs', cfg)
    monkeypatch.setattr('db.ritual.configs', cfg)
    monkeypatch.setattr('filter.configs', cfg)

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
def loop():
    return Loop()


@pytest.fixture
def state(loop):
    s = State(loop)
    s.last_modified = {}
    return s


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

    def test_is_media_needed_file_exists(self, mock_fetcher, db, state, mock_configs, tmp_path):
        mock_configs.media_save_path = str(tmp_path)
        filter_obj = Filter(mock_fetcher, db, 'po', state)
        
        post = {'no': 1, 'tim': 123456, 'ext': '.jpg', 'sub': 'test', 'com': 'test'}
        
        media_dir = tmp_path / 'po' / 'image' / '1234' / '56'
        media_dir.mkdir(parents=True, exist_ok=True)
        (media_dir / '123456.jpg').touch()
        
        result = filter_obj.is_media_needed(post, True, MediaType.full_media)
        assert result is False

    def test_is_media_needed_duplicate_hash(self, mock_fetcher, db, state, mock_configs, tmp_path):
        mock_configs.media_save_path = str(tmp_path)
        filter_obj = Filter(mock_fetcher, db, 'po', state)
        
        post = {'no': 1, 'tim': 123456, 'ext': '.jpg', 'md5': 'testhash123', 'sub': 'test', 'com': 'test'}
        stored_filename = '123456.jpg'
        
        media_dir = tmp_path / 'po' / 'image' / '1234' / '56'
        media_dir.mkdir(parents=True, exist_ok=True)
        (media_dir / stored_filename).touch()
        
        result = filter_obj.is_media_needed(post, True, MediaType.full_media)
        
        assert result is False

    def test_is_media_needed_pattern_match(self, mock_fetcher, db, state, mock_configs, tmp_path):
        mock_configs.media_save_path = str(tmp_path)
        filter_obj = Filter(mock_fetcher, db, 'po', state)
        
        post = {'no': 1, 'tim': 123456, 'ext': '.jpg', 'sub': 'test', 'com': 'wireguard'}
        pattern = '.*wireguard.*'
        
        result = filter_obj.is_media_needed(post, pattern, MediaType.full_media)
        
        assert result is True


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
