import pytest
from types import SimpleNamespace
from main import Catalog, Filter, State, Loop, Fetcher, Posts
import json
from utils import make_path

@pytest.fixture
def mock_configs(monkeypatch):
    cfg = SimpleNamespace(
        url_catalog='https://a.4cdn.org/{board}/catalog.json',
        headers={},
        request_cooldown_sec=0,
        add_random=False,
        logger=SimpleNamespace(info=lambda s: None),
        boards={'g': {}},
        ignore_last_modified=False,
        make_thumbnails=False,
    )
    monkeypatch.setattr('main.configs', cfg)
    return cfg

@pytest.fixture
def loop():
    return Loop()

@pytest.fixture
def state(loop):
    s = State(loop)
    s.last_modified = {}
    return s

@pytest.fixture
def catalog_json():
    with open(make_path('testing', 'catalog.json')) as f:
        return json.load(f)

@pytest.fixture
def thread_json():
    with open(make_path('testing', 'thread.json')) as f:
        return json.load(f)

def make_thread(no: int, lm: int) -> dict:
    return {'no': no, 'last_modified': lm}

def test_should_archive_whitelist_blacklist(monkeypatch, mock_configs):
    mock_configs.boards['g'] = {
        'blacklist': 'mac',
        'whitelist': 'linux|windows',
    }
    f = Filter(None, None, 'g', None)
    assert not f.should_archive('Mac OS', '')
    assert f.should_archive('Linux', '')
    assert f.should_archive('', 'Windows 10 discussion')
    assert not f.should_archive('', 'random')

def test_returns_true_if_no_cache_yet(state):
    t = make_thread(1, 100)
    assert state.is_thread_modified_cache_update('g', t)
    assert state.last_modified['g'][1] == 100

def test_returns_true_if_new_thread(state):
    state.last_modified = {'g': {10: 123}}
    t = make_thread(11, 456)
    assert state.is_thread_modified_cache_update('g', t)
    assert 11 in state.last_modified['g']

def test_returns_true_if_last_modified_changed(state):
    state.last_modified = {'g': {42: 100}}
    t = make_thread(42, 200)
    assert state.is_thread_modified_cache_update('g', t)
    assert state.last_modified['g'][42] == 200

def test_returns_false_if_last_modified_same(state):
    state.last_modified = {'g': {42: 100}}
    t = make_thread(42, 100)
    assert not state.is_thread_modified_cache_update('g', t)
    assert state.last_modified['g'][42] == 100

def test_creates_board_entry_if_missing(state: State):
    t = make_thread(5, 500)
    assert state.is_thread_modified_cache_update('biz', t)
    assert 'biz' in state.last_modified
    assert state.last_modified['biz'][5] == 500

def test_prunes_when_exceeding_limit(monkeypatch, state):
    board = 'g'
    state.last_modified[board] = {i: i for i in range(210)}
    sorted_called = {'count': 0}
    orig_sorted = sorted
    def fake_sorted(*a, **kw):
        sorted_called['count'] += 1
        return orig_sorted(*a, **kw)
    import builtins
    monkeypatch.setattr(builtins, 'sorted', fake_sorted)
    t = make_thread(999, 999)
    state.is_thread_modified_cache_update(board, t)
    assert sorted_called['count'] == 1
    assert len(state.last_modified[board]) <= 200
    assert 999 in state.last_modified[board]

def test_load_catalog_threads(state, catalog_json):
    board = 'po'
    catalog = Catalog(Fetcher(), board)
    catalog.catalog = catalog_json
    catalog.set_tid_2_thread()
    catalog.validate_threads()

    for page in catalog_json:
        for thread in page['threads']:
            assert state.is_thread_modified_cache_update(board, thread)
            assert state.last_modified[board][thread['no']] == thread['last_modified']
            assert not state.is_thread_modified_cache_update(board, thread)

def test_validate_posts(thread_json):
    posts = Posts(None, None, 'g', None)
    posts.validate_posts(thread_json['posts'])

def test_modified_thread_updates_last_modified(state):
    board = 'g'
    thread = {'no': 1234, 'last_modified': 1000}
    state.is_thread_modified_cache_update(board, thread)
    assert state.last_modified[board][1234] == 1000
    thread_updated = {'no': 1234, 'last_modified': 2000}
    assert state.is_thread_modified_cache_update(board, thread_updated)
    assert state.last_modified[board][1234] == 2000

def test_prune_old_threads(state):
    board = 'g'
    state.last_modified[board] = {i: i for i in range(210)}
    thread_new = {'no': 9999, 'last_modified': 9999}
    state.is_thread_modified_cache_update(board, thread_new)
    assert len(state.last_modified[board]) == 190
    assert 0 not in state.last_modified[board]
    assert 9999 in state.last_modified[board]

def test_thread_with_no_last_modified_returns_true(state):
    thread = {'no': 42}
    assert state.is_thread_modified_cache_update('g', thread)
    assert state.last_modified['g'][42] is None

def test_multiple_boards_independent(state):
    thread_g = {'no': 1, 'last_modified': 100}
    thread_biz = {'no': 2, 'last_modified': 200}
    assert state.is_thread_modified_cache_update('g', thread_g)
    assert state.is_thread_modified_cache_update('biz', thread_biz)
    assert state.last_modified['g'][1] == 100
    assert state.last_modified['biz'][2] == 200

def test_pruning_removes_oldest_entries(state):
    board = 'g'
    state.last_modified[board] = {i: i for i in range(201)}
    thread_new = {'no': 500, 'last_modified': 500}
    state.is_thread_modified_cache_update(board, thread_new)
    assert len(state.last_modified[board]) <= 200
    assert 500 in state.last_modified[board]

def test_thread_last_modified_none_and_cached_none(state):
    thread = {'no': 10, 'last_modified': None}
    assert state.is_thread_modified_cache_update('g', thread)

def test_thread_last_modified_changes_from_none(state):
    thread = {'no': 1, 'last_modified': None}
    state.is_thread_modified_cache_update('g', thread)
    thread_updated = {'no': 1, 'last_modified': 123}
    assert state.is_thread_modified_cache_update('g', thread_updated)
    assert state.last_modified['g'][1] == 123

def test_thread_last_modified_same_as_cached(state):
    thread = {'no': 5, 'last_modified': 555}
    state.is_thread_modified_cache_update('g', thread)
    assert not state.is_thread_modified_cache_update('g', thread)

def test_large_board_cache_pruning(state):
    board = 'g'
    state.last_modified[board] = {i: i*10 for i in range(205)}
    thread_new = {'no': 9999, 'last_modified': 9999}
    state.is_thread_modified_cache_update(board, thread_new)
    assert len(state.last_modified[board]) <= 190
    assert 9999 in state.last_modified[board]

if __name__ == '__main__':
    pytest.main(['-v', __file__])
