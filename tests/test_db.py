from types import SimpleNamespace
from unittest.mock import patch

import pytest

from db_ritual import RitualDb


@pytest.fixture
def mock_configs(monkeypatch):
    cfg = SimpleNamespace(
        boards={'test': {}},
        logger=SimpleNamespace(info=lambda s: None),
        unescape_data_b4_db_write=True,
    )
    monkeypatch.setattr('db_ritual.configs', cfg)
    return cfg


@pytest.fixture
def db(mock_configs):
    return RitualDb(':memory:')


class TestRitualDb:
    def test_get_pids_by_tid(self, db, mock_configs):
        db.run_query_tuple(
            f'insert into `test` (num, thread_num, subnum, op, timestamp, timestamp_expired, preview_w, preview_h, media_w, media_h, media_size, spoiler, deleted, capcode, sticky, locked, poster_ip, media_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            params=(1, 100, 0, 1, 1000, 0, 0, 0, 0, 0, 0, 0, 0, 'N', 0, 0, '0', 0)
        )
        db.run_query_tuple(
            f'insert into `test` (num, thread_num, subnum, op, timestamp, timestamp_expired, preview_w, preview_h, media_w, media_h, media_size, spoiler, deleted, capcode, sticky, locked, poster_ip, media_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            params=(2, 100, 0, 0, 1001, 0, 0, 0, 0, 0, 0, 0, 0, 'N', 0, 0, '0', 0)
        )
        
        pids = db.get_pids_by_tid('test', 100)
        
        assert 1 in pids
        assert 2 in pids

    def test_set_posts_deleted(self, db, mock_configs):
        db.run_query_tuple(
            f'insert into `test` (num, thread_num, subnum, op, timestamp, timestamp_expired, preview_w, preview_h, media_w, media_h, media_size, spoiler, deleted, capcode, sticky, locked, poster_ip, media_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            params=(1, 100, 0, 1, 1000, 0, 0, 0, 0, 0, 0, 0, 0, 'N', 0, 0, '0', 0)
        )
        
        db.set_posts_deleted('test', [1])
        
        rows = db.run_query_tuple(f'select deleted from `test` where num = ?', params=(1,))
        assert rows[0][0] == 1

    def test_set_threads_deleted(self, db, mock_configs):
        db.run_query_tuple(
            f'insert into `test` (num, thread_num, subnum, op, timestamp, timestamp_expired, preview_w, preview_h, media_w, media_h, media_size, spoiler, deleted, capcode, sticky, locked, poster_ip, media_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            params=(100, 100, 0, 1, 1000, 0, 0, 0, 0, 0, 0, 0, 0, 'N', 0, 0, '0', 0)
        )
        
        db.set_threads_deleted('test', [100])
        
        rows = db.run_query_tuple(f'select deleted from `test` where num = ?', params=(100,))
        assert rows[0][0] == 1

    def test_get_existing_media_hashes(self, db, mock_configs):
        db.run_query_tuple(
            f'insert into `test` (num, thread_num, subnum, op, timestamp, timestamp_expired, preview_w, preview_h, media_w, media_h, media_size, spoiler, deleted, capcode, sticky, locked, poster_ip, media_hash, media_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            params=(1, 100, 0, 1, 1000, 0, 0, 0, 0, 0, 0, 0, 0, 'N', 0, 0, '0', 'hash1', 0)
        )
        db.run_query_tuple(
            f'insert into `test` (num, thread_num, subnum, op, timestamp, timestamp_expired, preview_w, preview_h, media_w, media_h, media_size, spoiler, deleted, capcode, sticky, locked, poster_ip, media_hash, media_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            params=(2, 100, 0, 0, 1001, 0, 0, 0, 0, 0, 0, 0, 0, 'N', 0, 0, '0', 'hash2', 0)
        )
        
        hashes = db.get_existing_media_hashes('test', ['hash1', 'hash2', 'hash3'])
        
        assert 'hash1' in hashes
        assert 'hash2' in hashes
        assert 'hash3' not in hashes

    def test_get_existing_media_hashes_empty(self, db, mock_configs):
        hashes = db.get_existing_media_hashes('test', [])
        assert hashes == set()

    def test_upsert_posts(self, db, mock_configs):
        post = {
            'no': 1,
            'resto': 0,
            'time': 1000,
            'name': 'Test',
            'sub': 'Test thread',
            'com': 'Test comment',
            'tim': 123456,
            'ext': '.jpg',
            'filename': 'test',
            'fsize': 1000,
            'w': 100,
            'h': 100,
            'tn_w': 50,
            'tn_h': 50,
            'md5': 'testhash',
            'spoiler': 0,
            'filedeleted': 0,
            'sticky': 0,
            'closed': 0,
        }
        
        with patch('db_ritual.get_d_board') as mock_get_d_board:
            mock_get_d_board.return_value = {
                'media_id': 0,
                'poster_ip': '0',
                'num': 1,
                'subnum': 0,
                'thread_num': 1,
                'op': 1,
                'timestamp': 1000,
                'timestamp_expired': 0,
                'preview_orig': '123456s.jpg',
                'preview_w': 50,
                'preview_h': 50,
                'media_filename': 'test.jpg',
                'media_w': 100,
                'media_h': 100,
                'media_size': 1000,
                'media_hash': 'testhash',
                'media_orig': '123456.jpg',
                'spoiler': 0,
                'deleted': 0,
                'capcode': 'N',
                'email': None,
                'name': 'Test',
                'trip': None,
                'title': 'Test thread',
                'comment': 'Test comment',
                'delpass': None,
                'sticky': 0,
                'locked': 0,
                'poster_hash': None,
                'poster_country': None,
                'exif': None,
            }
            
            db.upsert_posts('test', [post])
            
            rows = db.run_query_tuple(f'select num from `test` where num = ?', params=(1,))
            assert len(rows) > 0

