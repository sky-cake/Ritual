import builtins
import json
import os

import pytest

from db.sqlite import SqliteDb
from utils import make_path


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(self, *args, **kwargs):
        raise RuntimeError('Network access is disabled in tests')

    monkeypatch.setattr('requests.sessions.Session.request', blocked)


@pytest.fixture(autouse=True)
def no_fs_writes(monkeypatch):
    def deny(*args, **kwargs):
        raise AssertionError('Filesystem writes are disabled in tests')

    real_open = builtins.open

    def guarded_open(file, mode='r', *args, **kwargs):
        if any(m in mode for m in ('w', 'a', 'x', '+')):
            raise AssertionError(f'File writes are disabled in tests: {file!r} mode={mode!r}')
        return real_open(file, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, 'open', guarded_open)
    monkeypatch.setattr(os, 'makedirs', deny)
    monkeypatch.setattr(os, 'mkdir', deny)
    monkeypatch.setattr(os, 'rename', deny)
    monkeypatch.setattr(os, 'replace', deny)
    monkeypatch.setattr(os, 'remove', deny)
    monkeypatch.setattr(os, 'unlink', deny)
    monkeypatch.setattr(os, 'rmdir', deny)
    monkeypatch.setattr(os, 'symlink', deny)
    monkeypatch.setattr(os, 'link', deny)
    monkeypatch.setattr(os, 'truncate', deny)


def create_test_sqlite_db(board: str) -> SqliteDb:
    """Create a SQLite database with test tables for the given board."""
    sqlite_db = SqliteDb(':memory:')
    
    # Create main board table
    sqlite_db.conn.execute(f'''
        CREATE TABLE IF NOT EXISTS `{board}` (
            num INTEGER,
            thread_num INTEGER,
            subnum INTEGER,
            op INTEGER,
            timestamp INTEGER,
            timestamp_expired INTEGER,
            preview_w INTEGER,
            preview_h INTEGER,
            media_w INTEGER,
            media_h INTEGER,
            media_size INTEGER,
            spoiler INTEGER,
            deleted INTEGER,
            capcode TEXT,
            sticky INTEGER,
            locked INTEGER,
            poster_ip TEXT,
            media_id INTEGER,
            media_hash TEXT,
            media_orig TEXT,
            media_filename TEXT,
            preview_orig TEXT,
            email TEXT,
            name TEXT,
            trip TEXT,
            title TEXT,
            comment TEXT,
            delpass TEXT,
            poster_hash TEXT,
            poster_country TEXT,
            exif TEXT,
            PRIMARY KEY (num, subnum)
        )
    ''')
    
    # Create images table
    sqlite_db.conn.execute(f'''
        CREATE TABLE IF NOT EXISTS `{board}_images` (
            media_hash TEXT PRIMARY KEY,
            media TEXT,
            total INTEGER,
            banned INTEGER
        )
    ''')
    
    # Create threads table
    sqlite_db.conn.execute(f'''
        CREATE TABLE IF NOT EXISTS `{board}_threads` (
            thread_num INTEGER PRIMARY KEY,
            time_op INTEGER,
            time_last INTEGER,
            time_bump INTEGER,
            time_ghost INTEGER,
            time_ghost_bump INTEGER,
            time_last_modified INTEGER,
            nreplies INTEGER,
            nimages INTEGER,
            sticky INTEGER,
            locked INTEGER
        )
    ''')
    
    sqlite_db.conn.commit()
    return sqlite_db


@pytest.fixture
def catalog_json():
    with open(make_path('tests', 'test_files', 'catalog.json'), 'r') as f:
        return json.load(f)


@pytest.fixture
def thread_json():
    with open(make_path('tests', 'test_files', 'thread.json'), 'r') as f:
        return json.load(f)
