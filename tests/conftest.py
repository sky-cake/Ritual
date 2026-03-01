import json
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.ritual import RitualDb
from db.sqlite import SqliteDb
from utils import make_path


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
