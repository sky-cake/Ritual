import os
from itertools import batched
from functools import lru_cache
import sqlite3
import time
import argparse


def get_root_path_from_args() -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument('--root')
    args = parser.parse_args()
    return args.root


def get_placeholders(l: list) -> str:
    if len(l) < 1:
        raise ValueError(l)
    return ','.join(['?'] * len(l))


class SqliteDb:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: sqlite3.Connection | None = None

    def connect(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)

    def close(self):
        self.conn.commit()
        self.conn.close()


class ScannerDb(SqliteDb):
    def __init__(self, db_path: str):
        if not db_path:
            db_path = make_path('scanner.db')
            print(f'Using default ScannerDb.db_path: {db_path}')
        super().__init__(db_path)

    def init_db(self):
        sqls = '''
        pragma journal_mode=wal;;
        pragma synchronous=normal;;
        pragma temp_store=memory;;

        create table if not exists directory (
            dir_id integer primary key,
            dirpath text unique             -- No trailing slash. Absolute path.
        );;

        create table if not exists extension (
            ext_id integer primary key,
            ext text unique                 -- No leading dot. Case sensitive.
        );;

        create table if not exists hashtab (
            dir_id           integer,
            filename_no_ext  text,
            ext_id           integer,
            md5              text,          -- api reported b64(md5 hash) value
            md5_computed     text,          -- our computed b64(md5 hash) against the downloaded file
            fsize            text,          -- api reported file size in bytes
            fsize_computed   integer,       -- our computed file size in bytes

            datetime_utc integer,

            unique (dir_id, filename_no_ext, ext_id)
        );;

        create index if not exists idx_hashtab_md5             on hashtab (md5);;
        create index if not exists idx_hashtab_md5_computed    on hashtab (md5_computed);;
        create index if not exists idx_hashtab_filename_no_ext on hashtab (filename_no_ext);;
        '''

        for sql in sqls.split(';;'):
            if sql.strip():
                self.conn.execute(sql)

        self.conn.commit()


    def insert_from_names(self, dirname: str, filename: str, deterministic_directory_mode: bool):
        self.connect()
        sql_insert_hashtab = f'insert or ignore into hashtab (dir_id, filename_no_ext, ext_id, datetime_utc) values (?,?,?,{int(time.time())});'

        dir_id = None
        if not deterministic_directory_mode:
            dir_id = self.get_dir_id(dirname)

        filename_no_ext, ext = filename.rsplit('.', maxsplit=1)
        ext_id = self.get_ext_id(ext)

        self.conn.execute(sql_insert_hashtab, (dir_id, filename_no_ext, ext_id))
        self.conn.commit()


    # don't expect many cache hits, keep low
    @lru_cache(maxsize=128)
    def get_dir_id(self, path: str) -> int:
        '''
        - dirpath has no trailing slash
        - dirpath is the absolute path
        '''
        result = self.conn.execute('insert or ignore into directory (dirpath) values (?)', (path,)).fetchall()
        if result:
            return result[0][0]
        return self.conn.execute('select dir_id from directory where dirpath=?', (path,)).fetchall()[0][0]


    # case sensitive
    # png jpeg jpg gif, N=4 extensions
    # png Png pNg pnG PNg pNG PnG PNG, M=3 extension length for png, 2^M combinations
    # summing all combinations, 8 + 16 + 8 + 8 = 40
    # let's assume a few extra valid image extensions
    # round up to 1024 because why not
    @lru_cache(maxsize=1024)
    def get_ext_id(self, ext: str) -> int:
        '''
        - ext has no leading dot
        - ext is case sensitive
        '''
        result = self.conn.execute('insert or ignore into extension (ext) values (?)', (ext,)).fetchall()
        if result:
            return result[0][0]
        return self.conn.execute('select ext_id from extension where ext=?', (ext,)).fetchall()[0][0]


class Counter:
    def __init__(self, name: str, stdout_every: int):
        self.name = name
        self.count = 0
        self.sub_counter = 0
        self.stdout_every = stdout_every

    def __call__(self, increment_by: int=1):
        self.count += increment_by
        self.sub_counter += increment_by
        if self.sub_counter >= self.stdout_every:
            self.sub_counter = 0
            print(f'\r{self.name}: {self.count:,}', end='', flush=True)


def make_path(*args) -> str:
    d = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(d, *args)


def iter_media_files(root_path: str, skip_dirnames: set[str] | None=None, valid_exts: set[str] | None=None):
    for dirpath, dirnames, filenames in os.walk(root_path):
        dirname = os.path.basename(dirpath)
        if skip_dirnames and dirname in skip_dirnames:
            dirnames[:] = []
            continue
        for filename in filenames:
            if '.' in filename:
                filename_no_ext, ext = filename.rsplit('.', maxsplit=1)
                if valid_exts and ext.lower() in valid_exts:
                    yield dirpath, filename_no_ext, ext


def iter_media_files_fast(root_path: str, valid_exts: set[str] | None=None):
    '''
    - deterministic_directory_mode
    - removed skip_dirnames
    '''
    for dirpath, dirnames, filenames in os.walk(root_path):
        for filename in filenames:
            if '.' in filename:
                filename_no_ext, ext = filename.rsplit('.', maxsplit=1)
                if valid_exts and ext.lower() in valid_exts:
                    yield filename_no_ext, ext


class ScannerConfig:
    db_path: str = '' # default is ./scanner.db
    root_path: str = get_root_path_from_args() or '/mnt/dl'
    file_exts: str = 'jpeg,jpg,png,gif' # comma separated, no dot in .ext

    skip_dirnames: set[str] = set()

    # directories can be created from columns in the `image` table
    # allows us to skip `dir_id` lookups
    # Note: running this against the same directory in different modes will result in "duplicate" hashtab records (dir_id = int, None)
    deterministic_directory_mode: bool = True # True, False

    ## End of configs - Do not touch ##
    ## End of configs - Do not touch ##
    ## End of configs - Do not touch ##
    file_exts: set[str] = set([e for e in file_exts.split(',')])


def gather_filesystem(db: ScannerDb, conf: ScannerConfig, batch_size: int=5_000):
    """
    Crawls a root path recursively, creating entries of existing files in the sql table `hashtab`.
    """
    counter = Counter('catalog_filesystem', batch_size)

    datetime_utc = int(time.time())
    sql_insert_hashtab = f'insert or ignore into hashtab (dir_id, filename_no_ext, ext_id, datetime_utc) values (?,?,?,{datetime_utc});'

    iter_media_func = iter_media_files
    if not conf.skip_dirnames and conf.deterministic_directory_mode:
        iter_media_func = iter_media_files_fast
        dir_id = None

    for batch in batched(iter_media_func(conf.root_path, valid_exts=conf.file_exts), batch_size):
        params = []
        for item in batch:
            if not conf.deterministic_directory_mode:
                dirpath, filename_no_ext, ext = item
                dir_id = db.get_dir_id(dirpath)
            else:
                filename_no_ext, ext = item

            ext_id = db.get_ext_id(ext)
            params.append((dir_id, filename_no_ext, ext_id))

        db.conn.executemany(sql_insert_hashtab, params)
        db.conn.commit()
        counter(increment_by=len(batch))

    print('\ncatalog_filesystem, completed')


if __name__ == '__main__':
    conf = ScannerConfig()

    assert conf.root_path
    assert os.path.isdir(conf.root_path), conf.root_path
    print(f'scanning: "{conf.root_path}" for {conf.file_exts}')

    db = None
    try:
        db = ScannerDb(conf.db_path)
        db.connect()
        db.init_db()
        gather_filesystem(db, conf)
    finally:
        if db:
            db.conn.commit()
            db.conn.close()
