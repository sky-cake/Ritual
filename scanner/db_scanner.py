from db_sqlite import SqliteDb


class ScannerDb(SqliteDb):
    def __init__(self, db_path, sql_echo=False):
        super().__init__(db_path, sql_echo)

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
            dir_id integer,
            filename_no_ext text,
            ext_id integer,
            error integer,                  -- null -> unknown (metadata not gathered yet); 0 -> no; 1 -> yes (gives option to skip or investigate errors later on)
            md5_b64_given text,             -- api reported hash value
            md5_b64_computed text,          -- our computed hash against downloaded file
            sha256 text unique,
            bsize integer,
            unique (dir_id, filename_no_ext, ext_id)
        );;

        create index if not exists idx_hashtab_md5_b64_given on hashtab (md5_b64_given);;
        create index if not exists idx_hashtab_sha256 on hashtab (sha256);;
        create index if not exists idx_hashtab_error on hashtab (error);;
        '''

        for sql in sqls.split(';;'):
            self.run_query_tuple(sql)

        self.save()


    def fetch_ext_map(self) -> dict[str, int]:
        return {row[0]: row[1] for row in self.run_query_tuple('select ext, ext_id from extension;')}


    def get_and_set_ext_id(self, cache: dict[str, int], ext: str) -> int:
        '''
        - ext has no leading dot
        - ext is case sensitive
        '''

        if ext_id := cache.get(ext):
            return ext_id

        self.run_query_tuple('insert or ignore into extension (ext) values (?);', (ext,))
        cache[ext] = self.run_query_tuple('select ext_id from extension where ext=?;', (ext,))[0][0]
        return cache[ext]


    def get_and_set_dir_id(self, cache: dict[str, int], dirpath: str) -> int:
        '''
        - dirpath has no trailing slash
        - dirpath is the absolute path
        '''
        if dir_id := cache.get(dirpath):
            return dir_id

        self.run_query_tuple('insert or ignore into directory (dirpath) values (?);', (dirpath,))
        cache[dirpath] = self.run_query_tuple('select dir_id from directory where dirpath=?;', (dirpath,))[0][0]

        return cache[dirpath]
