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
            dir_id           integer,
            filename_no_ext  text,
            ext_id           integer,
            md5              text,          -- api reported b64(md5 hash) value
            md5_computed     text,          -- our computed b64(md5 hash) against the downloaded file
            fsize            text,          -- api reported file size in bytes
            fsize_computed   integer,       -- our computed file size in bytes
            is_banned        integer,
            is_saved         integer,

            unique (dir_id, filename_no_ext, ext_id)
        );;

        create index if not exists idx_hashtab_md5           on hashtab (md5);;
        create index if not exists idx_hashtab_md5_computed  on hashtab (md5_computed);;
        '''

        for sql in sqls.split(';;'):
            if sql.strip():
                self.run_query_tuple(sql)

        self.save()


    def fetch_dir_map(self) -> dict[str, int]:
        return {row[0]: row[1] for row in self.run_query_tuple('select dirpath, dir_id from directory;')}


    def fetch_ext_map(self) -> dict[str, int]:
        return {row[0]: row[1] for row in self.run_query_tuple('select ext, ext_id from extension;')}


    def get_and_set_dir_id(self, cache: dict[str, int], dirpath: str) -> int:
        '''
        - dirpath has no trailing slash
        - dirpath is the absolute path
        '''
        if dir_id := cache.get(dirpath):
            return dir_id

        row = self.run_query_tuple('insert into directory (dirpath) values (?) on conflict(dirpath) do nothing returning dir_id;', (dirpath,))
        if row:
            dir_id = row[0][0]
        else:
            dir_id = self.run_query_tuple('select dir_id from directory where dirpath=?;', (dirpath,))[0][0]

        cache[dirpath] = dir_id
        return dir_id


    def get_and_set_ext_id(self, cache: dict[str, int], ext: str) -> int:
        '''
        - ext has no leading dot
        - ext is case sensitive
        '''
        if ext_id := cache.get(ext):
            return ext_id

        row = self.run_query_tuple('insert into extension (ext) values (?) on conflict(ext) do nothing returning ext_id;', (ext,))
        if row:
            ext_id = row[0][0]
        else:
            ext_id = self.run_query_tuple('select ext_id from extension where ext=?;', (ext,))[0][0]

        cache[ext] = ext_id
        return ext_id
