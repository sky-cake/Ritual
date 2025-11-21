from db_sqlite import SqliteDb


class ScannerDb(SqliteDb):

    def __init__(self, db_path, sql_echo=False):
        super().__init__(db_path, sql_echo)

    def init_db(self):
        sqls = '''
        create table if not exists directory (
            dir_id integer primary key,
            dirpath text unique
        );

        create table if not exists extension (
            ext_id integer primary key,
            ext text unique
        );

        create table if not exists file (
            file_id integer primary key,
            dir_id integer,
            filename_no_ext text,
            ext_id integer,
            attempted integer, -- null-no, 1-yes
            sha256 text,
            md5_b64 text,
            bsize integer,
            width integer,
            height integer,
            unique (dir_id, filename_no_ext, ext_id)
        );

        create index if not exists idx_file_attempted on file (attempted);
        '''

        for sql in sqls.split(';'):
            self.run_query_tuple(sql)

        self.save()


    def fetch_ext_map(self) -> dict[str, int]:
        return {row[1]: row[0] for row in self.run_query_tuple('select ext_id, ext from extension')}


    def get_and_set_ext_id(self, cache: dict[str,int], ext: str) -> int:
        '''
        - ext has no leading dot
        - does not commit data on inserts
        '''

        if ext in cache:
            return cache[ext]

        self.run_query_tuple('insert or ignore into extension (ext) values (?)', (ext,))
        ext_id = self.run_query_tuple('select ext_id from extension where ext=?', (ext,))[0][0]

        cache[ext] = ext_id
        return ext_id


    def get_and_set_dir_id(self, cache: dict[str, int], dirpath: str) -> int:
        '''
        - does not commit data on inserts
        '''
        if dirpath in cache:
            return cache[dirpath]

        self.run_query_tuple('insert or ignore into directory (dirpath) values (?)', (dirpath,))
        dir_id = self.run_query_tuple('select dir_id from directory where dirpath=?', (dirpath,))[0][0]

        cache[dirpath] = dir_id
        return dir_id
