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
            sha256           text,          -- our computed sha256 hash against the downloaded file
            is_banned        integer,
            is_saved         integer,
            has_error        integer,

            unique (dir_id, filename_no_ext, ext_id)
        );;

        create index if not exists idx_hashtab_md5           on hashtab (md5);;
        create index if not exists idx_hashtab_md5_computed  on hashtab (md5_computed);;
        create index if not exists idx_hashtab_sha256        on hashtab (sha256);;

        --
        -- view to allow inserting using names instead of ids
        --

        create view if not exists hashtab_view as
            select
                d.dirpath,
                h.filename_no_ext,
                e.ext,
                h.md5,
                h.md5_computed,
                h.fsize,
                h.fsize_computed,
                h.sha256,
                h.is_banned,
                h.is_saved,
                h.has_error
            from hashtab h
                join directory d using(dir_id)
                join extension e using(ext_id)
        ;;

        --
        -- trigger to auto-insert directory and extension
        --

        create trigger if not exists trigger_insert_hashtab_view
        instead of insert on hashtab_view
        begin
            insert into directory (dirpath) values (new.dirpath) on conflict (dirpath) do nothing;

            insert into extension (ext) values (new.ext) on conflict (ext) do nothing;

            insert into hashtab (
                dir_id,
                filename_no_ext,
                ext_id,
                md5,
                md5_computed,
                fsize,
                fsize_computed,
                sha256,
                is_banned,
                is_saved,
                has_error
            )
            values (
                (select dir_id from directory where dirpath=new.dirpath),
                new.filename_no_ext,
                (select ext_id from extension where ext=new.ext),
                new.md5,
                new.md5_computed,
                new.fsize,
                new.fsize_computed,
                new.sha256,
                new.is_banned,
                new.is_saved,
                new.has_error
            )
            on conflict (dir_id, filename_no_ext, ext_id) do update set
                md5           = excluded.md5,
                md5_computed  = excluded.md5_computed,
                fsize         = excluded.fsize,
                fsize_computed= excluded.fsize_computed,
                sha256        = excluded.sha256,
                is_banned     = excluded.is_banned,
                is_saved      = excluded.is_saved,
                has_error     = excluded.has_error;
        end;;
        '''

        for sql in sqls.split(';;'):
            if sql.strip():
                self.run_query_tuple(sql)

        self.save()


    # def fetch_ext_map(self) -> dict[str, int]:
    #     return {row[0]: row[1] for row in self.run_query_tuple('select ext, ext_id from extension;')}


    # def get_and_set_ext_id(self, cache: dict[str, int], ext: str) -> int:
    #     '''
    #     - ext has no leading dot
    #     - ext is case sensitive
    #     '''

    #     if ext_id := cache.get(ext):
    #         return ext_id

    #     self.run_query_tuple('insert or ignore into extension (ext) values (?);', (ext,))
    #     cache[ext] = self.run_query_tuple('select ext_id from extension where ext=?;', (ext,))[0][0]
    #     return cache[ext]


    # def get_and_set_dir_id(self, cache: dict[str, int], dirpath: str) -> int:
    #     '''
    #     - dirpath has no trailing slash
    #     - dirpath is the absolute path
    #     '''
    #     if dir_id := cache.get(dirpath):
    #         return dir_id

    #     self.run_query_tuple('insert or ignore into directory (dirpath) values (?);', (dirpath,))
    #     cache[dirpath] = self.run_query_tuple('select dir_id from directory where dirpath=?;', (dirpath,))[0][0]

    #     return cache[dirpath]
