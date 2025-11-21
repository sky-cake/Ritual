import configs
from db_sqlite import SqliteDb, get_placeholders
from utils import get_d_board, make_path


class RitualDb(SqliteDb):

    def __init__(self, db_path, sql_echo=False):
        super().__init__(db_path, sql_echo)

        for board in configs.boards:
            try:
                self.run_query_tuple(f'SELECT * FROM `{board}` LIMIT 1;')
                configs.logger.info(f'[{board}] Tables already exist.')
            except Exception:
                configs.logger.info(f'[{board}] Creating tables.')

                with open(make_path('schema.sql')) as f:
                    sql = f.read()

                sqls = sql.replace('%%BOARD%%', board).split(';')

                for sql in sqls:
                    self.run_query_tuple(sql)

        self.save()


    def get_pids_by_tid(self, board: str, tid: int) -> list[int]:
        rows = self.run_query_tuple(f'select num from `{board}` where thread_num = ?', params=(tid,))
        return [row[0] for row in rows] if rows else []


    def set_posts_deleted(self, board: str, pids: list[int]) -> None:
        if not pids:
            return

        sql = f"update `{board}` set deleted = 1 where num in ({get_placeholders(pids)});"
        self.run_query_tuple(sql, params=pids, commit=True)


    def set_threads_deleted(self, board: str, tids: list[int]) -> None:
        if not tids:
            return

        sql = f"update `{board}` set deleted = 1 where num in ({get_placeholders(tids)});"
        self.run_query_tuple(sql, params=tids, commit=True)


    def upsert_many(self, board: str, rows: list[dict], conflict_col: str, batch_size: int=500):
        if not rows:
            return

        keys = list(rows[0])
        placeholder = '(' + get_placeholders(keys) + ')'
        sql_cols = ', '.join(keys)
        sql_conflict = ', '.join([f'{k}=excluded.{k}' for k in keys])

        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            placeholders = ', '.join([placeholder] * len(chunk))
            sql = f"""insert into `{board}` ({sql_cols}) values {placeholders} on conflict({conflict_col}) do update set {sql_conflict};"""
            flat_values = [v for row in chunk for v in row.values()]
            self.run_query_tuple(sql, params=flat_values, commit=True)


    def get_existing_media_hashes(self, board: str, media_hashes: list[str]) -> set[str]:
        if not media_hashes:
            return set()

        sql = f"""
            select distinct media_hash
            from `{board}`
            where
                media_hash in ({get_placeholders(media_hashes)})
                and media_hash is not null;
        """
        rows = self.run_query_tuple(sql, params=media_hashes)
        return {row[0] for row in rows} if rows else set()


    def get_media_hash_info(self, board: str, media_hashes: list[str]) -> tuple[dict[str, str], set[str]]:
        if not media_hashes:
            return dict(), set()

        sql = f"""
            select media_hash, media, banned
            from `{board}_images`
            where media_hash in ({get_placeholders(media_hashes)});
        """
        rows = self.run_query_tuple(sql, params=media_hashes)
        if not rows:
            return dict(), set()

        md5_2_media_filename = dict()
        banned_hashes = set()
        for row in rows:
            hash_val = row[0]
            media_filename = row[1]
            banned_val = row[2]
            if media_filename:
                md5_2_media_filename[hash_val] = media_filename
            if banned_val != 0:
                banned_hashes.add(hash_val)

        return md5_2_media_filename, banned_hashes


    def upsert_image(self, board: str, media_hash: str, media: str | None):
        """
        Ritual doesn't make a distinction between OP and reply thumbnails.
        """
        if not media_hash:
            return

        sql = f"""
            insert into `{board}_images` (media_hash, media, total, banned)
            values (?, ?, ?, 1, 0)
            on conflict(media_hash) do update set
                total = total + 1,
                media = coalesce(media, excluded.media)
        ;"""
        self.run_query_tuple(sql, params=(media_hash, media), commit=True)


    def upsert_posts(self, board: str, posts: list[dict]):
        posts_to_insert = []

        for post in posts:
            d_board = get_d_board(post, unescape_data_b4_db_write=configs.unescape_data_b4_db_write)
            posts_to_insert.append(d_board)

        self.upsert_many(board, posts_to_insert, 'num, subnum')
