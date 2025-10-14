import configs
from asagi import get_d_board, post_is_sticky
from db_sqlite import SqliteDb, get_placeholders
from utils import make_path


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


    def upsert_posts(self, board: str, posts: list[dict]):
        posts_to_insert = []

        for post in posts:
            if post_is_sticky(post):
                continue

            d_board = get_d_board(post, unescape_data_b4_db_write=configs.unescape_data_b4_db_write)
            posts_to_insert.append(d_board)

        self.upsert_many(board, posts_to_insert, 'num, subnum')
