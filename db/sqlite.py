import functools
import sqlite3

from db.base import BaseDb, DotDict


def row_factory(cursor, row: tuple):
    keys = [col[0] for col in cursor.description]
    return DotDict(zip(keys, row))


class SqliteDb(BaseDb):
    placeholder = '?'


    def __init__(self, db_path: str, sql_echo: bool=False):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, autocommit=True)
        self.conn.row_factory = row_factory
        self.sql_echo = sql_echo


    @functools.lru_cache(maxsize=128)
    def get_upsert_clause(self, conflict_col: str, update_cols: list[str]) -> str:
        return f'on conflict({conflict_col}) do update set {", ".join(f'{k}=excluded.{k}' for k in update_cols)}'


    def save(self):
        self.conn.commit()


    def close(self):
        self.conn.close()


    def save_and_close(self):
        self.save()
        self.close()


    def _set_row_factory(self, dict_row: bool):
        if dict_row and not self.conn.row_factory:
            self.conn.row_factory = row_factory
            return

        if not dict_row and self.conn.row_factory:
            self.conn.row_factory = None
            return


    def _run_query(self, sql_string: str, params: tuple=None, commit: bool=False, dict_row: bool=True):
        if self.sql_echo:
            print(f'{sql_string=}\n{params=}')

        self._set_row_factory(dict_row)

        cursor = self.conn.execute(sql_string, params or ())
        results = cursor.fetchall()
        cursor.close()

        if commit:
            self.conn.commit()

        return results


    def run_query_tuple(self, sql_string: str, params: tuple=None, commit: bool=False):
        return self._run_query(sql_string, params, commit=commit, dict_row=False)


    def run_query_dict(self, sql_string: str, params: tuple=None, commit: bool=False):
        return self._run_query(sql_string, params, commit=commit, dict_row=True)


    def run_query_many(self, sql_string: str, params: tuple=None, commit: bool=False, dict_row=False):
        if self.sql_echo:
            print(f'{sql_string=}\n{params=}')

        self._set_row_factory(dict_row)

        results = self.conn.executemany(sql_string, params or ()).fetchall()

        if commit:
            self.conn.commit()

        return results
