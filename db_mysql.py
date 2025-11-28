import functools
import mysql.connector

from db_base import BaseDb, DotDict


class MysqlDb(BaseDb):
    placeholder = '%s'


    def __init__(self, host: str, user: str, password: str, database: str, port: int=3306, sql_echo: bool=False):
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            autocommit=False
        )
        self.sql_echo = sql_echo


    @functools.lru_cache(maxsize=128)
    def get_upsert_clause(self, conflict_col: str, update_cols: list[str]) -> str:
        return f'on duplicate key update {", ".join(f'{k}=values({k})' for k in update_cols)}'


    def save(self):
        self.conn.commit()


    def close(self):
        self.conn.close()


    def save_and_close(self):
        self.save()
        self.close()


    def _row_to_dict(self, cursor, row: tuple) -> DotDict:
        keys = [col[0] for col in cursor.description]
        return DotDict(zip(keys, row))


    def _run_query(self, sql_string: str, params: tuple=None, commit: bool=False, dict_row: bool=True):
        if self.sql_echo:
            print(f'{sql_string=}\n{params=}')

        cursor = self.conn.cursor()
        cursor.execute(sql_string, params or ())
        results = cursor.fetchall()

        if dict_row:
            results = [self._row_to_dict(cursor, row) for row in results]

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

        cursor = self.conn.cursor()
        cursor.executemany(sql_string, params or ())
        results = cursor.fetchall()

        if dict_row:
            results = [self._row_to_dict(cursor, row) for row in results]

        cursor.close()

        if commit:
            self.conn.commit()

        return results
