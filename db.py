import sqlite3
from functools import lru_cache

import configs


@lru_cache()
def get_placeholders_by_len(length, symbol='?'):
    return ','.join([symbol] * length)


def get_placeholders(l: list, symbol='?'):
    return get_placeholders_by_len(len(l), symbol=symbol)


def dict_factory(cursor, row):
    d = {}
    for i, col in enumerate(cursor.description):
        d[col[0]] = row[i]
    return d


def get_connection():
    # print('Creating database connection, started.')
    assert isinstance(configs.database, str)
    connection = sqlite3.connect(configs.database)
    connection.row_factory = dict_factory
    # print('Creating database connection, completed.')
    return connection
