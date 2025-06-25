import sqlite3

import configs


def dict_factory(cursor, row):
    d = {}
    for i, col in enumerate(cursor.description):
        d[col[0]] = row[i]
    return d


def get_connection():
    print('Creating database connection, started.')
    assert isinstance(configs.database, str)
    connection = sqlite3.connect(configs.database)
    connection.row_factory = dict_factory
    print('Creating database connection, completed.')
    return connection
