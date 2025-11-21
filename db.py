import sqlite3

import configs


assert isinstance(configs.db_path, str)


def dict_factory(cursor, row):
    d = {}
    for i, col in enumerate(cursor.description):
        d[col[0]] = row[i]
    return d


def get_connection():
    # print('Creating database connection, started.')
    connection = sqlite3.connect(configs.db_path)
    connection.row_factory = dict_factory
    # print('Creating database connection, completed.')
    return connection
