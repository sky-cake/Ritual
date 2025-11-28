import asyncio
import configs
from db_base import BaseDb
from db_mysql import MysqlDb
from db_sqlite import SqliteDb
from utils import get_d_board
from asagi_tables.main import execute_action


class RitualDb:
    def __init__(self, db: BaseDb):
        self.db = db

        boards_list = list(configs.boards.keys())
        side_tables = ['threads', 'images', 'deleted']

        async def setup_tables():
            configs.logger.info('Creating base tables.')
            await execute_action('base', 'table_add', boards_list)

            configs.logger.info('Creating side tables.')
            await execute_action('side', 'table_add', boards_list, side_tables=side_tables)

            configs.logger.info('Creating base indexes.')
            await execute_action('base', 'index_add', boards_list)

            configs.logger.info('Creating side indexes.')
            await execute_action('side', 'index_add', boards_list, side_tables=side_tables)

        asyncio.run(setup_tables())
        self.db.save()


    def get_pids_by_tid(self, board: str, tid: int) -> list[int]:
        ph = self.db.placeholder
        rows = self.db.run_query_tuple(f'select num from `{board}` where thread_num = {ph}', params=(tid,))
        return [row[0] for row in rows] if rows else []


    def set_posts_deleted(self, board: str, pids: list[int]) -> None:
        if not pids:
            return

        ph = self.db.placeholder
        placeholders = ','.join([ph] * len(pids))
        sql = f"update `{board}` set deleted = 1 where num in ({placeholders});"
        self.db.run_query_tuple(sql, params=tuple(pids), commit=True)


    def set_threads_deleted(self, board: str, tids: list[int]) -> None:
        if not tids:
            return

        ph = self.db.placeholder
        placeholders = ','.join([ph] * len(tids))
        sql = f"update `{board}` set deleted = 1 where num in ({placeholders});"
        self.db.run_query_tuple(sql, params=tuple(tids), commit=True)


    def upsert_many(self, board: str, rows: list[dict], conflict_col: str, batch_size: int=500):
        if not rows:
            return

        keys = list(rows[0])
        ph = self.db.placeholder
        placeholder = '(' + ','.join([ph] * len(keys)) + ')'
        sql_cols = ', '.join(keys)
        sql_conflict = self.db.get_upsert_clause(conflict_col, tuple(keys))

        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            placeholders = ', '.join([placeholder] * len(chunk))
            sql = f"insert into `{board}` ({sql_cols}) values {placeholders} {sql_conflict};"
            flat_values = [v for row in chunk for v in row.values()]
            self.db.run_query_tuple(sql, params=tuple(flat_values), commit=True)


    def get_existing_media_hashes(self, board: str, media_hashes: list[str]) -> set[str]:
        if not media_hashes:
            return set()

        ph = self.db.placeholder
        placeholders = ','.join([ph] * len(media_hashes))
        sql = f"""
            select distinct media_hash
            from `{board}`
            where
                media_hash in ({placeholders})
                and media_hash is not null;
        """
        rows = self.db.run_query_tuple(sql, params=tuple(media_hashes))
        return {row[0] for row in rows} if rows else set()


    def get_media_hash_info(self, board: str, media_hashes: list[str]) -> tuple[dict[str, str], set[str]]:
        if not media_hashes:
            return dict(), set()

        ph = self.db.placeholder
        placeholders = ','.join([ph] * len(media_hashes))
        sql = f"""
            select media_hash, media, banned
            from `{board}_images`
            where media_hash in ({placeholders});
        """
        rows = self.db.run_query_tuple(sql, params=tuple(media_hashes))
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
        if not media_hash:
            return

        ph = self.db.placeholder
        if isinstance(self.db, SqliteDb):
            conflict_clause = 'on conflict(media_hash) do update set total = total + 1, media = coalesce(media, excluded.media)'
        else:
            conflict_clause = 'on duplicate key update total = total + 1, media = coalesce(media, values(media))'
        sql = f"""
            insert into `{board}_images` (media_hash, media, total, banned)
            values ({ph}, {ph}, 1, 0)
            {conflict_clause};
        """
        self.db.run_query_tuple(sql, params=(media_hash, media), commit=True)


    def upsert_posts(self, board: str, posts: list[dict]):
        posts_to_insert = []

        for post in posts:
            d_board = get_d_board(post, unescape_data_b4_db_write=configs.unescape_data_b4_db_write)
            posts_to_insert.append(d_board)

        self.upsert_many(board, posts_to_insert, 'num, subnum')


    def upsert_thread_stats(self, board: str, thread_stats: dict):
        ph = self.db.placeholder
        update_cols = ('time_op', 'time_last', 'time_bump', 'time_ghost', 'time_ghost_bump', 'time_last_modified', 'nreplies', 'nimages', 'sticky', 'locked')
        conflict_clause = self.db.get_upsert_clause('thread_num', update_cols)
        placeholders = ', '.join([ph] * (len(update_cols) + 1))
        sql = f"""
            insert into `{board}_threads` (
                thread_num, time_op, time_last, time_bump, time_ghost, time_ghost_bump, time_last_modified, nreplies, nimages, sticky, locked
            )
            values ({placeholders})
            {conflict_clause}
        """
        self.db.run_query_tuple(
            sql,
            params=(
                thread_stats['thread_num'],
                thread_stats['time_op'],
                thread_stats['time_last'],
                thread_stats['time_bump'],
                thread_stats.get('time_ghost'),
                thread_stats.get('time_ghost_bump'),
                thread_stats['time_last_modified'],
                thread_stats['nreplies'],
                thread_stats['nimages'],
                thread_stats['sticky'],
                thread_stats['locked'],
            ),
            commit=True
        )


    def save_and_close(self):
        self.db.save_and_close()


def create_ritual_db() -> RitualDb:
    if configs.db_type == 'mysql':
        db = MysqlDb(
            host=configs.db_mysql_host,
            user=configs.db_mysql_user,
            password=configs.db_mysql_password,
            database=configs.db_mysql_database,
            port=configs.db_mysql_port,
            sql_echo=configs.db_echo
        )
    elif configs.db_type == 'sqlite':
        db = SqliteDb(configs.db_sqlite_path, configs.db_echo)
    else:
        raise ValueError(configs.db_type)

    return RitualDb(db)
