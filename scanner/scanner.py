import os
import sys
from itertools import batched

from configs import Config
from db_scanner import ScannerDb
from file_meta_extract import get_sha256_and_md5_b64
from mm import MediaMeta
from progress import Counter
from utils import iter_media_files


def get_and_set_metadata(mm: MediaMeta, file_read_buffer_size: int) -> MediaMeta:

    mm.bsize = os.path.getsize(mm.fullpath)
    mm.sha256, mm.md5_b64_computed = get_sha256_and_md5_b64(mm.fullpath, file_read_buffer_size)

    return mm


def gather_filesystem(root_path: str, db: ScannerDb, skip_dirnames: set[str], exts: set[str], batch_size: int):
    """
    Crawls a root path recursively, creating entries of existing files in the sql table hashtab.
    """
    dirpath_2_id = dict()

    ext_2_id = db.fetch_ext_map()

    counter = Counter('catalog_filesystem', batch_size)

    sql_insert = '''insert or ignore into hashtab (dir_id, filename_no_ext, ext_id) values (?,?,?);'''

    for batch in batched(iter_media_files(root_path, skip_dirnames=skip_dirnames, valid_exts=exts), batch_size):
        batch = tuple(
            (
                db.get_and_set_dir_id(dirpath_2_id, dirpath),
                filename_no_ext,
                db.get_and_set_ext_id(ext_2_id, ext),
            )
            for dirpath, filename_no_ext, ext in batch
        )

        db.run_query_many(sql_insert, batch, commit=True)

        counter(increment_by=len(batch))
    print('\ncatalog_filesystem, completed')


def gather_metadata(db: ScannerDb, batch_size: int, file_read_buffer_size: int):
    """
    Upsert all file metadata in the table hashtab.
    """

    counter = Counter('gather_metadata', batch_size)

    sql_insert = '''
    insert or ignore into hashtab
    (
        dir_id,
        filename_no_ext,
        ext_id,
        error,
        md5_b64_given,
        md5_b64_computed,
        sha256,
        bsize
    )
    values (?,?,?,?,?,?,?,?)
    on conflict (dir_id, filename_no_ext, ext_id)
    do update set
        error            = excluded.error,
        md5_b64_given    = excluded.md5_b64_given,
        md5_b64_computed = excluded.md5_b64_computed,
        sha256           = excluded.sha256,
        bsize            = excluded.bsize
    ;
    '''

    missing_meta_file_count = int(db.run_query_tuple('select count(*) from hashtab where error is null;')[0][0])
    if missing_meta_file_count == 0:
        print('Nothing to do - all files have had their metadata gathered already.')
        return

    print(f'Starting to gather metadata for {missing_meta_file_count} files...')

    while True:
        sql_select = f'''
        select
            dirpath,
            filename_no_ext,
            ext,
            dir_id,
            ext_id
        from hashtab
            join extension using (ext_id)
            join directory using (dir_id)
        where
            hashtab.error is null
        limit {batch_size}
        ;'''
        rows = db.run_query_tuple(sql_select)
        if not rows:
            break

        params = []
        for row in rows:
            mm = get_and_set_metadata(MediaMeta(
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
            ), file_read_buffer_size)
            params.append((
                mm.dir_id,
                mm.filename_no_ext,
                mm.ext_id,
                0,
                mm.md5_b64_given,
                mm.md5_b64_computed,
                mm.sha256,
                mm.bsize
            ))

        db.run_query_many(sql_insert, params=params)
        counter(increment_by=len(rows))
    print('\ngather_metadata, completed')


if __name__ == '__main__':
    config = Config()
    try:
        config.print_and_verify()
        print('Running...')
    except AssertionError:
        print('Exiting...')
        sys.exit(0)

    db = ScannerDb(config.db_path)
    db.init_db()

    gather_filesystem(config.root_path, db, config.skip_dirnames, config.file_exts, config.gather_filesystem_batch_size)

    gather_metadata(db, config.gather_metadata_batch_size, config.file_read_buffer_size)

    db.save_and_close()
