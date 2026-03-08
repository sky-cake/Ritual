import os
import sys
from itertools import batched

from configs import Config
from db_scanner import ScannerDb
from file_meta_extract import get_sha256_and_md5_b64
from progress import Counter
from utils import iter_media_files


def gather_filesystem(root_path: str, db: ScannerDb, skip_dirnames: set[str], exts: set[str], batch_size: int):
    """
    Crawls a root path recursively, creating entries of existing files in the sql table `hashtab`.
    Uses `hashtab_view` and `trigger_insert_hashtab_view` to automatically handle directory and extension mappings to ids.
    """
    counter = Counter('catalog_filesystem', batch_size)

    sql_insert = '''insert or ignore into hashtab_view (dirpath, filename_no_ext, ext) values (?,?,?);'''

    for batch in batched(iter_media_files(root_path, skip_dirnames=skip_dirnames, valid_exts=exts), batch_size):
        batch = tuple((dirpath, filename_no_ext, ext) for dirpath, filename_no_ext, ext in batch)
        db.run_query_many(sql_insert, batch, commit=True)
        counter(increment_by=len(batch))
    print('\ncatalog_filesystem, completed')


def gather_metadata(db: ScannerDb, batch_size: int):
    """
    Upserts hashtab with metadata.
    """
    counter = Counter('gather_metadata', batch_size)

    missing_meta_file_count = int(db.run_query_tuple('select count(*) from hashtab where sha256 is null and is_saved is null;')[0][0])
    if missing_meta_file_count == 0:
        print('Nothing to do - all files have had their metadata gathered already.')
        return

    print(f'Starting to gather metadata for ({missing_meta_file_count}) files...')

    sql_select = f'''
    select
        d.dirpath,
        h.filename_no_ext,
        e.ext
    from hashtab h
        join directory d using (dir_id)
        join extension e using (ext_id)
    where
        h.sha256 is null and is_saved is null
    limit {int(batch_size)};
    '''

    # trigger handles the upsert on hashtab
    sql_insert = '''
    insert or ignore into hashtab_view
    (
        dirpath,
        filename_no_ext,
        ext,
        md5_computed,
        fsize_computed,
        sha256,
        is_saved
    )
    values (?,?,?,?,?,?,?);
    '''

    while True:
        rows = db.run_query_tuple(sql_select)
        if not rows:
            break

        params = []
        for row in rows:
            fullpath = os.path.join(row[0], f'{row[1]}.{row[2]}')

            # file could have been deleted since the gather_filesystem()'s last run
            if not os.path.isfile(fullpath):
                is_saved = 0
                md5_computed = None
                sha256 = None
                fsize_computed = None
            else:
                is_saved = 1
                sha256, md5_computed = get_sha256_and_md5_b64(fullpath)
                fsize_computed = os.path.getsize(fullpath)

            params.append((
                row[0],
                row[1],
                row[2],
                md5_computed,
                fsize_computed,
                sha256,
                is_saved,
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

    gather_metadata(db, config.gather_metadata_batch_size)

    db.save_and_close()
