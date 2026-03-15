import os
import sys
from itertools import batched

from configs import Config
from db_scanner import ScannerDb
from file_meta_extract import get_md5_b64
from progress import Counter
from utils import iter_media_files


def gather_filesystem(root_path: str, db: ScannerDb, skip_dirnames: set[str], exts: set[str], batch_size: int):
    """
    Crawls a root path recursively, creating entries of existing files in the sql table `hashtab`.
    """
    counter = Counter('catalog_filesystem', batch_size)

    dir_cache = db.fetch_dir_map()
    ext_cache = db.fetch_ext_map()

    sql_insert_hashtab = '''
        insert or ignore into hashtab (dir_id, filename_no_ext, ext_id)
        values (?,?,?);
    '''

    for batch in batched(iter_media_files(root_path, skip_dirnames=skip_dirnames, valid_exts=exts), batch_size):
        params = []
        for dirpath, filename_no_ext, ext in batch:
            dir_id = db.get_and_set_dir_id(dir_cache, dirpath)
            ext_id = db.get_and_set_ext_id(ext_cache, ext)
            params.append((dir_id, filename_no_ext, ext_id))

        db.run_query_many(sql_insert_hashtab, params=params, commit=True)
        counter(increment_by=len(batch))

    print('\ncatalog_filesystem, completed')


def gather_metadata(db: ScannerDb, batch_size: int):
    counter = Counter('gather_metadata', batch_size)

    missing_meta_file_count = int(db.run_query_tuple('select count(*) from hashtab where md5_computed is null and is_saved is null;')[0][0])
    if missing_meta_file_count == 0:
        print('Nothing to do - all files have had their metadata gathered already.')
        return

    print(f'Starting to gather metadata for ({missing_meta_file_count}) files...')

    sql_select = f'''
    select
        h.rowid,
        d.dirpath,
        h.filename_no_ext,
        e.ext
    from hashtab h
        join directory d using (dir_id)
        join extension e using (ext_id)
    where
        h.md5_computed is null and is_saved is null
    limit {int(batch_size)};
    '''

    sql_update = '''update hashtab set md5_computed = ?, fsize_computed = ?, is_saved = ? where rowid = ?'''

    while True:
        rows = db.run_query_tuple(sql_select)
        if not rows:
            break

        params = []
        for rowid, dirpath, filename_no_ext, ext in rows:
            fullpath = os.path.join(dirpath, f'{filename_no_ext}.{ext}')

            # file could have been deleted since the gather_filesystem()'s last run
            if not os.path.isfile(fullpath):
                is_saved = 0
                md5_computed = None
                fsize_computed = None
            else:
                is_saved = 1
                md5_computed = get_md5_b64(fullpath)
                fsize_computed = os.path.getsize(fullpath)

            params.append((
                md5_computed,
                fsize_computed,
                is_saved,
                rowid,
            ))

        db.run_query_many(sql_update, params=params, commit=True)
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
