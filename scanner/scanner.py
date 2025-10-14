import os
import sys
from itertools import batched
from path import Path
from file_meta_extract import get_dimensions, hash_file
from db_scanner import ScannerDb
from progress import Counter


def iter_media_files(root: str, skip_dirnames: set[str] | None=None):
    for dirpath, dirnames, filenames in os.walk(root):
        dirname = os.path.basename(dirpath)
        if skip_dirnames and dirname in skip_dirnames:
            dirnames[:] = []
            continue
        for filename in filenames:
            if '.' in filename:
                filename_no_ext, ext = filename.rsplit('.', maxsplit=1)
                if ext in IMAGE_EXTS or ext in VIDEO_EXTS:
                    yield dirpath, filename_no_ext, ext


def get_and_set_metadata(path: Path) -> Path:

    path.bsize = os.path.getsize(path.fullpath)
    path.sha256, path.md5_b64 = hash_file(path.fullpath, HASH_BUFFER_LIMIT)
    path.w, path.h = get_dimensions(path.fullpath)

    return path


def catalog_filesystem(root_dir: str, db: ScannerDb, skip_dirnames: set[str]):
    dirpath_2_id = dict()

    ext_2_id = db.fetch_ext_map()

    batch_size = 25_000
    counter = Counter('catalog_filesystem', batch_size)

    sql_insert = '''insert or ignore into file (dir_id, filename_no_ext, ext_id) values (?,?,?)'''

    for batch in batched(iter_media_files(root_dir, skip_dirnames=skip_dirnames), batch_size):
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


def gather_metadata(db: ScannerDb):
    limit = 500
    counter = Counter('gather_metadata', limit)

    sql_insert = '''
    insert or ignore into file
    (
        dir_id,
        filename_no_ext,
        ext_id,
        attempted,
        sha256,
        md5_b64,
        bsize,
        width,
        height
    )
    values (?,?,?,?,?,?,?,?,?)
    on conflict (dir_id, filename_no_ext, ext_id)
    do update set
        attempted = excluded.attempted,
        sha256    = excluded.sha256,
        md5_b64   = excluded.md5_b64,
        bsize     = excluded.bsize,
        width     = excluded.width,
        height    = excluded.height
    ;
    '''

    missing_meta_file_count = db.run_query_tuple('select count(*) from file where attempted is null')[0][0]
    if missing_meta_file_count == 0:
        print('All files have had metadata scanned and stored.')
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
        from file
            join extension using (ext_id)
            join directory using (dir_id)
        where
            file.attempted is null
        limit {limit}
        '''
        rows = db.run_query_tuple(sql_select)
        if not rows:
            break

        params = []
        for row in rows:
            path = get_and_set_metadata(Path(
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
            ))
            params.append((
                path.dir_id,
                path.filename_no_ext,
                path.ext_id,
                1,
                path.sha256,
                path.md5_b64,
                path.bsize,
                path.w,
                path.h,
            ))

        db.run_query_many(sql_insert, params=params, commit=True)
        counter(increment_by=len(rows))
    print('\ngather_metadata, completed')


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: python3 scanner.py <db_path> <root_dir>')
        sys.exit(1)

    db_path = os.path.realpath(sys.argv[1])
    root_dir = os.path.realpath(sys.argv[2])
    skip_dirnames = {'thumb'} # asagi fs layout

    # ext ids mappings are not hard-coded
    # you can add any other exts supported by get_dimensions()
    IMAGE_EXTS = {'jpg', 'jpeg', 'png', 'gif'}
    VIDEO_EXTS = {'mp4', 'webm'}

    # image board file size limits are typically 4MB
    HASH_BUFFER_LIMIT = 10 * 1024 * 1024

    print(f'{db_path=}')
    print(f'{root_dir=}')
    print(f'{skip_dirnames=}')

    assert input('Proceed? (y/n)') == 'y'

    db = ScannerDb(db_path)

    # custom sql
    sqls = '''
    pragma journal_mode=wal;
    pragma synchronous=normal;
    pragma temp_store=memory;
    '''
    for sql in sqls.split(';'):
        db.run_query_tuple(sql)

    # ensure schema exists
    db.init_db()

    # meant as a fast filesystem scan
    catalog_filesystem(root_dir, db, skip_dirnames)

    # uses the filesystem catalog to crawl files without stored metadata
    gather_metadata(db)

    # commit any leftover transactions
    db.save_and_close()
