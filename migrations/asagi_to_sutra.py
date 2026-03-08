"""
There exists a bunch of hard linked inodes from running fclones https://github.com/pkolaczk/fclones in the past with the following commands,

1. fclones group ./media --cache ./fcc > dupes.txt
2. fclones link --priority oldest < dupes.txt

So now the hard links should be removed, and a purely de-duplicated root media folder,

1. fclones group /path/to/root/media --match-links > dups_ml.txt
    - treats all linked files as duplicates
    - https://github.com/pkolaczk/fclones?tab=readme-ov-file#handling-links
2. fclones remove --priority oldest <dupes.txt
    - remove the oldest replicas
    - https://github.com/pkolaczk/fclones?tab=readme-ov-file#removing-files

Pseudo code,

- loop over batches of `hashtab.md5` hashes in scanner.db
    - look for `<board>.media_hash` matches in ritual.db across all boards
    - check if any AsagiMediaFP `<board>.media_orig` filepaths exist
        - if none match, or no filepaths exist
            - continue
        - if multiple filepaths for the media hash exist, assert all their md5 hashes are equal.
            - if they are not, prompt the user
                - print the paths of the differing files
                - ask the user what to do,
                    - default: (S)kip and log each media hash on a line like `<md5>: [<filepath1>, <filepath2>, ...]`
                    -          (P)ick an file to migrate, delete the others. Requires a second prompt "Choose filepath (1-N): ".
                    -          (R)andom file is chosen to migrate, the others are deleted.
                    -          (D)elete all files. Requires a second "Are you sure? (y/n)" prompt.
            - else pick any file
                - full media and thumbnail files get moves to the Sutra filepaths
                - delete the remaining matching AsagiMediaFPs
        - if one filepath for the media hash exists,
            - full media and thumbnail files get moves to the Sutra filepaths

SutraMediaFP:
- full media: `.../img/md5[:2]/md5[2:4]/md5[4:6]/md5.ext/md5<ext>`
- thumbnail: `.../thb/md5[:2]/md5[2:4]/md5[4:6]/md5.ext/md5.jpg`

AsagiMediaFP:
- full media: `.../<board>/image/tim[:4]/tim[4:6]/tim<ext>`
- thumbnail: `.../<board>/thumb/tim[:4]/tim[4:6]/tim<s.jpg>`
"""

import os
import shutil
import sqlite3
from itertools import batched
import base64
import hashlib


def get_md5_b64(path: str) -> str:
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(10_485_760), b''):
            md5.update(chunk)
    return base64.b64encode(md5.digest()).decode()


_b64_fs_trans = str.maketrans({
    '+': '-',
    '/': '_',
})


def get_fs_safe_b64(b64: str) -> str:
    return b64.translate(_b64_fs_trans)


def ensure_parent(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def copy_file(src: str, dst: str):
    ensure_parent(dst)
    shutil.copy2(src, dst, follow_symlinks=False)


def unlink_if_exists(path: str):
    if os.path.exists(path):
        os.unlink(path)


def fetch_md5_batches(db: sqlite3.Connection, batch_size: int):
    cur = db.cursor()
    cur.execute('select distinct md5 from hashtab where md5 is not null;')
    rows = [r[0] for r in cur.fetchall()]
    for batch in batched(rows, batch_size):
        yield batch


def fetch_files_for_md5s(db: sqlite3.Connection, md5_batch: list[str]) -> tuple:
    placeholders = ','.join(['?'] * len(md5_batch))
    sql = f'''
    select
        d.dirpath,
        h.filename_no_ext,
        e.ext,
        h.md5
    from hashtab h
        join directory d using (dir_id)
        join extension e using (ext_id)
    where h.md5 = ({placeholders})
    ;
    '''
    return db.execute(sql, parameters=md5_batch).fetchall()


def assert_md5_computed_equal(rows: list[tuple]) -> str | None:
    md5 = None
    for _, _, _, m in rows:
        if md5 is None:
            md5 = m
        elif md5 != m:
            return None
    return md5


def prompt_conflict(md5: str, rows: list[tuple]) -> tuple[str, int | None]:
    print(f'conflict for md5 {md5}')
    for i, r in enumerate(rows, 1):
        print(f'{i}. {os.path.join(r[0], r[1] + "." + r[2])}')
    print('(S)kip  (P)ick  (R)andom  (D)elete all')
    choice = input('> ').strip().lower()
    if choice == 'p':
        idx = int(input('Choose filepath (1-N): ')) - 1
        return 'pick', idx
    if choice == 'r':
        return 'random', None
    if choice == 'd':
        confirm = input('Are you sure? (y/n): ').strip().lower()
        if confirm == 'y':
            return 'delete', None
        return 'skip', None
    return 'skip', None


def make_path(*filepaths):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *filepaths)


def main(
    scanner_db_path: str,
    ritual_img_root: str,
    ritual_thb_root: str,
    batch_size: int,
    dry_run: bool,
):
    db = sqlite3.connect(scanner_db_path, autocommit=True)

    with open(make_path('migration.log'), 'w') as log:
        for md5_batch in fetch_md5_batches(db, batch_size):
            rows: tuple = fetch_files_for_md5s(db, md5_batch)
            # rows = [
            #   d.dirpath,
            #   h.filename_no_ext,
            #   e.ext,
            #   h.md5,
            # ]

    db.close()


# Work in progress

# if __name__ == '__main__':
#     main(
#         scanner_db_path='/path/to/scanner/scanner.db',
#         ritual_img_root='/path/to/media/img',
#         ritual_thb_root='/path/to/media/thb',
#         batch_size=500,
#         dry_run=True,
#     )