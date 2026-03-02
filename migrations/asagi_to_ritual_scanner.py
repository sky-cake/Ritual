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

- loop over batches of `hashtab.md5_b64_given` hashes in scanner.db
    - look for `<board>.media_hash` matches in ritual.db across all boards
    - check if any Asagi convention `<board>.media_orig` filepaths exist
        - if none match, or no filepaths exist
            - continue
        - if multiple filepaths for the media hash exist, assert all their sha256 hashes are equal.
            - if they are not, prompt the user
                - print the paths of the differing files
                - ask the user what to do,
                    - default: (S)kip and log each media hash on a line like `<md5_b64_given>: [<filepath1>, <filepath2>, ...]`
                    -          (P)ick an file to migrate, delete the others. Requires a second prompt "Choose filepath (1-N): ".
                    -          (R)andom file is chosen to migrate, the others are deleted.
                    -          (D)elete all files. Requires a second "Are you sure? (y/n)" prompt.
            - else pick any file
                - full media and thumbnail files get moves to the Ritual Scanner filepaths
                - delete the remaining matching Asagi convention filepaths
        - if one filepath for the media hash exists,
            - full media and thumbnail files get moves to the Ritual Scanner filepaths

Ritual Scanner filepath convention:
- full media: `.../img/sha256[:2]/sha256[2:4]/sha256[4:6]/sha256.ext/sha256<ext>`
- thumbnail: `.../thb/sha256[:2]/sha256[2:4]/sha256[4:6]/sha256.ext/sha256.jpg`

Asagi filepath convention:
- full media: `.../<board>/image/tim[:4]/tim[4:6]/tim<ext>`
- thumbnail: `.../<board>/thumb/tim[:4]/tim[4:6]/tim<s.jpg>`
"""

import os
import shutil
import sqlite3
from itertools import batched
import base64
import hashlib


def get_sha256_and_md5_b64(path: str, buffer_limit: int) -> tuple[str, str]:
    sha = hashlib.sha256()
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        while True:
            b = f.read(buffer_limit)
            if not b:
                break
            sha.update(b)
            md5.update(b)
    return sha.hexdigest(), base64.b64encode(md5.digest()).decode()



def ritual_paths(root_img: str, root_thb: str, sha256: str, ext: str) -> tuple[str, str]:
    sub = os.path.join(sha256[:2], sha256[2:4], sha256[4:6], f'{sha256}.{ext}')
    img = os.path.join(root_img, sub, f'{sha256}.{ext}')
    thb = os.path.join(root_thb, sub, f'{sha256}.jpg')
    return img, thb


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
    cur.execute('select distinct md5_b64_given from hashtab where md5_b64_given is not null;')
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
        h.md5_b64_given,
        h.sha256
    from hashtab h
        join directory d using (dir_id)
        join extension e using (ext_id)
    where h.md5_b64_given = ({placeholders})
    ;
    '''
    return db.execute(sql, parameters=md5_batch).fetchall()


def assert_sha256_equal(rows: list[tuple]) -> str | None:
    sha = None
    for _, _, _, s in rows:
        if sha is None:
            sha = s
        elif sha != s:
            return None
    return sha


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
            #   h.md5_b64_given,
            #   h.sha256,
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