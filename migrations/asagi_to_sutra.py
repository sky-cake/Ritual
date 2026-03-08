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

- loop over batches of `hashtab.md5_computed` hashes in scanner.db
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
            - else, pick any file
                - full media and thumbnail files get moves to the Sutra filepaths
                - delete the remaining matching AsagiMediaFPs
        - if one filepath for the media hash exists,
            - full media and thumbnail files get moves to the Sutra filepaths
"""


import os
import sqlite3
import hashlib
import random
import shutil
import base64


_b64_fs_table = str.maketrans({'+': '-', '/': '_'})


def get_fs_safe_b64(b64: str) -> str:
    return b64.translate(_b64_fs_table)


def get_md5_b64(path: str) -> str:
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(10_485_760), b''):
            md5.update(chunk)
    return base64.b64encode(md5.digest()).decode()


def get_sutra_paths(root: str, md5: str, ext: str):
    safe = get_fs_safe_b64(md5)
    dirpath = os.path.join(root, 'image', safe[:4], safe[4:6], safe[6:8])
    filepath = os.path.join(dirpath, f'{safe}{ext}')
    return dirpath, filepath


def get_sutra_thumb(root: str, md5: str):
    safe = get_fs_safe_b64(md5)
    dirpath = os.path.join(root, 'thumb', safe[:4], safe[4:6], safe[6:8])
    filepath = os.path.join(dirpath, f'{safe}.jpg')
    return dirpath, filepath


def get_asagi_full(root: str, board: str, media: str):
    tim = media[:-len(os.path.splitext(media)[1])]
    return os.path.join(root, board, 'image', tim[:4], tim[4:6], media)


def get_asagi_thumb(root: str, board: str, media: str):
    tim = media[:-len(os.path.splitext(media)[1])]
    return os.path.join(root, board, 'thumb', tim[:4], tim[4:6], f'{tim}s.jpg')


def query_hashes(cur, batch: int):
    rows = cur.fetchmany(batch)
    while rows:
        yield [r[0] for r in rows]
        rows = cur.fetchmany(batch)


def get_boards(cur):
    cur.execute("select name from sqlite_master where type='table'")
    tables = [r[0] for r in cur.fetchall() if r[0].endswith('_images')]
    return [t[:-7] for t in tables]


def get_media_paths(cur, boards, md5, root):
    paths = []
    for board in boards:
        cur.execute(f'select media from `{board}_images` where media_hash=?', (md5,))
        for (media,) in cur.fetchall():
            p = get_asagi_full(root, board, media)
            if os.path.isfile(p):
                paths.append((board, media, p))
    return paths


def verify_same_md5(paths):
    hashes = {get_md5_b64(p) for p in paths}
    return len(hashes) == 1


def prompt_user(md5, paths):
    print('\nconflict for', md5)
    for i, p in enumerate(paths, 1):
        print(f'{i}: {p}')
    print('(S)kip\n(P)ick\n(R)andom\n(D)elete all')
    choice = input('choice: ').lower().strip()
    if choice == 'p':
        idx = int(input(f'Choose filepath (1-{len(paths)}): '))
        return paths[idx - 1], 'pick'
    if choice == 'r':
        return random.choice(paths), 'pick'
    if choice == 'd' and input('Are you sure? (y/n) ').lower() == 'y':
        return None, 'delete'
    return None, 'skip'


def remove_other_links(src_path, keep_path):
    # remove all hard links or symlinks to a source file except the chosen one

    if not os.path.exists(src_path):
        return

    # Get inode and device info for detecting hard links
    stat_info = os.stat(src_path)
    inode = stat_info.st_ino
    device = stat_info.st_dev
    # list all files in the source directory tree to find links with same inode
    for root_dir, _, files in os.walk(os.path.dirname(src_path)):
        for f in files:
            fpath = os.path.join(root_dir, f)
            try:
                if fpath == keep_path:
                    continue
                # remove symlinks
                if os.path.islink(fpath):
                    os.unlink(fpath)
                    continue
                # remove hard links with same inode on same device
                st = os.stat(fpath)
                if st.st_ino == inode and st.st_dev == device:
                    os.unlink(fpath)
            except FileNotFoundError:
                continue


def move_to_sutra(asagi_root: str, sutra_root: str, md5: str, board: str, media: str):
    full_src = get_asagi_full(asagi_root, board, media)
    thumb_src = get_asagi_thumb(asagi_root, board, media)
    ext = os.path.splitext(media)[1]

    dir_full, file_full = get_sutra_paths(sutra_root, md5, ext)
    dir_thumb, file_thumb = get_sutra_thumb(sutra_root, md5)

    os.makedirs(dir_full, exist_ok=True)
    os.makedirs(dir_thumb, exist_ok=True)

    # remove other hard links or symlinks before moving full media
    remove_other_links(full_src, full_src)
    if os.path.isfile(full_src):
        shutil.move(full_src, file_full)  # Move main file to Sutra location

    # remove other hard links or symlinks before moving thumbnail
    remove_other_links(thumb_src, thumb_src)
    if os.path.isfile(thumb_src):
        shutil.move(thumb_src, file_thumb) # move thumbnail to Sutra location


def migrate(scanner_db_path, ritual_db_path, asagi_root, sutra_root, log_file_path, batch=500):
    scanner_con = sqlite3.connect(scanner_db_path)
    scanner_cur = scanner_con.cursor()
    scanner_cur.execute('select md5_computed from hashtab')

    ritual_con = sqlite3.connect(ritual_db_path)
    ritual_cur = ritual_con.cursor()
    boards = get_boards(ritual_cur)

    log_file = open(log_file_path, 'a')

    for hashes in query_hashes(scanner_cur, batch):
        for md5 in hashes:
            matches = get_media_paths(ritual_cur, boards, md5, asagi_root)
            if not matches:
                continue

            files = [p for _, _, p in matches]
            if len(files) > 1:
                if not verify_same_md5(files):
                    chosen, action = prompt_user(md5, files)
                    if action == 'skip':
                        log_file.write(f'{md5}: {[str(p) for p in files]}\n')
                        continue
                    if action == 'delete':
                        for p in files:
                            os.remove(p)
                        continue
                else:
                    chosen = files[0]
            else:
                chosen = files[0]

            board, media, _ = next(x for x in matches if x[2] == chosen)
            move_to_sutra(asagi_root, sutra_root, md5, board, media)

            for _, _, p in matches:
                if p != chosen and os.path.isfile(p):
                    os.remove(p)

    log_file.close()
    scanner_con.close()
    ritual_con.close()


if __name__ == '__main__':
    migrate(
        scanner_db_path='/path/to/scanner/scanner.db',
        ritual_db_path='/path/to/ritual/ritual.db',
        asagi_root='/path/to/media/asagi',
        sutra_root='/path/to/media/sutra',
        log_file_path='/path/to/migration.log',
        batch=500
    )
