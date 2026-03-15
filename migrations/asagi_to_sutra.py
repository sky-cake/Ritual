"""
There exists a bunch of hard linked inodes from running fclones https://github.com/pkolaczk/fclones
in the past with the following commands,

    1. fclones group ./media --cache ./fcc > dupes.txt
    2. fclones link --priority newest < dupes.txt

So now the hard links should be removed, leaving a purely de-duplicated root media folder,

    1. fclones group /path/to/root/media --match-links > dups_ml.txt
        - treats all linked files as duplicates
        - https://github.com/pkolaczk/fclones?tab=readme-ov-file#handling-links
    2. fclones remove --priority newest < dupes.txt
        - remove the newest replicas. The olders files are less likely to be maliciously md5 overwritten
        - https://github.com/pkolaczk/fclones?tab=readme-ov-file#removing-files

Once that is done, this script should migrates from `media_fp.AsagiMediaFP` to `media_fp.SutraMediaFP`.
"""


import os
import sqlite3


all_boards = [
    '3','a','aco','adv','an','b','bant','biz','c','cgl','ck','cm','co','diy','e','f','fa','fit','g','gd','gif','h','hc','his','hm','hr','i','ic','int','jp','k','lgbt','lit','m','mlp','mu','n','news','o','out','p','po','pol','pw','qst','r','r9k','s','s4s','sci','soc','sp','t','tg','toy','trv','tv','u','v','vg','vip','vm','vmg','vp','vr','vrpg','vst','vt','w','wg','wsg','wsr','x','xs','y',
]


def get_valid_boards(db_path_ritual: str) -> list[str]:
    valid = []
    con = sqlite3.connect(db_path_ritual)
    cur = con.cursor()
    for board in all_boards:
        try:
            cur.execute(f'SELECT 1 FROM `{board}` LIMIT 1')
            valid.append(board)
            print(f'  {board}: exists')
        except sqlite3.OperationalError:
            print(f'  {board}: not found')
    con.close()
    return valid


def confirm(prompt: str) -> bool:
    while True:
        response = input(f'{prompt} [y/n]: ').strip().lower()
        if response in ('y', 'yes', 'true'):
            return True
        if response in ('n', 'no', 'false'):
            return False
        print('Please enter y or n')


def build_union_sql(boards: list[str]) -> str:
    parts = [f'SELECT media_hash, media_orig FROM `{b}`' for b in boards]
    return ' UNION ALL '.join(parts)


def migrate(boards: list[str], db_path_scanner: str, db_path_ritual: str, root_asagi: str, root_sutra: str):
    scanner_con = sqlite3.connect(db_path_scanner)
    ritual_con = sqlite3.connect(db_path_ritual)

    scanner_cur = scanner_con.cursor()
    ritual_cur = ritual_con.cursor()

    try:
        scanner_cur.execute('SELECT COUNT(*) FROM hashtab')
        total_in_scanner = scanner_cur.fetchone()[0]
        print(f'total files in scanner: {total_in_scanner}')

        scanner_cur.execute('SELECT filename_no_ext || "." || ext FROM hashtab JOIN extension USING (ext_id)')

        sutra_full_root = os.path.join(root_sutra, 'image')
        sutra_thumb_root = os.path.join(root_sutra, 'thumb')

        union_sql = build_union_sql(boards)

        moved = 0
        processed = 0
        dir_cache: set[str] = set()
        trans_table = str.maketrans({'+': '-', '/': '_'})

        while True:
            filenames = [r[0] for r in scanner_cur.fetchmany(1000)]
            if not filenames:
                break

            placeholder = ','.join('?' for _ in filenames)
            sql = f'select media_hash, media_orig from ({union_sql}) where media_orig in ({placeholder})'
            rows: tuple[str, str] = ritual_cur.execute(sql, filenames)

            for media_hash, media_orig in rows:
                if not media_hash or not media_orig:
                    continue

                media_hash = media_hash.translate(trans_table)

                name, ext = media_orig.rsplit('.', 1)

                board = None
                for b in boards:
                    if os.path.exists(os.path.join(root_asagi, b, 'image', media_orig[:4], media_orig[4:6], media_orig)):
                        board = b
                        break
                if board is None:
                    continue

                asagi_full_root = os.path.join(root_asagi, board, 'image')
                asagi_thumb_root = os.path.join(root_asagi, board, 'thumb')

                src_full = os.path.join(asagi_full_root, media_orig[:4], media_orig[4:6], media_orig)

                thumb_name = f'{name}s.jpg'
                src_thumb = os.path.join(asagi_thumb_root, thumb_name[:4], thumb_name[4:6], thumb_name)

                dst_full_name = f'{media_hash}.{ext}'
                dst_thumb_name = f'{media_hash}.jpg'

                dst_full = os.path.join(sutra_full_root, dst_full_name[:2], dst_full_name[2:4], dst_full_name[4:6], dst_full_name)
                dst_thumb = os.path.join(sutra_thumb_root, dst_thumb_name[:2], dst_thumb_name[2:4], dst_thumb_name[4:6], dst_thumb_name)

                d = os.path.dirname(dst_full)
                if d not in dir_cache:
                    os.makedirs(d, exist_ok=True)
                    dir_cache.add(d)

                d = os.path.dirname(dst_thumb)
                if d not in dir_cache:
                    os.makedirs(d, exist_ok=True)
                    dir_cache.add(d)

                if os.path.exists(src_full):
                    os.replace(src_full, dst_full)
                    moved += 1

                if os.path.exists(src_thumb):
                    os.replace(src_thumb, dst_thumb)

                if moved % 1000 == 0 and moved:
                    print(f'\rmoved {moved}/{total_in_scanner} ({100*moved//total_in_scanner}%)', end='', flush=True)

            processed += len(filenames)
            print(f'\rprocessed {processed}/{total_in_scanner} ({100*processed//total_in_scanner}%)', end='', flush=True)

        print(f'\rmoved {moved}/{total_in_scanner} ({100*moved//total_in_scanner}%)')

    finally:
        scanner_con.close()
        ritual_con.close()


if __name__ == '__main__':
    db_path_ritual = '/home/dolphin/Documents/code/ritual/ritual.db'
    db_path_scanner='/home/dolphin/Documents/code/ritual/scanner/scanner.db'
    root_asagi='/home/dolphin/Documents/code/ritual/media_asagi'
    root_sutra='/home/dolphin/Documents/code/ritual/media_sutra'
    
    print('Checking which boards exist in ritual database...')
    boards = get_valid_boards(db_path_ritual)
    
    if not boards:
        print('No valid boards found. Exiting.')
        exit(1)
    
    print(f'\nFound {len(boards)} valid boards: {", ".join(boards)}')
    if not confirm('Proceed with migration?'):
        print('Aborted.')
        exit(0)
    
    migrate(
        boards=boards,
        db_path_scanner=db_path_scanner,
        db_path_ritual=db_path_ritual,
        root_asagi=root_asagi,
        root_sutra=root_sutra,
    )
