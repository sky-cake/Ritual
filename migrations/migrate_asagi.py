import sqlite3

from tqdm import tqdm

all_4chan_boards = {
    "a": "Anime & Manga",
    "b": "Random",
    "c": "Anime/Cute",
    "d": "Hentai/Alternative",
    "e": "Ecchi",
    "f": "Flash",
    "g": "Technology",
    "gif": "Adult GIF",
    "h": "Hentai",
    "hr": "High Resolution",
    "k": "Weapons",
    "m": "Mecha",
    "o": "Auto",
    "p": "Photo",
    "r": "Adult Requests",
    "s": "Sexy Beautiful Women",
    "t": "Torrents",
    "u": "Yuri",
    "v": "Video Games",
    "vg": "Video Game Generals",
    "vm": "Video Games/Multiplayer",
    "vmg": "Video Games/Mobile",
    "vr": "Retro Games",
    "vrpg": "Video Games/RPG",
    "vst": "Video Games/Strategy",
    "w": "Anime/Wallpapers",
    "wg": "Wallpapers/General",
    "i": "Oekaki",
    "ic": "Artwork/Critique",
    "r9k": "ROBOT9001",
    "s4s": "Shit 4chan Says",
    "vip": "Very Important Posts",
    "qa": "Question & Answer",
    "cm": "Cute/Male",
    "hm": "Handsome Men",
    "lgbt": "LGBT",
    "y": "Yaoi",
    "3": "3DCG",
    "aco": "Adult Cartoons",
    "adv": "Advice",
    "an": "Animals & Nature",
    "bant": "International/Random",
    "biz": "Business & Finance",
    "cgl": "Cosplay & EGL",
    "ck": "Food & Cooking",
    "co": "Comics & Cartoons",
    "diy": "Do It Yourself",
    "fa": "Fashion",
    "fit": "Fitness",
    "gd": "Graphic Design",
    "hc": "Hardcore",
    "his": "History & Humanities",
    "int": "International",
    "jp": "Otaku Culture",
    "lit": "Literature",
    "mlp": "Pony",
    "mu": "Music",
    "n": "Transportation",
    "news": "Current News",
    "out": "Outdoors",
    "po": "Papercraft & Origami",
    "pol": "Politically Incorrect",
    "pw": "Professional Wrestling",
    "qst": "Quests",
    "sci": "Science & Math",
    "soc": "Cams & Meetups",
    "sp": "Sports",
    "tg": "Traditional Games",
    "toy": "Toys",
    "trv": "Travel",
    "tv": "Television & Film",
    "vp": "Pokemon",
    "vt": "Virtual YouTubers",
    "wsg": "Worksafe GIF",
    "wsr": "Worksafe Requests",
    "x": "Paranormal",
    "xs": "Extreme Sports",
}

# set your paths!
# then run `python3 migrate_asagi.py`
db_path = '/home/USER/Documents/ritual/migrations/ritual_bk.db'
sql_file_path = '/home/USER/Documents/ritual/migrations/not_null_migration.sql'

def do_migration():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    boards = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
    boards = [b for b in boards if b in all_4chan_boards.keys()]
    input(f'Found {len(boards)} boards.\n\n{boards}.\n\nDo `CRTL-C` if NOT ok. Otherwise, hit enter.\n')
    for board in boards:

        print(f'Running sql script for: {board}.')

        with open(sql_file_path) as f:
            sql = f.read()

        sqls = sql.replace('%%BOARD%%', board).split(';')
        for sql in tqdm(sqls):
            conn.execute(sql)
        conn.commit()

    cursor.close()
    conn.close()

do_migration()
