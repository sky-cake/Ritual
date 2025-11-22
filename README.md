# Ritual

Ritual is a 4chan archiver that focuses on simplicity.

Notable features include,

- Built using Python 3.12.
- Uses the Asagi schema
- Runs in a synchronous, step-by-step manner that's easy read.
- `requests` and `pydantic` are the only dependencies.
- Flexible configurations. You can choose whether you download text, thumbnails, and/or full media for each post.
- Sqlite database.
- Avoids downloading duplicate media files.

## Getting Started

Ritual will create schemas for you. But note, in the future, when you need database tools, check out https://github.com/sky-cake/asagi-tables.

1. Create a file called `configs.py` using `rename_to_configs.py`, and configure it.
1. Create a venv and install dependencies,
    - `python3.12 -m venv venv`
    - `source venv/bin/activate`
    - `python3.12 -m pip install -r requirements.txt`
1. `python3.12 main.py` to run the scraper.

If you want the program to persist after leaving your shell, you can run Ritual using `screen`, likeso.

1. `screen -S ritual` (you might need to `sudo apt install screen`)
1. `python3.12 main.py` to run the scraper.
1. `ctrl-A`, `d` to leave the screen
1. `screen -r ritual` to reattach to the screen


## Known Issues

- `<board>_images.total` is not accurate. This arises from supporting partial media downloading.


## Backups

```
sqlite3 /path/to/db "VACUUM INTO '/path/to/backup'"
sqlite3 /path/to/backup 'PRAGMA integrity_check' # optional
gzip /path/to/backup # optional
```

## Configurations

Here is how the flexible archive configurations work.

- `op_comment_min_chars` and `op_comment_min_chars_unique` filter everything first.
- If a post is blacklisted and whitelisted, it will not be archived - blacklisted filters take precedence over whitelisted filters.
- If only a blacklist is specified, skip blacklisted posts, and archive everything else.
- If only a whitelist is specified, archive whitelisted posts, and skip everything else.
- If no white/black lists are specified, archive everything.
- If a thread is marked as "should archive" from the above rules, media downloads can be further filtered based on dl_thumbs, and db_full_media.
- To download all/no media, specify True/False. To filter media, assign a regex pattern.

Here is an example from `rename_to_configs.py`,

```python

boards = {
    'g': {
        'blacklist': '.*(local models).*', # if an OP contains "local models" in the subject or comment - skip thread
        'whitelist': '.*(home server|linux).*', # if not, then for OPs with "home server" or "linux" in the subject or comment...
        'dl_thumbs': '.*(home server general).*', # download thumbnails, but ONLY if it's a "home server general"
        'dl_full_media': '.*(wireguard).*', # if any replies mention "wireguard", get its full media, if applicable
        'dl_full_media_op': '.*(cloud[0-9]).*', # if an OP mentions "cloud[0-9]", get its full media, if applicable
        'thread_text': True, # archive the text if we pass the black/white lists.
    },
    'gif': {
        'thread_text': True, # only gather thread text from /gif/ - no files
    },
    'ck': {
        'whitelist': '.*Coffee Time General.*', # only gather thread text, and thumbnails from "Coffee Time General" threads on /ck/
        'dl_thumbs': True,
        'dl_full_media': False,
        'thread_text': True,
    },
    't': {
        'dl_full_media_op': True, # download all thread text, but only thumbnails and full media for the OP posts on /t/
        'dl_thumbs_op': True,
        'thread_text': True,
    }
    'biz': {
        'thread_text': True,
        'op_comment_min_chars': 4, # OP comment must be at least 10 characters long (does not archive: "omg", ".", "lol", etc.)
        'op_comment_min_chars_unique': 3, # OP comment must have 5 unique character (does not archive: ".", "lol", "hahaha", "aaaaa", etc.)
    }
}
```
