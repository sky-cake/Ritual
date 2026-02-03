# Ritual

Ritual is a 4chan archiver that focuses on simplicity.

Notable features include,

- Built using Python 3.12.
- Uses the Asagi schema
- Runs in a synchronous, step-by-step manner that's easy read.
- Minimal dependencies.
- Flexible configurations. You can choose whether you download text, thumbnails, and/or full media for each post.
- Sqlite and MySQL database support.
- Avoids downloading duplicate media files.

## Getting Started

Ritual will create schemas for you. But note, in the future, when you need database tools, check out https://github.com/sky-cake/asagi-tables.

1. Create a file called `configs.py` using `rename_to_configs.py`, and configure it.
1. Create a venv and install dependencies,
    - `uv venv`
    - `source .venv/bin/activate`
    - `uv pip install -r requirements.txt`
    - Ritual depends on https://github.com/sky-cake/asagi-tables for Sqlite and MySQL schema creation.
        - Please consult its documentation for installation and set up - it's very simple. Follow install option 2.
        - Ritual will run the following asagi-table commands,
            - `asagi base table add [board(s)]`
            - `asagi base index add [board(s)]`
            - `asagi side table add [board(s)]`
            - `asagi side index add [board(s)]`
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

- If a thread is marked as "should archive" from the above rules, media downloads can be further filtered based on dl_th_*, and dl_fm_* configs.
    - `dl`: download
    - `th`: thumb
    - `fm`: full_media

- To download all/no media, specify True/False. To filter media, assign a regex pattern. Media can be filtered based on three levels.
    - `op`: OP media
    - `thread`: media in the whole thread
    - `post`: media per post

Here is an example from `rename_to_configs.py`,

```python
boards = {
    'g': {
        'blacklist': '.*(local models).*', # If an OP contains "local models" in the subject or comment, then skip the thread.
        'whitelist': '.*(home server|linux).*', # otherwise, for OPs with "home server" or "linux" in the subject or comment, apply the other configs.

        'thread_text': True, # Archive the text? Blacklist and whitelist filters apply. Disable by setting {'thread_text': False}.

        'dl_fm_thread': '.*(wireguard).*', # if a thread/OP mentions "wireguard", get its all the full media for the thread
        'dl_fm_post': '.*(wireguard).*', # if a replies mentions "wireguard", get its the post's full media
        'dl_fm_op': '.*(wireguard).*', # if an OP mentions "wireguard", get downloads the OP's full media
        
        # Thumbnail downloads work the same way, but they are specified with dl_th_* instead of dl_fm_*
    },
    'gif': {
        # This will only gather thread text from /gif/. No files.
        # By default, we assume {'thread_text': True}
    },
    'ck': {
        # Archive "Coffee Time General" threads with thumbnails only.
        'whitelist': '.*Coffee Time General.*',
        'thread_text': True,
        'dl_th_post': True,
        'dl_fm_post': False,
    },
    't': {
        # Archive threads. Only download OP full media and OP thumbnails.
        'thread_text': True,
        'dl_fm_op': True,
        'dl_th_op': True,
    },
    'biz': {
        # Archive threads. No files.
        'thread_text': True,
        'op_comment_min_chars': 4, # Skips "omg" "." "lol"
        'op_comment_min_chars_unique': 3, # Skips "lol" "hahaha" "aaaaa"
    }
}
```
