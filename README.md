# Ritual

Ritual is a **very** simple 4chan archiver that complies to the Asagi schema.

It is built using Python3, and **SQLite as a database**, although MySQL may be supported in the future.

## What's special about this particular archiver?

- Ritual is **under 500 lines of code**. For reference, NeoFuuka has 2,600 lines of Python code (including whitespace). Meanwhile, Hayden has over 11,400 lines of C# code (NOT including nearly 3,000 lines of whitespace).

- The requests library is Ritual's only dependency.

- The second advantage of Ritual is its **amazingly flexible configurations**. Ritual allows you to choose whether you download text, thumbnails, and/or full media at the thread level - not the global level.

- Ritual is simple. It has no threading, no complex database, and its main loop is easly to read in under a minute.


## Getting Started

Ritual will create schemas for you.

1. Create a file called `configs.py` using `rename_to_configs.py`, and configure it.
2. Create a virtualenv and install dependencies,
    - `python3 -m venv venv`
    - `source venv/bin/activate`
    - `python3 -m pip install -r requirements.txt`
3. `screen -S ritual` (you might need to `sudo apt install screen`)
4. `python3 main.py` to run the scraper.
5. `ctrl-A`, `d` to leave the screen
6. `screen -r ritual` to reattach to the screen


## Migrations

Recently, there was an update to the code to make it fully asagi schema compliant. Please make a backup of your database, then run the script `./migrations/migrate_asagi.py`. After this, you can run `VACUUM;` on your database. The result of this will be a `.db` file the same size as before the migrations.


## Configurations

Here is how the flexible archive configurations work.

- If a post is blacklisted and whitelisted, it will not be archived - blacklisted filters take precedence over whitelisted filters.
- If only a blacklist is specified, skip blacklisted posts, and archive everything else.
- If only a whitelist is specified, archive whitelisted posts, and skip everything else.
- If no lists are specified, archive everything.
- If a thread is marked as "should archive" from the above rules, media downloads can be further filtered based on dl_thumbs, and db_full_media.
- To download all/no media, specify True/False. To filter media, assign a regex pattern.

Here is an example from `rename_to_configs.py`,

```python

boards = {
    'g': {
        'blacklist': '.*(local models).*', # if an OP contains "local models" in the subject or comment - skip thread
        'whitelist': '.*(home server|linux).*', # if not, then for OPs with "home server" or "linux" in the subject or comment...
        'dl_thumbs': '.*(home server general).*', # download thumbnails, but ONLY if it's a "home server general"
        'dl_full_media': '.*(wireguard).*', # if anyone mentions "wireguard", get the full media if applicable
        'thread_text': True, # archive the text if we pass the black/white lists.
    },
    'gif': {
        'dl_thumbs': False,
        'dl_full_media': False,
        'thread_text': True, # only gather thread text from /gif/ - no files
    },
    'ck': {
        'whitelist': '.*Coffee Time General.*', # only gather thread text, and thumbnails from "Coffee Time General" threads on /ck/
        'dl_thumbs': True,
        'dl_full_media': False,
        'thread_text': True,
    }
}
```
