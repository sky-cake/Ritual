import os

import tqdm

from utils import make_path, setup_logger

request_cooldown_sec = 1.2
loop_cooldown_sec = 30.0
video_cooldown_sec = 3.2
image_cooldown_sec = 2.2
add_random = False # add random sleep intervals

ignore_last_modified = True # on restarts, ignore last modified times and go through all threads

media_save_path = make_path('media')
database = make_path('ritual.db') # sqlite


# If new posts are added to the database and Ritual is interrupted before their media is downloaded,
# it's likely that some media will be missed when you restart Ritual, resulting in gaps of posts with no media.
# Setting this to True ensures that all media (according to the per-board download configs below) is fetched
# by verifying each post's media file on disk.
# This incurs a minimal CPU cost (os.path.isfile() is cheap), so the default is True.
ensure_all_files_downloaded = True


headers = None
# headers = {'User-Agent', ''}


# 4chan API data should html unescaped before writes i.e. unescape_data_b4_db_write = True
# The goal here is to persist data that is not html escaped
unescape_data_b4_db_write = True


## 4chan
url_catalog = "https://a.4cdn.org/{board}/catalog.json"
url_thread = "https://a.4cdn.org/{board}/thread/{thread_id}.json"
url_full_media = "https://i.4cdn.org/{board}/{image_id}{ext}" # str or None
url_thumbnail = "https://i.4cdn.org/{board}/{image_id}s.jpg" # str or None

## lainchan
# url_full_media = "https://lainchan.org/{board}/src/{image_id}{ext}"
# url_thumbnail = None
# url_catalog = "https://lainchan.org/{board}/catalog.json"
# url_thread = "https://lainchan.org/{board}/res/{thread_id}.json"


logger_name = 'ritual'
log_file = False # or make_path("ritual.log") if you want to log to files
MB_5 = 5 * 1024 * 1024
logger = setup_logger(logger_name, log_file=log_file, stdout=True, file_rotate_size=MB_5, max_files=3)

# logger.info = tqdm.tqdm.write # do this when using stdout logging and tqdm
disable_tqdm = True

make_thumbnails = False # don't download thumbnails, create them when downloading full media

# ARCHIVE RULES - What to archive.

# `op_comment_min_chars` and `op_comment_min_chars_unique` filter everything first.

# If a post is blacklisted and whitelisted, it will not be archived - blacklisted filters take precedence over whitelisted filters.
# If only a blacklist is specified, skip blacklisted posts, and archive everything else.
# If only a whitelist is specified, archive whitelisted posts, and skip everything else.
# If no lists are specified, archive everything.

# If a thread is marked as "should archive" from the above rules, media downloads can be further filtered based on dl_thumbs, and db_full_media.
# To download all/no media, specify True/False. To filter media, assign a regex pattern.
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
        'thread_text': True, # only gather thread text from /gif/ - no files.
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
    },
    'biz': {
        'thread_text': True,
        'op_comment_min_chars': 4, # OP comment must be at least 10 characters long (does not archive: "omg", ".", "lol", etc.)
        'op_comment_min_chars_unique': 3, # OP comment must have 5 unique character (does not archive: ".", "lol", "hahaha", "aaaaa", etc.)
    },
}


# Do not touch the configs below (unless you know what you're doing)
# Do not touch the configs below (unless you know what you're doing)
# Do not touch the configs below (unless you know what you're doing)
# Do not touch the configs below (unless you know what you're doing)
# Do not touch the configs below (unless you know what you're doing)

if not os.path.isdir(media_save_path):
    os.mkdir(media_save_path, mode=775)
    os.chmod(media_save_path, 0o775)
