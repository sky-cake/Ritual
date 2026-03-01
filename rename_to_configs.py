import os

from utils import make_path, setup_logger

request_cooldown_sec = 1.2
loop_cooldown_sec = 30.0
video_cooldown_sec = 3.2
image_cooldown_sec = 2.2
add_random = False # add random sleep intervals

ignore_thread_cache = True # on restarts, ignore thread cache and go through all threads
ignore_http_cache = False # always ignore http cache and go through all threads (used for testing)


# If the md5 from the API, and the md5 from the downloaded file do not match,
# download anyway (True) or do not download (False)
download_files_with_mismatched_md5 = False


# Duplicate files can be replace with hardlinks or softlinks with the tool https://github.com/pkolaczk/fclones
#   For ****example****,
#     1. mkdir fcc
#     2. fclones group ./media --cache ./fcc > dupes.txt
#     3. fclones link --priority oldest < dupes.txt


media_save_path = make_path('media')


db_type = 'sqlite' # 'sqlite' or 'mysql'
db_echo = False


# must have db_type = 'sqlite'
db_sqlite_path = make_path('ritual.db') # sqlite


# must have db_type = 'mysql'
db_mysql_host = 'localhost'
db_mysql_user = 'user'
db_mysql_password = 'password'
db_mysql_database = 'ritual'
db_mysql_port = 3306


headers = None
# headers = {'User-Agent', ''}


# 4chan API data should html unescaped before writes i.e. unescape_data_b4_db_write = True
# The goal here is to persist data that is not html escaped
unescape_data_b4_db_write = True


## 4chan
url_catalog = "https://a.4cdn.org/{board}/catalog.json"
url_thread = "https://a.4cdn.org/{board}/thread/{thread_id}.json"
url_archive = "https://a.4cdn.org/{board}/archive.json"
url_boards = "https://a.4cdn.org/boards.json"
url_full_media = "https://i.4cdn.org/{board}/{image_id}{ext}" # str or None
url_thumbnail = "https://i.4cdn.org/{board}/{image_id}s.jpg" # str or None


# A thread is marked as deleted if the thread is not in the /catalog or /archive
# and all three of these are true
not_deleted_if_bump_age_exceeds_n_min = 60 # thread got recent attention, so mods should see it
not_deleted_if_page_n_reached = 5          # thread is has reached a higher page, going ignored
not_deleted_if_n_replies = 30              # thread is popular, and no one deleted it

## lainchan
# url_full_media = "https://lainchan.org/{board}/src/{image_id}{ext}"
# url_thumbnail = None
# url_catalog = "https://lainchan.org/{board}/catalog.json"
# url_thread = "https://lainchan.org/{board}/res/{thread_id}.json"
# url_archive = "https://lainchan.org/{board}/archive.json"
# url_boards = "https://lainchan.org/boards.json"

logger_name = 'ritual'
log_file = False # or make_path("ritual.log") if you want to log to files
MB_5 = 5 * 1024 * 1024
logger = setup_logger(logger_name, log_file=log_file, stdout=True, file_rotate_size=MB_5, max_files=3)


make_thumbnails = False # don't download thumbnails, create them when downloading full media

# ARCHIVE RULES - What to archive.

# - `op_comment_min_chars` and `op_comment_min_chars_unique` filter everything first.
# - If a post is blacklisted and whitelisted, it will not be archived - blacklisted filters take precedence over whitelisted filters.
# - If only a blacklist is specified, skip blacklisted posts, and archive everything else.
# - If only a whitelist is specified, archive whitelisted posts, and skip everything else.
# - If no white/black lists are specified, archive everything.

# - If a thread is marked as "should archive" from the above rules, media downloads can be further filtered based on dl_th_*, and dl_fm_* configs.
#     - `dl`: download
#     - `th`: thumb
#     - `fm`: full_media

# - To download all/no media, specify True/False. To filter media, assign a regex pattern. Media can be filtered based on three levels.
#     - `op`: OP media
#     - `thread`: media in the whole thread
#     - `post`: media per post

boards = {
    'g': {
        'blacklist': '.*(local models).*', # If an OP contains "local models" in the subject or comment, then skip the thread.
        'whitelist': '.*(home server|linux).*', # otherwise, for OPs with "home server" or "linux" in the subject or comment, apply the other configs.

        'thread_text': True, # Archive the text? Blacklist and whitelist filters apply.

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
        'dl_fm_op': True,
        'dl_th_op': True,
    },
    'biz': {
        # Archive threads. No files.
        'op_comment_min_chars': 4, # Skips "omg" "." "lol"
        'op_comment_min_chars_unique': 3, # Skips "lol" "hahaha" "aaaaa"
    }
}


# Do not touch the configs below (unless you know what you're doing)
# Do not touch the configs below (unless you know what you're doing)
# Do not touch the configs below (unless you know what you're doing)
# Do not touch the configs below (unless you know what you're doing)
# Do not touch the configs below (unless you know what you're doing)

if not os.path.isdir(media_save_path):
    os.mkdir(media_save_path, mode=775)
    os.chmod(media_save_path, 0o775)
