import os

from utils import make_path, setup_logger

debug = False

request_cooldown_sec = 1.0 # advised by the 4chan API docs
catalog_cooldown_sec = 30.0 # [0, inf]

media_save_path = make_path('media')

database = make_path('ritual.db') # sqlite

user_agent = ''

logger_name = 'ritual'
log_file = make_path("ritual.log") # or False if you don't want log files
MB_5 = 5 * 1024 * 1024
logger = setup_logger(logger_name, log_file=log_file, stdout=True, file_rotate_size=MB_5, max_files=3)


# ARCHIVE RULES - What to archive.

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
