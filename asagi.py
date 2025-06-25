import html
import json
import os
import re

from enums import MediaType
from utils import (
    create_thumbnail_from_image,
    create_thumbnail_from_video,
    make_path
)


def is_post_media_file_video(post):
    return post.get('ext', '').endswith(('webm', 'mp4', 'gif'))


def is_post_media_file_image(post):
    return post.get('ext', '').endswith(('jpg', 'png', 'jpeg'))


def convert_to_asagi_capcode(a):
    if a:
        if a == "mod": return "M"
        if a == "admin": return "A"
        if a == "admin_highlight": return "A"
        if a == "developer": return "D"
        if a == "verified": return "V"
        if a == "founder": return "F"
        if a == "manager": return "G"

        return "M"

    return "N"


def convert_to_asagi_comment(a):
    if not a:
        return a

    # literal tags
    if "[" in a:
        a = re.sub(
            "\\[(/?(spoiler|code|math|eqn|sub|sup|b|i|o|s|u|banned|info|fortune|shiftjis|sjis|qstcolor))\\]",
            "[\\1:lit]",
            a
        )
    
    # abbr, exif, oekaki
    if "\"abbr" in a: a = re.sub("((<br>){0-2})?<span class=\"abbr\">(.*?)</span>", "", a)
    if "\"exif" in a: a = re.sub("((<br>)+)?<table class=\"exif\"(.*?)</table>", "", a)
    if ">Oek" in a: a = re.sub("((<br>)+)?<small><b>Oekaki(.*?)</small>", "", a)
    
    # banned
    if "<stro" in a:
        a = re.sub("<strong style=\"color: ?red;?\">(.*?)</strong>", "[banned]\\1[/banned]", a)
    
    # fortune
    if "\"fortu" in a:
        a = re.sub(
            "<span class=\"fortune\" style=\"color:(.+?)\"><br><br><b>(.*?)</b></span>",
            "\n\n[fortune color=\"\\1\"]\\2[/fortune]",
            a
        )
    
    # dice roll
    if "<b>" in a:
        a = re.sub(
            "<b>(Roll(.*?))</b>",
            "[b]\\1[/b]",
            a
        )
    
    # code tags
    if "<pre" in a:
        a = re.sub("<pre[^>]*>", "[code]", a)
        a = a.replace("</pre>", "[/code]")
    
    # math tags
    if "\"math" in a:
        a = re.sub("<span class=\"math\">(.*?)</span>", "[math]\\1[/math]", a)
        a = re.sub("<div class=\"math\">(.*?)</div>", "[eqn]\\1[/eqn]", a)
    
    # sjis tags
    if "\"sjis" in a:
        a = re.sub("<span class=\"sjis\">(.*?)</span>", "[shiftjis]\\1[/shiftjis]", a) # use [sjis] maybe?
    
    # quotes & deadlinks
    if "<span" in a:
        a = re.sub("<span class=\"quote\">(.*?)</span>", "\\1", a)
        
        # hacky fix for deadlinks inside quotes
        for idx in range(3):
            if not "deadli" in a: break
            a = re.sub("<span class=\"(?:[^\"]*)?deadlink\">(.*?)</span>", "\\1", a)
    
    # other links
    if "<a" in a:
        a = re.sub("<a(?:[^>]*)>(.*?)</a>", "\\1", a)
    
    # spoilers
    a = a.replace("<s>", "[spoiler]")
    a = a.replace("</s>", "[/spoiler]")
    
    # newlines
    a = a.replace("<br>", "\n")
    a = a.replace("<wbr>", "")
    
    a = html.unescape(a)
    
    return a


def post_has_file(post: dict) -> bool:
    return post.get('tim') and post.get('ext') and post.get('md5')


def get_fs_filename_full_media(post: dict) -> str:
    if post_has_file(post):
        return f"{post.get('tim')}{post.get('ext')}"


def get_fs_filename_thumbnail(post: dict) -> str:
    if post_has_file(post):
        return f"{post.get('tim')}s.jpg"


def post_is_sticky(post: dict) -> bool:
    return post.get('sticky') == 1


def create_thumbnail(post: dict, full_path: str, thumb_path: str, logger=None):
    if not post_has_file(post):
        return

    if is_post_media_file_video(post):
        create_thumbnail_from_video(full_path, thumb_path, logger=logger)
        return
    
    if is_post_media_file_image(post):
        create_thumbnail_from_image(full_path, thumb_path, logger=logger)
        return

def get_filepath(media_save_path: str, board: str, media_type: MediaType, filename: str) -> str:
    tim = filename.split('.')[0]
    assert len(tim) >= 6 and tim[:6].isdigit()
    dir_path = make_path(media_save_path, board, media_type, filename[:4], filename[4:6])
    os.makedirs(dir_path, mode=775, exist_ok=True)
    os.chmod(dir_path, 0o775)
    return os.path.join(dir_path, filename)


def get_thread_id_2_last_replies(catalog):
    thread_id_2_last_replies = {}
    for page in catalog:
        for thread in page['threads']:
            if thread.get('last_replies'):
                thread_id_2_last_replies[thread['no']] = thread.get('last_replies')
    return thread_id_2_last_replies


def get_d_board(post: dict, media_id: int = None, unescape_data_b4_db_write: bool=True):
    return {
        # 'doc_id': post.get('doc_id'), # autoincremented
        'media_id': media_id or 0,
        'poster_ip': post.get('poster_ip', '0'),
        'num': post.get('no', 0),
        'subnum': post.get('subnum', 0),
        'thread_num': post.get('no') if post.get('resto') == 0 else post.get('resto'),
        'op': 1 if post.get('resto') == 0 else 0,
        'timestamp': post.get('time', 0),
        'timestamp_expired': post.get('archived_on', 0),
        'preview_orig': get_fs_filename_thumbnail(post),
        'preview_w': post.get('tn_w', 0),
        'preview_h': post.get('tn_h', 0),
        'media_filename': html.unescape(f"{post.get('filename')}{post.get('ext')}") if post.get('filename') and post.get('ext') and unescape_data_b4_db_write else None,
        'media_w': post.get('w', 0),
        'media_h': post.get('h', 0),
        'media_size': post.get('fsize', 0),
        'media_hash': post.get('md5'),
        'media_orig': get_fs_filename_full_media(post),
        'spoiler': post.get('spoiler', 0),
        'deleted': post.get('filedeleted', 0),
        'capcode': convert_to_asagi_capcode(post.get('capcode')),
        'email': post.get('email'),
        'name': html.unescape(post.get('name')) if post.get('name') and unescape_data_b4_db_write else None,
        'trip': post.get('trip'),
        'title': html.unescape(post.get('sub')) if post.get('sub') and unescape_data_b4_db_write else None,
        'comment': convert_to_asagi_comment(post.get('com')) if unescape_data_b4_db_write else post.get('com'),
        'delpass': post.get('delpass'),
        'sticky': post.get('sticky', 0),
        'locked': post.get('closed', 0),
        'poster_hash': post.get('id'),
        'poster_country': post.get('country_name'),
        'exif': json.dumps({'uniqueIps': int(post.get('unique_ips'))}) if post.get('unique_ips') else None,
    }


def get_d_image(post: dict, is_op: bool):
    return {
        # 'media_id': post.get('media_id'), # autoincremented
        'media_hash': post.get('md5'),
        'media': get_fs_filename_full_media(post),
        'preview_op': get_fs_filename_thumbnail(post) if is_op else None,
        'preview_reply': get_fs_filename_thumbnail(post) if not is_op else None,
        'total': 0,
        'banned': 0,
    }
