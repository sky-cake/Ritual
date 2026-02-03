import base64
import hashlib
import html
import html.parser
import json
import logging
import os
import random
import re
import secrets
import string
import subprocess
import time
from collections import OrderedDict
from logging.handlers import RotatingFileHandler
from typing import Annotated, Literal

from pydantic import BaseModel, Field
from requests import Session
from requests import get as requests_get

from enums import MediaType


def is_post_media_file_video(post):
    return post.get('ext', '').endswith(('webm', 'mp4', 'gif'))


def is_post_media_file_image(post):
    return post.get('ext', '').endswith(('jpg', 'png', 'jpeg', 'webp', 'bmp'))


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
    a = a.replace("<br/>", "\n")
    a = a.replace("<wbr>", "")

    a = html.unescape(a)

    return a


def post_has_file(post: dict) -> bool:
    return post.get('tim') and post.get('ext')


def get_fs_filename_full_media(post: dict) -> str:
    if post_has_file(post):
        return f"{post.get('tim')}{post.get('ext')}"


def get_fs_filename_thumbnail(post: dict) -> str:
    if post_has_file(post):
        return f"{post.get('tim')}s.jpg"


def create_thumbnail(post: dict, full_path: str, thumb_path: str, logger=None):
    if not post_has_file(post):
        return
    
    if not os.path.isfile(full_path):
        return

    if is_post_media_file_video(post):
        create_thumbnail_from_video(full_path, thumb_path, logger=logger)
        return

    if is_post_media_file_image(post):
        create_thumbnail_from_image(full_path, thumb_path, logger=logger)
        return


digits = '0123456789'
def get_filepath(media_save_path: str, board: str, media_type: MediaType, filename: str) -> str:
    """Will create filepath directories if they don't exist."""
    tim = filename.rsplit('.', maxsplit=1)[0]
    assert len(tim) >= 6 and all(t in digits for t in tim[:6])
    dir_path = make_path(media_save_path, board, media_type.value, filename[:4], filename[4:6])
    os.makedirs(dir_path, mode=775, exist_ok=True)
    os.chmod(dir_path, 0o775)
    return os.path.join(dir_path, filename)



def get_d_board(post: dict, media_id: int | None = None, unescape_data_b4_db_write: bool=True):
    return {
        # 'doc_id': post.get('doc_id'), # autoincremented
        'media_id': media_id or 0, # inserted/updated by triggers
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


def get_thread_id_2_last_replies(catalog):
    thread_id_2_last_replies = {}
    for page in catalog:
        for thread in page['threads']:
            if thread.get('last_replies'):
                thread_id_2_last_replies[thread['no']] = thread.get('last_replies')
    return thread_id_2_last_replies


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


PositiveInt = Annotated[int, Field(gt=0)]
NonNegativeInt = Annotated[int, Field(ge=0)]
ZeroOrOne = Annotated[int, Field(ge=0, le=1)]

ExtLiteral = Literal['.jpg', '.png', '.gif', '.pdf', '.swf', '.webm', '.mp4']
CapcodeLiteral = Literal['mod', 'admin', 'admin_highlight', 'manager', 'developer', 'founder']

ShortStr = Annotated[str, Field(min_length=0, max_length=512)]
LongStr = Annotated[str, Field(min_length=0, max_length=16_384)]


class BasePost(BaseModel):
    no: PositiveInt
    resto: NonNegativeInt
    sticky: ZeroOrOne | None = None
    closed: ZeroOrOne | None = None
    now: ShortStr
    time: PositiveInt
    name: ShortStr | None = None
    trip: ShortStr | None = None
    id: Annotated[str, Field(max_length=32)] | None = None
    capcode: CapcodeLiteral | None = None
    country: Annotated[str, Field(min_length=2, max_length=2)] | None = None
    country_name: ShortStr | None = None
    sub: ShortStr | None = None
    com: LongStr | None = None
    tim: PositiveInt | None = None
    filename: ShortStr | None = None
    ext: ExtLiteral | None = None
    fsize: PositiveInt | None = None
    md5: Annotated[str, Field(min_length=24, max_length=24)] | None = None
    w: PositiveInt | None = None
    h: PositiveInt | None = None
    tn_w: PositiveInt | None = None
    tn_h: PositiveInt | None = None
    filedeleted: ZeroOrOne | None = None
    spoiler: ZeroOrOne | None = None
    custom_spoiler: Annotated[int, Field(ge=1, le=10)] | None = None
    m_img: ZeroOrOne | None = None

    replies: NonNegativeInt | None = None
    images: NonNegativeInt | None = None
    bumplimit: ZeroOrOne | None = None
    imagelimit: ZeroOrOne | None = None
    tag: ShortStr | None = None
    semantic_url: ShortStr | None = None
    since4pass: Annotated[int, Field(ge=2000, le=2099)] | None = None
    unique_ips: PositiveInt | None = None


class ChanPost(BasePost):
    '''https://github.com/4chan/4chan-API/blob/master/pages/Threads.md'''
    board_flag: ShortStr | None = None
    flag_name: ShortStr | None = None
    archived: ZeroOrOne | None = None
    archived_on: PositiveInt | None = None


class ChanThread(BasePost):
    '''https://github.com/4chan/4chan-API/blob/master/pages/Catalog.md'''
    last_modified: PositiveInt | None = None
    omitted_posts: NonNegativeInt | None = None
    omitted_images: NonNegativeInt | None = None
    last_replies: list[ChanPost] | None = None


def test_deps(logger: logging.Logger):
    ffmpeg_path = subprocess.run(['which', 'ffmpeg'], capture_output=True, text=True).stdout.strip()
    convert_path = subprocess.run(['which', 'convert'], capture_output=True, text=True).stdout.strip()
    logger.info(f'FFmpeg Path: {ffmpeg_path}')
    logger.info(f'Convert Path: {convert_path}')


def make_path(*filepaths):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *filepaths)


def setup_logger(logger_name, log_file=False, stdout=True, file_rotate_size=1 * 1024 * 1024, max_files=3, log_level=logging.INFO):
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    formatter = logging.Formatter('%(message)s')

    if stdout:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if log_file:
        file_handler = RotatingFileHandler(log_file, maxBytes=file_rotate_size, backupCount=max_files)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def write_json_obj_to_file(filepath: str, obj):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, mode='w', encoding='utf-8') as f:
        json.dump(obj, f)


def read_json(fpath) -> dict:
    if not os.path.isfile(fpath):
        return None

    with open(fpath, mode='r', encoding='utf-8') as f:
        return json.load(f)


def sleep(t: int, add_random: bool=False):
    if add_random:
        t += random.uniform(0.0, 1.0)

    time.sleep(t)


def log_util(logger: logging.Logger, message: str):
    if logger:
        logger.warning(message)
    else:
        print(message)


class TextExtractor(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = []

    def handle_data(self, data: str):
        self.text.append(data)

    def get_text(self) -> str:
        return ' '.join(self.text)


def extract_text_from_html(html_str: str) -> str:
    if not html_str:
        return ''
    parser = TextExtractor()
    parser.feed(html_str)
    return html.unescape(parser.get_text())


def fullmatch_sub_and_com(post: dict, pattern: str) -> bool:
    """Compares a post's raw api data to patterns."""
    sub = post.get('sub')
    com = post.get('com')

    if sub:
        sub_text = html.unescape(sub)
        if re.fullmatch(pattern, sub_text, re.IGNORECASE):
            return True

    if com:
        com_text = extract_text_from_html(com)
        if re.fullmatch(pattern, com_text, re.IGNORECASE):
            return True

    return False


def get_n_random_chars(n: int) -> str:
    return ''.join(secrets.choice(string.ascii_letters) for _ in range(n))


def get_random_querystring() -> str:
    return f'{get_n_random_chars(5)}={get_n_random_chars(5)}'


def get_url_and_filename(configs, board: str, post: dict, media_type: MediaType):
    if media_type == MediaType.thumbnail:
        url = configs.url_thumbnail.format(board=board, image_id=post['tim']) # ext is always .jpg
        filename = get_fs_filename_thumbnail(post)

    elif media_type == MediaType.full_media:
        url = configs.url_full_media.format(board=board, image_id=post['tim'], ext=post['ext'])
        filename = get_fs_filename_full_media(post)

    else:
        raise ValueError(media_type)

    # avoid cloudflare's recompressed media
    url = f'{url}?{get_random_querystring()}'
    return url, filename


def get_filename(post: dict, media_type: MediaType):
    if media_type == MediaType.thumbnail:
        filename = get_fs_filename_thumbnail(post)

    elif media_type == MediaType.full_media:
        filename = get_fs_filename_full_media(post)

    else:
        raise ValueError(media_type)

    return filename


def get_md5_hash_bytes(content: bytes) -> str:
    hash_obj = hashlib.md5()
    hash_obj.update(content)
    return base64.b64encode(hash_obj.digest()).decode('ascii')


def download_file(
    url: str,
    filepath: str,
    video_cooldown_sec: float=3.2,
    image_cooldown_sec: float=2.2,
    headers: dict | None=None,
    logger: logging.Logger | None=None,
    session: Session | None=None,
    expected_size: int | None=None,
    expected_md5: str | None=None,
    download_files_with_mismatched_md5: bool=False,
) -> bool:
    ts = video_cooldown_sec if is_video_path(filepath) else image_cooldown_sec if is_image_path(filepath) else 2.0

    resp = session.get(url, headers=headers) if session else requests_get(url, headers=headers)

    if resp.status_code != 200:
        log_util(logger, f'{url=} {resp.status_code=}')
        return False

    if not resp.content:
        return False

    content = resp.content

    if len(content) > expected_size:
        log_util(logger, f'File size large than expected {url=} {filepath=} expected={expected_size} got={len(content)}. Skipping.')
        return False

    if expected_md5:
        file_hash = get_md5_hash_bytes(content)
        if file_hash != expected_md5:
            if download_files_with_mismatched_md5:
                log_util(logger, f'Hashes differ: {url=} {filepath=} told={expected_md5} found={file_hash}')
            return download_files_with_mismatched_md5

    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'wb') as f:
        f.write(content)
    
    sleep(ts)
    return True


def is_video_path(path: str) -> bool:
    return path.endswith(('webm', 'mp4', 'gif'))


def is_image_path(path: str) -> bool:
    return path.endswith(('jpg', 'jpeg', 'png', 'webp', 'bmp'))


class MaxQueue:
    def __init__(self, boards, max_items_per_board=150*151*2):
        """
        threads_per_catalog = 150
        images_per_thread = 151
        """

        # use OrderedDict for lookup speed, rather than a list
        self.items = {b: OrderedDict() for b in boards}

        self.max_items_per_board = max_items_per_board

    def add(self, board: str, filepath: str):
        board_items = self.items[board]

        if filepath in board_items:
            return

        while len(board_items) >= self.max_items_per_board:
            board_items.popitem(last=False)

        board_items[filepath] = 1

    def __contains__(self, filepath: str) -> bool:
        return any(filepath in board_items for board_items in self.items.values())

    def __getitem__(self, board: str) -> OrderedDict:
        return self.items[board]


def create_thumbnail_from_video(video_path: str, out_path: str, width: int=400, height: int=400, quality: int=25, logger=None):
    """width and height form the max box boundary for the resulting image"""

    if not is_video_path(video_path):
        raise ValueError(video_path)

    command = f"""ffmpeg -hide_banner -loglevel error -ss 0 -i "{video_path}" -pix_fmt yuvj420p -q:v 2 -frames:v 1 -f image2pipe - | convert - -resize {width}x{height} -quality {quality} "{out_path}" """

    try:
        subprocess.run(command, shell=True, check=True, stdout=subprocess.DEVNULL)
        if logger:
            logger.info(f'    Created thumb {os.path.getsize(video_path) / 1024:.1f}kb -> {os.path.getsize(out_path) / 1024:.1f}kb')
    except Exception as e:
        if logger:
            logger.error(f'Error creating thumbnail from {video_path}\n{str(e)}')


def create_thumbnail_from_image(image_path: str, out_path: str, width: int=400, height: int=400, quality: int=25, logger=None):
    """width and height form the max box boundary for the resulting image"""

    if not is_image_path(image_path):
        raise ValueError(image_path)

    command = f"""convert "{image_path}" -resize {width}x{height} -quality {quality} "{out_path}" """

    try:
        subprocess.run(command, shell=True, check=True, stdout=subprocess.DEVNULL)
        if logger:
            logger.info(f'    Created thumb {os.path.getsize(image_path) / 1024:.1f}kb -> {os.path.getsize(out_path) / 1024:.1f}kb')
    except Exception as e:
        if logger:
            logger.error(f'    Error creating thumbnail from {image_path}\n{str(e)}')
