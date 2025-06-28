import json
import logging
import os
import random
import subprocess
import time
from collections import OrderedDict
from logging.handlers import RotatingFileHandler

from requests import Session, get as requests_get


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


def write_json(fpath, obj):
    os.makedirs(os.path.dirname(fpath), exist_ok=True)
    with open(fpath, mode='w', encoding='utf-8') as f:
        json.dump(obj, f)


def read_json(fpath):
    if not os.path.isfile(fpath):
        return None

    with open(fpath, mode='r', encoding='utf-8') as f:
        return json.load(f)


def sleep(t: int, add_random: bool=False):
    if add_random:
        t += random.uniform(0.0, 1.0)

    time.sleep(t)


def log_warning(logger: logging.Logger, message: str):
    if logger:
        logger.warning(message)
    print(f'Warning: {message}')


def fetch_json(url, headers=None, request_cooldown_sec: float=None, add_random: bool=False, session: Session=None) -> dict:
    try:
        resp = session.get(url, headers=headers) if session else requests_get(url, headers=headers)
        if request_cooldown_sec:
            sleep(request_cooldown_sec, add_random=add_random)

        if resp.status_code == 200:
            return resp.json()
    except Exception:
        sleep(7.5)


def download_file(url: str, filepath: str, video_cooldown_sec: float=3.2, image_cooldown_sec: float=2.2, add_random: bool=False, headers: dict=None, logger: logging.Logger=None, session: Session=None):
    if is_video_path(filepath):
        ts = [video_cooldown_sec, 4.0, 6.0, 10.0]
    elif is_image_path(filepath):
        ts = [image_cooldown_sec, 4.0, 6.0, 10.0]
    else:
        raise ValueError(filepath)

    for i, t in enumerate(ts):
        resp = session.get(url, headers=headers) if session else requests_get(url, headers=headers)
        if i > 0:
            video_cooldown_sec += 1.0
            image_cooldown_sec += 1.0
            log_warning(logger, f'Incremented 1 second to cooldowns, new cooldowns are: {video_cooldown_sec=} {image_cooldown_sec=}')

        sleep(t, add_random=add_random)

        if resp.status_code != 200:
            log_warning(logger, f'{url=} {resp.status_code=}')
            return

        if resp.status_code == 200 and resp.content:
            with open(filepath, 'wb') as f:
                f.write(resp.content)
            return True
        
        log_warning(logger, f'No content {url=} {filepath=} {resp.content=}')

    log_warning(logger, f'Max retries exceeded {url=} {filepath=}')


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
