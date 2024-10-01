import html
import json
import logging
import os
import re
from logging.handlers import RotatingFileHandler


def make_path(*filepaths):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *filepaths)

def setup_logger(logger_name, log_file=False, stdout=True, file_rotate_size=5 * 1024 * 1024, max_files=3, log_level=logging.INFO):
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    if stdout:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if log_file:
        file_handler = RotatingFileHandler(log_file, maxBytes=file_rotate_size, backupCount=max_files)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def save_json(name, o):
    with open(make_path('json', f'{name}.json'), mode='w', encoding='utf-8') as f:
        json.dump(o, f)


def read_json(name):
    with open(make_path('json', f'{name}.json'), mode='r', encoding='utf-8') as f:
        return json.load(f)


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