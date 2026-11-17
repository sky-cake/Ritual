import argparse
import re
import sys

import configs
from filter import Filter
from utils import normalize_exts


class StubDb:
    def get_banned_media_hashes(self, board: str, media_hashes: list[str]) -> set[str]:
        return set()


def make_post(no: int, subject: str, comment: str) -> dict:
    return {
        'no': no,
        'sub': subject,
        'com': comment,
        'tim': str(no * 1000),
        'ext': '.jpg',
        'md5': f'h{no}',
    }


def archive_reason(board: str, subject: str, comment: str) -> str | None:
    board_cfg = configs.boards[board]

    min_chars = board_cfg.get('op_comment_min_chars')
    if min_chars and len(comment) < min_chars:
        return 'min_chars'

    min_unique = board_cfg.get('op_comment_min_chars_unique')
    if min_unique and len(set(comment)) < min_unique:
        return 'min_unique'

    blacklist = board_cfg.get('blacklist')
    if blacklist:
        if (subject and re.search(blacklist, subject, re.IGNORECASE)) or (
            comment and re.search(blacklist, comment, re.IGNORECASE)
        ):
            return 'blacklist'

    whitelist = board_cfg.get('whitelist')
    if whitelist:
        if not ((subject and re.search(whitelist, subject, re.IGNORECASE)) or (
            comment and re.search(whitelist, comment, re.IGNORECASE)
        )):
            return 'whitelist'

    return None


def full_value(pid: int, full_pids: set[int], skip: str | None) -> str:
    if skip:
        return skip
    return 'YES' if pid in full_pids else 'NO'


def thumb_value(pid: int, full_pids: set[int], thumb_pids: set[int], skip: str | None) -> str:
    if skip:
        return skip
    if pid in thumb_pids:
        return 'YES'
    if configs.make_thumbnails and pid in full_pids:
        return 'GEN'
    return 'NO'


def thread_value(key: str, board_cfg: dict, thread_post: dict, full_pids: set[int], filter_obj: Filter, skip: str | None) -> str:
    if skip:
        return skip
    if not filter_obj.is_media_needed_conf(thread_post, board_cfg.get(key)):
        return 'NO'
    if key == 'dl_th_thread' and configs.make_thumbnails and full_pids:
        return 'GEN'
    return 'YES'


def fmt(label: str, value: str) -> None:
    print(f'  {label:<20}{value}')


def print_report(board: str, board_cfg: dict, text_value: str, full_pids: set[int], thumb_pids: set[int], thread_post: dict, filter_obj: Filter, skip: str | None) -> None:
    print(f'Board: {board}')
    for key in (
        'whitelist', 'blacklist',
        'op_comment_min_chars', 'op_comment_min_chars_unique',
        'dl_fm_op', 'dl_fm_thread', 'dl_fm_post',
        'dl_th_op', 'dl_th_thread', 'dl_th_post',
    ):
        if key in board_cfg:
            print(f'  {key}: {board_cfg[key]}')
    print(f'  thread_text: {board_cfg.get("thread_text", True)}')
    print(f'  make_thumbnails: {configs.make_thumbnails}')
    print()

    fmt('thread text', text_value)
    fmt('full media OP', full_value(1, full_pids, skip))
    fmt('full media thread', thread_value('dl_fm_thread', board_cfg, thread_post, full_pids, filter_obj, skip))
    fmt('full media post', full_value(2, full_pids, skip))
    fmt('thumb OP', thumb_value(1, full_pids, thumb_pids, skip))
    fmt('thumb thread', thread_value('dl_th_thread', board_cfg, thread_post, full_pids, filter_obj, skip))
    fmt('thumb post', thumb_value(2, full_pids, thumb_pids, skip))


def text_value(board_cfg: dict, skip: str | None) -> str:
    if skip:
        return skip
    if board_cfg.get('thread_text', True) is False:
        return 'OFF (thread_text=False)'
    return 'YES'


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Validate configs for a board against thread text. No network, filesystem, or db usage.'
    )
    parser.add_argument('--board', required=True)
    parser.add_argument('--text', help='tested as both subject and comment')
    parser.add_argument('--subject', help='overrides --text for the subject')
    parser.add_argument('--comment', help='overrides --text for the comment')
    return parser


def main() -> None:
    args = get_parser().parse_args()

    if args.board not in configs.boards:
        print(f'board {args.board!r} is not configured')
        sys.exit(1)

    subject = args.subject if args.subject is not None else (args.text or '')
    comment = args.comment if args.comment is not None else (args.text or '')

    configs.full_media_exts = normalize_exts(configs.full_media_exts)
    configs.thumbnail_exts = normalize_exts(configs.thumbnail_exts)

    board_cfg = configs.boards[args.board]
    tid = 1
    thread_post = make_post(tid, subject, comment)
    reply_post = make_post(2, subject, comment)

    filter_obj = Filter(None, StubDb(), args.board, None)
    full_pids, thumb_pids = filter_obj.get_pids_for_download(
        {tid: [thread_post, reply_post]},
        {tid: thread_post},
    )

    skip = archive_reason(args.board, subject, comment)
    if skip:
        skip = f'SKIP ({skip})'

    print_report(
        args.board,
        board_cfg,
        text_value(board_cfg, skip),
        full_pids,
        thumb_pids,
        thread_post,
        filter_obj,
        skip,
    )


if __name__ == '__main__':
    main()
