import os
import base64
import hashlib


def iter_media_files(root_path: str, skip_dirnames: set[str] | None = None, valid_exts: set[str] | None = None):
    for dirpath, dirnames, filenames in os.walk(root_path):
        dirname = os.path.basename(dirpath)
        if skip_dirnames and dirname in skip_dirnames:
            dirnames[:] = []
            continue
        for filename in filenames:
            if '.' in filename:
                filename_no_ext, ext = filename.rsplit('.', maxsplit=1)
                if valid_exts and ext.lower() in valid_exts:
                    yield dirpath, filename_no_ext, ext


def get_md5_b64_hash(content: bytes) -> str:
    return base64.b64encode(hashlib.md5(content).digest(), altchars=b'-_').decode()


def main():
    src_root_path = '/mnt/dl'
    dst_root_path = '/mnt/sutra'

    skip_dirnames = {'thumb'}
    exts = {'jpeg', 'jpg', 'png', 'gif', 'webm', 'mp4'}

    count_moved_img = 0
    count_skip_img = 0
    count_moved_thb = 0
    count_skip_thb = 0
    count_not_found_thb = 0

    count_print = 0
    print_page_size = 5000

    dir_cache = set()

    for dirpath, filename_no_ext, ext in iter_media_files(src_root_path, skip_dirnames=skip_dirnames, valid_exts=exts):
        src_img = os.path.join(dirpath, f'{filename_no_ext}.{ext}')
        src_thb = os.path.join(dirpath.replace('image', 'thumb'), f'{filename_no_ext}s.jpg')

        with open(src_img, 'rb') as f:
            md5 = get_md5_b64_hash(f.read())

        dst_img = os.path.join(dst_root_path, 'img', md5[:2], md5[2:4], md5[4:6], f'{md5}.{ext}')
        dst_thb = os.path.join(dst_root_path, 'thb', md5[:2], md5[2:4], md5[4:6], f'{md5}.jpg')

        d = os.path.dirname(dst_img)
        if d not in dir_cache:
            os.makedirs(d, exist_ok=True)
            dir_cache.add(d)

        d = os.path.dirname(dst_thb)
        if d not in dir_cache:
            os.makedirs(d, exist_ok=True)
            dir_cache.add(d)

        if not os.path.isfile(dst_img):
            os.replace(src_img, dst_img)
            count_moved_img += 1
        else:
            count_skip_img += 1

        if os.path.isfile(src_thb):
            if not os.path.isfile(dst_thb):
                os.replace(src_thb, dst_thb)
                count_moved_thb += 1
            else:
                count_skip_thb += 1
        else:
            count_not_found_thb += 1

        count_print += 1
        if count_print >= print_page_size:
            print(f'({count_print}) {count_moved_img=} {count_skip_img=} {count_moved_thb=} {count_skip_thb=} {count_not_found_thb=}')
            count_print = 0

    print(f'{count_moved_img=} {count_skip_img=} {count_moved_thb=} {count_skip_thb=} {count_not_found_thb=}')


if __name__=='__main__':
    main()
