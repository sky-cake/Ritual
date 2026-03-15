import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from functools import lru_cache

IMAGE_EXTS = {'jpg', 'jpeg', 'png', 'gif'}
VIDEO_EXTS = {'mp4', 'webm'}
MEDIA_EXTS = IMAGE_EXTS | VIDEO_EXTS

PRINT_PAGE_SIZE = 5000
MAX_WORKERS = 3
MAX_PENDING_FUTURES = MAX_WORKERS * 3


def iter_media_files(root_path: str, skip_dirnames: set[str] | None = None, valid_exts: set[str] | None = None):
    valid_exts = valid_exts or MEDIA_EXTS
    for dirpath, dirnames, filenames in os.walk(root_path):
        if skip_dirnames:
            dirnames[:] = [d for d in dirnames if d not in skip_dirnames]
        for filename in filenames:
            if '.' not in filename:
                continue
            base, ext = filename.rsplit('.', 1)
            if ext.lower() in valid_exts:
                yield dirpath, base, ext


@lru_cache(maxsize=64)
def is_video_ext(ext: str) -> bool:
    return ext in VIDEO_EXTS


@lru_cache(maxsize=64)
def is_image_ext(ext: str) -> bool:
    return ext in IMAGE_EXTS


def create_thumbnail_from_video(video_path: str, out_path: str, width: int = 400, height: int = 400, quality: int = 25):
    cmd = f'ffmpeg -hide_banner -loglevel error -ss 0 -i "{video_path}" -pix_fmt yuvj420p -q:v 2 -frames:v 1 -f image2pipe - | convert - -resize {width}x{height} -quality {quality} "{out_path}"'
    subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def create_thumbnail_from_image(image_path: str, out_path: str, width: int = 400, height: int = 400, quality: int = 25):
    cmd = f'convert "{image_path}" -resize {width}x{height} -quality {quality} "{out_path}"'
    subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def process_file(img_path: str, thb_path: str, ext: str) -> int:
    try:
        if os.path.isfile(thb_path):
            return 0
        os.makedirs(os.path.dirname(thb_path), exist_ok=True)
        if is_video_ext(ext):
            create_thumbnail_from_video(img_path, thb_path)
        elif is_image_ext(ext):
            create_thumbnail_from_image(img_path, thb_path)
        else:
            return 0
        return 1
    except Exception:
        return 2


def main():
    sutra_root = '/mnt/sutra'
    img_root = os.path.join(sutra_root, 'img')
    thb_root = os.path.join(sutra_root, 'thb')

    scanned = 0
    created = 0
    skipped = 0
    errors = 0
    page_count = 0

    futures = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for dirpath, filename_no_ext, ext in iter_media_files(img_root):

            img_path = os.path.join(dirpath, f'{filename_no_ext}.{ext}')

            rel = os.path.relpath(img_path, img_root)
            thb_rel = rel.rsplit('.', 1)[0] + '.jpg'
            thb_path = os.path.join(thb_root, thb_rel)

            if os.path.isfile(thb_path):
                skipped += 1
                scanned += 1
                page_count += 1
                if page_count >= PRINT_PAGE_SIZE:
                    page_count = 0
                    print(f'\r({scanned}) created={created} skipped={skipped} errors={errors} pending={len(futures)}', end='', flush=True)
                continue

            while len(futures) >= MAX_PENDING_FUTURES:
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for f in done:
                    try:
                        r = f.result()
                    except Exception:
                        r = 2
                    if r == 1:
                        created += 1
                    else:
                        errors += 1
                    futures.remove(f)

            futures.add(pool.submit(process_file, img_path, thb_path, ext))
            scanned += 1
            page_count += 1

            if page_count >= PRINT_PAGE_SIZE:
                page_count = 0
                print(f'\r({scanned}) created={created} skipped={skipped} errors={errors} pending={len(futures)}', end='', flush=True)
    
    print()
    print(f'final: scanned={scanned} created={created} skipped={skipped} errors={errors}', flush=True)


if __name__ == '__main__':
    main()