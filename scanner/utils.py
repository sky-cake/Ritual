import os


def make_path(*args) -> str:
    d = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(d, *args)


def iter_media_files(root_path: str, skip_dirnames: set[str] | None=None, valid_exts: set[str] | None=None):
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
