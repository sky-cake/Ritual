import os


class Path:
    __slots__ = (
        'dirpath',
        'filename_no_ext',
        'ext',
        'dir_id',
        'ext_id',

        'sha256',
        'md5_b64',
        'bsize',
        'w',
        'h',
    )

    def __init__(self, dirpath, filename_no_ext, ext, dir_id, ext_id):
        self.filename_no_ext = filename_no_ext
        self.dirpath = dirpath
        self.ext = ext
        self.dir_id = dir_id
        self.ext_id = ext_id

        self.sha256 = None
        self.md5_b64 = None
        self.bsize = None
        self.w = None
        self.h = None

    @property
    def fullpath(self) -> str:
        return os.path.join(self.dirpath, self.filename_w_ext)

    @property
    def filename_w_ext(self) -> str:
        return f'{self.filename_no_ext}.{self.ext}'

