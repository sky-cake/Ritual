import os


class MediaMeta:
    __slots__ = (
        'dirpath',
        'filename_no_ext',
        'ext',
        'dir_id',
        'ext_id',

        'md5_b64_given',
        'md5_b64_computed',
        'sha256',
        'bsize',
    )

    def __init__(self, dirpath, filename_no_ext, ext, dir_id, ext_id):
        self.filename_no_ext = filename_no_ext
        self.dirpath = dirpath
        self.ext = ext
        self.dir_id = dir_id
        self.ext_id = ext_id

        self.md5_b64_given = None
        self.md5_b64_computed = None
        self.sha256 = None
        self.bsize = None

    @property
    def fullpath(self) -> str:
        return os.path.join(self.dirpath, self.filename_w_ext)

    @property
    def filename_w_ext(self) -> str:
        return f'{self.filename_no_ext}.{self.ext}'

