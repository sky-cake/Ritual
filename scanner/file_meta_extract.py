import base64
import hashlib


def get_md5_b64(path: str) -> str:
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(10_485_760), b''):
            md5.update(chunk)
    return base64.b64encode(md5.digest()).decode()
