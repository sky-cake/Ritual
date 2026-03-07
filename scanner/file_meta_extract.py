import base64
import hashlib


def get_sha256_and_md5_b64(path: str) -> tuple[str, str]:
    sha = hashlib.sha256()
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(10_485_760), b''):
            sha.update(chunk)
            md5.update(chunk)
    return sha.hexdigest(), base64.b64encode(md5.digest()).decode()
