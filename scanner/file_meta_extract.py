import base64
import hashlib


def get_sha256_and_md5_b64(path: str, buffer_limit: int) -> tuple[str, str]:
    sha = hashlib.sha256()
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        while True:
            b = f.read(buffer_limit)
            if not b:
                break
            sha.update(b)
            md5.update(b)
    return sha.hexdigest(), base64.b64encode(md5.digest()).decode()
