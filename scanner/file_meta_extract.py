from typing import TypeAlias
import struct
from path import Path
import hashlib
import base64


WidthHeight: TypeAlias = tuple[int, int] | tuple[None, None]


def get_png_dimensions(file_path: str) -> WidthHeight:
    with open(file_path, 'rb') as f:
        if f.read(8) != b'\x89PNG\r\n\x1a\n':
            return None, None
        f.read(4)
        if f.read(4) != b'IHDR':
            return None, None
        width, height = struct.unpack('>II', f.read(8))
        return width, height
    return None, None


def get_jpeg_dimensions(file_path: str) -> WidthHeight:
    with open(file_path, 'rb') as f:
        if f.read(2) != b'\xff\xd8':
            return None, None
        while True:
            marker_bytes = f.read(2)
            if len(marker_bytes) < 2:
                break
            marker, = struct.unpack('>H', marker_bytes)
            if marker == 0xffd9:
                return None, None
            length_bytes = f.read(2)
            if len(length_bytes) < 2:
                break
            length, = struct.unpack('>H', length_bytes)
            if 0xffc0 <= marker <= 0xffcf and marker not in (0xffc4, 0xffc8, 0xffcc):
                f.read(1)
                hw = f.read(4)
                if len(hw) < 4:
                    break
                height, width = struct.unpack('>HH', hw)
                return width, height
            f.seek(length - 2, 1)
    return None, None


def get_gif_dimensions(file_path: str) -> WidthHeight:
    with open(file_path, 'rb') as f:
        sig = f.read(6)
        if sig not in (b'GIF87a', b'GIF89a'):
            return None, None
        dims = f.read(4)
        if len(dims) < 4:
            return None, None
        width, height = struct.unpack('<HH', dims)
        return width, height
    return None, None


def get_webp_dimensions(file_path: str) -> WidthHeight:
    with open(file_path, 'rb') as f:
        header = f.read(64)
        if not (header.startswith(b'RIFF') and header[8:12] == b'WEBP'):
            return None, None
        ext = header.find(b'VP8X')
        if ext != -1 and ext + 14 < len(header):
            width = int.from_bytes(header[ext+8:ext+11], 'little') + 1
            height = int.from_bytes(header[ext+11:ext+14], 'little') + 1
            return width, height
        vp8 = header.find(b'VP8 ')
        if vp8 != -1 and vp8 + 10 < len(header):
            width = int.from_bytes(header[vp8+6:vp8+8], 'little') & 0x3fff
            height = int.from_bytes(header[vp8+8:vp8+10], 'little') & 0x3fff
            return width, height
        vp8l = header.find(b'VP8L')
        if vp8l != -1 and vp8l + 9 < len(header):
            b = header[vp8l+5:vp8l+9]
            width = 1 + (((b[1] & 0x3f) << 8) | b[0])
            height = 1 + (((b[3] & 0x0f) << 10) | (b[2] << 2) | ((b[1] & 0xc0) >> 6))
            return width, height
    return None, None


def get_webm_dimensions(file_path: str) -> WidthHeight:
    with open(file_path, 'rb') as f:
        header = f.read(64 * 1024)
        if not header.startswith(b'\x1a\x45\xdf\xa3'):
            return None, None
        i = header.find(b'\x16\x54\xae\x6b')
        if i != -1:
            video_idx = header.find(b'\xe0', i)
            if video_idx != -1:
                w_idx = header.find(b'\xb0', video_idx)
                h_idx = header.find(b'\xba', video_idx)
                if w_idx != -1 and h_idx != -1:
                    w = int.from_bytes(header[w_idx+2:w_idx+4], 'big')
                    h = int.from_bytes(header[h_idx+2:h_idx+4], 'big')
                    return w, h
    return None, None


def get_mp4_dimensions(file_path: str) -> WidthHeight:
    with open(file_path, 'rb') as f:
        header = f.read(64 * 1024)
        i = 0
        while i + 8 <= len(header):
            size = int.from_bytes(header[i:i+4], 'big')
            if size < 8:
                break
            boxtype = header[i+4:i+8]
            if boxtype == b'moov':
                moov = header[i:i+size]
                j = 8
                while j + 8 <= len(moov):
                    boxsize = int.from_bytes(moov[j:j+4], 'big')
                    if boxsize < 8:
                        break
                    boxtype2 = moov[j+4:j+8]
                    if boxtype2 == b'tkhd':
                        version = moov[j+8]
                        offset = 20 if version == 0 else 32
                        w = int.from_bytes(moov[j+offset+36:j+offset+40], 'big') >> 16
                        h = int.from_bytes(moov[j+offset+40:j+offset+44], 'big') >> 16
                        return w, h
                    j += boxsize
            i += size
    return None, None


def get_avi_dimensions(file_path: str) -> WidthHeight:
    with open(file_path, 'rb') as f:
        header = f.read(64 * 1024)
        if not header.startswith(b'RIFF') or header[8:12] != b'AVI ':
            return None, None
        strh = header.find(b'strh')
        strf = header.find(b'strf', strh)
        if strf != -1 and strf + 40 <= len(header):
            bi_width = int.from_bytes(header[strf+8:strf+12], 'little')
            bi_height = int.from_bytes(header[strf+12:strf+16], 'little')
            return bi_width, abs(bi_height)
    return None, None


def get_mpeg_dimensions(file_path: str) -> WidthHeight:
    with open(file_path, 'rb') as f:
        data = f.read(64 * 1024)
        i = 0
        while i < len(data) - 4:
            if data[i] == 0x00 and data[i+1] == 0x00 and data[i+2] == 0x01 and data[i+3] == 0xB3:
                width = (data[i+4] << 4) | (data[i+5] >> 4)
                height = ((data[i+5] & 0x0F) << 8) | data[i+6]
                return width, height
            i += 1
    return None, None


def get_dimensions(fullpath: str=None, path: Path=None) -> WidthHeight:
    if fullpath:
        ext = fullpath.rsplit('.', maxsplit=1)[1]
    elif path:
        fullpath = path.fullpath
        ext = path.ext
    else:
        raise ValueError()

    match ext:
        case 'jpg' | 'jpeg':
            return get_jpeg_dimensions(fullpath)
        case 'webm' | 'mkv':
            return get_webm_dimensions(fullpath)
        case 'png':
            return get_png_dimensions(fullpath)
        case 'gif':
            return get_gif_dimensions(fullpath)
        case 'webp':
            return get_webp_dimensions(fullpath)
        case 'mp4' | 'mov' | 'm4v':
            return get_mp4_dimensions(fullpath)
        case 'avi':
            return get_avi_dimensions(fullpath)
        case 'mpg' | 'mpeg' | 'ts' | 'vob':
            return get_mpeg_dimensions(path.fullpath)


def hash_file(path: str, buffer_limit: int) -> tuple[str, str]:
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
