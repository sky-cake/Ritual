from enum import Enum


class MediaType(Enum):
    full_media = "image"
    thumbnail = "thumb"


class URL4chan(Enum):
    full_media = "https://i.4cdn.org/{board}/{image_id}{ext}"
    thumbnail = "https://i.4cdn.org/{board}/{image_id}s.jpg"
    catalog = "https://a.4cdn.org/{board}/catalog.json"
    thread = "https://a.4cdn.org/{board}/thread/{thread_id}.json"


class URLlainchan(Enum):
    full_media = "https://lainchan.org/{board}/src/{image_id}{ext}"
    thumbnail = None
    catalog = "https://lainchan.org/{board}/catalog.json"
    thread = "https://lainchan.org/{board}/res/{thread_id}.json"
