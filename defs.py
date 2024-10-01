from enum import Enum


class MediaType(Enum):
    full_media = "image"
    thumbnail = "thumb"


class URL(Enum):
    full_media = "https://i.4cdn.org/{board}/{image_id}{ext}"
    thumbnail = "https://i.4cdn.org/{board}/{image_id}s.jpg"
    catalog = "https://a.4cdn.org/{board}/catalog.json"
    thread = "https://a.4cdn.org/{board}/thread/{thread_id}.json"


h = {
    "headers": [
        # {"name": "Accept-Encoding", "value": "gzip, deflate, br, zstd"},
        # {"name": "Accept-Language", "value": "en-US,en;q=0.5"},
        # {"name": "Connection", "value": "keep-alive"},
    ]
}

h = {x["name"]: x["value"] for x in h["headers"]}
