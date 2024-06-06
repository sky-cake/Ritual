from enum import Enum


class MediaType(Enum):
    full_media = 'image'
    thumbnail = 'thumb'


class URL(Enum):
    full_media = 'https://i.4cdn.org/{board}/{image_id}{ext}'
    thumbnail = 'https://i.4cdn.org/{board}/{image_id}s.jpg'
    catalog = 'https://a.4cdn.org/{board}/catalog.json'
    thread = 'https://a.4cdn.org/{board}/thread/{thread_id}.json'