from enum import Enum


class MediaType(Enum):
    full_media = 'image'
    thumbnail = 'thumb'


class DeletionType(Enum):
    archived = 'archived'
    deleted = 'deleted'
    pruned = 'pruned'
