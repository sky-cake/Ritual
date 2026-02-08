from enum import Enum


class MediaType(Enum):
    full_media = 'image'
    thumbnail = 'thumb'


class DeletionType(Enum):
    inconclusive = 'inconclusive' # do nothing
    archived = 'archived' # marked as locked
    deleted = 'deleted' # marked as deleted
    pruned = 'pruned' # do nothing
