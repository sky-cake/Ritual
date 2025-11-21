CREATE TABLE IF NOT EXISTS `%%BOARD%%` (
    doc_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    poster_ip TEXT NOT NULL,
    num INTEGER NOT NULL,
    subnum INTEGER NOT NULL,
    thread_num INTEGER NOT NULL,
    op INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    timestamp_expired INTEGER NOT NULL,
    preview_orig TEXT,
    preview_w INTEGER NOT NULL,
    preview_h INTEGER NOT NULL,
    media_filename TEXT,
    media_w INTEGER NOT NULL,
    media_h INTEGER NOT NULL,
    media_size INTEGER NOT NULL,
    media_hash TEXT,
    media_orig TEXT,
    spoiler INTEGER NOT NULL,
    deleted INTEGER NOT NULL,
    capcode TEXT NOT NULL,
    email TEXT,
    name TEXT,
    trip TEXT,
    title TEXT,
    comment TEXT,
    delpass TEXT,
    sticky INTEGER NOT NULL,
    locked INTEGER NOT NULL,
    poster_hash TEXT,
    poster_country TEXT,
    exif TEXT,
    UNIQUE (num, subnum)
);

CREATE TABLE IF NOT EXISTS `%%BOARD%%_images` (
    media_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    media_hash TEXT NOT NULL,
    media TEXT,
    preview_op TEXT,
    preview_reply TEXT,
    total INTEGER NOT NULL,
    banned INTEGER NOT NULL,
    UNIQUE (media_hash)
);

CREATE TABLE IF NOT EXISTS `%%BOARD%%_threads` (
    thread_num INTEGER NOT NULL PRIMARY KEY,
    time_op INTEGER NOT NULL,
    time_last INTEGER NOT NULL,
    time_bump INTEGER NOT NULL,
    time_ghost INTEGER,
    time_ghost_bump INTEGER,
    time_last_modified INTEGER NOT NULL,
    nreplies INTEGER NOT NULL,
    nimages INTEGER NOT NULL,
    sticky INTEGER NOT NULL,
    locked INTEGER NOT NULL,
    UNIQUE (thread_num)
);

CREATE TABLE IF NOT EXISTS `%%BOARD%%_deleted` (
    doc_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    poster_ip TEXT NOT NULL,
    num INTEGER NOT NULL,
    subnum INTEGER NOT NULL,
    thread_num INTEGER NOT NULL,
    op INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    timestamp_expired INTEGER NOT NULL,
    preview_orig TEXT,
    preview_w INTEGER NOT NULL,
    preview_h INTEGER NOT NULL,
    media_filename TEXT,
    media_w INTEGER NOT NULL,
    media_h INTEGER NOT NULL,
    media_size INTEGER NOT NULL,
    media_hash TEXT,
    media_orig TEXT,
    spoiler INTEGER NOT NULL,
    deleted INTEGER NOT NULL,
    capcode TEXT NOT NULL,
    email TEXT,
    name TEXT,
    trip TEXT,
    title TEXT,
    comment TEXT,
    delpass TEXT,
    sticky INTEGER NOT NULL,
    locked INTEGER NOT NULL,
    poster_hash TEXT,
    poster_country TEXT,
    exif TEXT,
    UNIQUE (num)
);

DROP INDEX IF EXISTS idx_%%BOARD%%_num;
CREATE UNIQUE INDEX IF NOT EXISTS idx_%%BOARD%%_num_subnum ON `%%BOARD%%` (num, subnum);

CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_media_id ON `%%BOARD%%` (media_id);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_thread_num ON `%%BOARD%%` (thread_num);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_timestamp ON `%%BOARD%%` (timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON `%%BOARD%%_images` (media_id);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON `%%BOARD%%_images` (media_hash);

CREATE UNIQUE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON `%%BOARD%%_threads` (thread_num);
