CREATE TABLE IF NOT EXISTS %%BOARD%% (
    doc_id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER,
    poster_ip TEXT,
    num INTEGER NOT NULL,
    subnum INTEGER,
    thread_num INTEGER,
    op INTEGER,
    timestamp INTEGER,
    timestamp_expired INTEGER,
    preview_orig TEXT,
    preview_w INTEGER,
    preview_h INTEGER,
    media_filename TEXT,
    media_w INTEGER,
    media_h INTEGER,
    media_size INTEGER,
    media_hash TEXT,
    media_orig TEXT,
    spoiler INTEGER,
    deleted INTEGER,
    capcode TEXT,
    email TEXT,
    name TEXT,
    trip TEXT,
    title TEXT,
    comment TEXT,
    delpass TEXT,
    sticky INTEGER,
    locked INTEGER,
    poster_hash TEXT,
    poster_country TEXT,
    exif TEXT,
    UNIQUE (num)
);

CREATE TABLE IF NOT EXISTS %%BOARD%%_images (
    media_id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_hash TEXT NOT NULL,
    media TEXT,
    preview_op TEXT,
    preview_reply TEXT,
    total INTEGER,
    banned INTEGER,
    UNIQUE (media_hash)
);

CREATE TABLE IF NOT EXISTS %%BOARD%%_threads (
    thread_num INTEGER PRIMARY KEY,
    time_op INTEGER,
    time_last INTEGER,
    time_bump INTEGER,
    time_ghost INTEGER,
    time_ghost_bump INTEGER,
    time_last_modified INTEGER,
    nreplies INTEGER,
    nimages INTEGER,
    sticky INTEGER,
    locked INTEGER,
    UNIQUE (thread_num)
);


CREATE UNIQUE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON %%BOARD%% (num);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_media_id ON %%BOARD%% (media_id);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_thread_num ON %%BOARD%% (thread_num);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_timestamp ON %%BOARD%% (timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON %%BOARD%%_images (media_id);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON %%BOARD%%_images (media_hash);

CREATE UNIQUE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON %%BOARD%%_threads (thread_num);
