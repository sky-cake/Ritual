update %%BOARD%% set doc_id = 0 where doc_id is null;
update %%BOARD%% set media_id = 0 where media_id is null;
update %%BOARD%% set poster_ip = '0' where poster_ip is null;
update %%BOARD%% set num = 0 where num is null;
update %%BOARD%% set subnum = 0 where subnum is null;
update %%BOARD%% set thread_num = 0 where thread_num is null;
update %%BOARD%% set op = 0 where op is null;
update %%BOARD%% set timestamp = 0 where timestamp is null;
update %%BOARD%% set timestamp_expired = 0 where timestamp_expired is null;
update %%BOARD%% set preview_w = 0 where preview_w is null;
update %%BOARD%% set preview_h = 0 where preview_h is null;
update %%BOARD%% set media_w = 0 where media_w is null;
update %%BOARD%% set media_h = 0 where media_h is null;
update %%BOARD%% set media_size = 0 where media_size is null;
update %%BOARD%% set spoiler = 0 where spoiler is null;
update %%BOARD%% set deleted = 0 where deleted is null;
update %%BOARD%% set capcode = 'N' where capcode is null;
update %%BOARD%% set sticky = 0 where sticky is null;
update %%BOARD%% set locked = 0 where locked is null;

update %%BOARD%%_images set media_id = 0 where media_id is null;
update %%BOARD%%_images set media_hash = '' where media_hash is null;
update %%BOARD%%_images set total = 0 where total is null;
update %%BOARD%%_images set banned = 0 where banned is null;

update %%BOARD%%_threads set thread_num = 0 where thread_num is null;
update %%BOARD%%_threads set time_op = 0 where time_op is null;
update %%BOARD%%_threads set time_last = 0 where time_last is null;
update %%BOARD%%_threads set time_bump = 0 where time_bump is null;
update %%BOARD%%_threads set time_last_modified = 0 where time_last_modified is null;
update %%BOARD%%_threads set nreplies = 0 where nreplies is null;
update %%BOARD%%_threads set nimages = 0 where nimages is null;
update %%BOARD%%_threads set sticky = 0 where sticky is null;
update %%BOARD%%_threads set locked = 0 where locked is null;

CREATE TABLE IF NOT EXISTS %%BOARD%%_asagi (
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

CREATE TABLE IF NOT EXISTS %%BOARD%%_asagi_images (
    media_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    media_hash TEXT NOT NULL,
    media TEXT,
    preview_op TEXT,
    preview_reply TEXT,
    total INTEGER NOT NULL,
    banned INTEGER NOT NULL,
    UNIQUE (media_hash)
);

CREATE TABLE IF NOT EXISTS %%BOARD%%_asagi_threads (
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

INSERT INTO %%BOARD%%_asagi
SELECT * FROM %%BOARD%%;
INSERT INTO %%BOARD%%_asagi_images
SELECT * FROM %%BOARD%%_images;
INSERT INTO %%BOARD%%_asagi_threads
SELECT * FROM %%BOARD%%_threads;

drop table %%BOARD%%;
drop table %%BOARD%%_images;
drop table %%BOARD%%_threads;

ALTER TABLE %%BOARD%%_asagi rename to %%BOARD%%;
ALTER TABLE %%BOARD%%_asagi_images rename to %%BOARD%%_images;
ALTER TABLE %%BOARD%%_asagi_threads rename to %%BOARD%%_threads;

CREATE UNIQUE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON %%BOARD%% (num);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_media_id ON %%BOARD%% (media_id);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_thread_num ON %%BOARD%% (thread_num);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_timestamp ON %%BOARD%% (timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON %%BOARD%%_images (media_id);
CREATE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON %%BOARD%%_images (media_hash);

CREATE UNIQUE INDEX IF NOT EXISTS idx_%%BOARD%%_num ON %%BOARD%%_threads (thread_num);