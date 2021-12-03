import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')

LOG_DATA     = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA    = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (event_id       integer IDENTITY(0,1) PRIMARY KEY,
     artist         VARCHAR,
     auth           VARCHAR,
     firstName      VARCHAR,
     gender         VARCHAR,
     itemInSession  INTEGER,
     lastName       VARCHAR,
     length         FLOAT,
     level          VARCHAR,
     location       VARCHAR,
     method         VARCHAR,
     page           VARCHAR,
     registration   VARCHAR,
     sessionId      INTEGER,
     song           VARCHAR,
     status         INTEGER,
     ts             BIGINT,
     userAgent      VARCHAR,
     user_id        INTEGER);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (song_id           VARCHAR PRIMARY KEY,
     num_songs         INTEGER,
     artist_id         VARCHAR,
     artist_latitude   FLOAT,
     artist_longitude  FLOAT,
     artist_location   VARCHAR,
     artist_name       VARCHAR,
     title             VARCHAR,
     duration          FLOAT,
     year              INTEGER);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (songplay_id   INTEGER   IDENTITY(0,1) PRIMARY KEY, 
     start_time    TIMESTAMP NOT NULL, 
     user_id       INTEGER   NOT NULL,
     level         VARCHAR   NOT NULL,
     song_id       VARCHAR,
     artist_id     VARCHAR,
     session_id    INTEGER   NOT NULL,
     location      VARCHAR   NOT NULL,
     user_agent    VARCHAR   NOT NULL);
""")


user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (user_id       INTEGER  PRIMARY KEY, 
     first_name    VARCHAR  NOT NULL, 
     last_name     VARCHAR  NOT NULL,
     gender        CHAR(1)  NOT NULL,
     level         VARCHAR  NOT NULL);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (song_id   VARCHAR  PRIMARY KEY, 
     title     VARCHAR  NOT NULL, 
     artist_id VARCHAR  NOT NULL,
     year      INTEGER  NOT NULL,
     duration  FLOAT    NOT NULL);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (artist_id   VARCHAR  PRIMARY KEY, 
     name        VARCHAR, 
     location    VARCHAR,
     latitude    FLOAT,
     longitude   FLOAT);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (start_time   TIMESTAMP  PRIMARY KEY, 
     hour         INTEGER    NOT NULL, 
     day          INTEGER    NOT NULL,
     week         INTEGER    NOT NULL,
     month        INTEGER    NOT NULL,
     year         INTEGER    NOT NULL,
     weekday      INTEGER    NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    format as json {} compupdate off REGION 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    format as json 'auto' compupdate off REGION 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        DATEADD(s, CONVERT(BIGINT, se.ts) / 1000, CONVERT(DATETIME, '1-1-1970 00:00:00')), 
        se.user_id, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.sessionId, 
        se.location, 
        se.userAgent
    FROM staging_events AS se INNER JOIN staging_songs AS ss
    ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        user_id, 
        firstName, 
        lastName, 
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong' AND user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        start_time, 
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time), 
        EXTRACT(week FROM start_time), 
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(weekday FROM start_time)
    FROM songplays
    WHERE start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
