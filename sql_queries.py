#!/usr/bin/env python3

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS STG_EVENTS;"
staging_songs_table_drop = "DROP TABLE IF EXISTS STG_SONGS;"
songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS;"
user_table_drop = "DROP TABLE IF EXISTS USERS;"
song_table_drop = "DROP TABLE IF EXISTS SONGS;"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS;"
time_table_drop = "DROP TABLE IF EXISTS TIME;"

# CREATE TABLES

#ARTIST, AUTH, FIRST_NAME, GENDER, ITEM_IN_SESSION, LAST_NAME, LENGTH, LEVEL, LOCATION, METHOD, PAGE, REGISTRATION, SESSION_ID, SONG, STATUS, TS, USER_AGENT,USER_ID
staging_events_table_create= ("""
CREATE TABLE STG_EVENTS (
USER_IDEN_KEY INT IDENTITY(1,1),
ARTIST VARCHAR,
AUTH VARCHAR,
FIRST_NAME VARCHAR,
GENDER VARCHAR,
ITEM_IN_SESSION INT,
LAST_NAME VARCHAR,
LENGTH FLOAT, 
LEVEL VARCHAR,
LOCATION VARCHAR,
METHOD VARCHAR,
PAGE VARCHAR,
REGISTRATION BIGINT,
SESSION_ID INT,
SONG VARCHAR,
STATUS INT,
TS BIGINT,
USER_AGENT VARCHAR,
USER_ID INT
);
""")

#NUM_SONGS, ARTIST_ID, ARTIST_LATITUDE, ARTIST_LONGITUDE, ARTIST_LOCATION, ARTIST_NAME, SONG_ID, TITLE, DURATION, YEAR
staging_songs_table_create = ("""
CREATE TABLE STG_SONGS (
NUM_SONGS INT,
ARTIST_ID VARCHAR,
ARTIST_LATITUDE FLOAT,
ARTIST_LONGITUDE FLOAT,
ARTIST_LOCATION VARCHAR,
ARTIST_NAME VARCHAR,
SONG_ID VARCHAR,
TITLE VARCHAR,
DURATION FLOAT,
YEAR INT
);
""")

#SONGPLAY_ID, START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT
songplay_table_create = ("""
CREATE TABLE SONGPLAYS (
START_TIME BIGINT NOT NULL sortkey,
USER_ID INT NOT NULL,
LEVEL VARCHAR,
SONG_ID VARCHAR NOT NULL,
ARTIST_ID VARCHAR NOT NULL distkey,
SESSION_ID INT, 
LOCATION VARCHAR,
USER_AGENT VARCHAR,
FOREIGN KEY (USER_ID) REFERENCES USERS (USER_ID),
FOREIGN KEY (ARTIST_ID) REFERENCES ARTISTS (ARTIST_ID),
FOREIGN KEY (SONG_ID) REFERENCES SONGS (SONG_ID)
);
""")

#USER_ID, FIRST_NAME, LAST_NAME, GENDER, LEVEL
user_table_create = ("""
CREATE TABLE USERS (
USER_ID INT NOT NULL PRIMARY KEY,
FIRST_NAME VARCHAR,
LAST_NAME VARCHAR NOT NULL,
GENDER VARCHAR,
LEVEL VARCHAR
);
""")

#SONG_ID, TITLE, ARTIST_ID, YEAR, DURATION
song_table_create = ("""
CREATE TABLE SONGS (
SONG_ID VARCHAR NOT NULL PRIMARY KEY sortkey,
TITLE VARCHAR NOT NULL, 
ARTIST_ID VARCHAR NOT NULL,
YEAR INT NOT NULL,
DURATION FLOAT NOT NULL
);
""")

#ARTIST_ID, NAME, LOCATION, LATITUDE, LONGITUDE
artist_table_create = ("""
CREATE TABLE ARTISTS (
ARTIST_ID VARCHAR NOT NULL PRIMARY KEY sortkey distkey,
ARTIST_NAME TEXT NOT NULL, 
ARTIST_LOCATION VARCHAR,
ARTIST_LATITUDE NUMERIC,
ARTIST_LONGITUDE NUMERIC
);
""")

#START_TIME, HOUR, DAY, WEEK, MONTH, YEAR, WEEKDAY
time_table_create = ("""
CREATE TABLE TIME (
START_TIME TIMESTAMPTZ NOT NULL PRIMARY KEY sortkey, 
HOUR INT NOT NULL,
DAY INT NOT NULL,
WEEK INT NOT NULL,
MONTH INT NOT NULL,
YEAR INT NOT NULL,
WEEKDAY INT NOT NULL
);
""")

#Get values of S3/IAM_ROLE sections in dwh.cfg file

EVENTS_DATA= config.get("S3","LOG_DATA")
SONGS_DATA= config.get("S3","SONG_DATA")
LOG_PATH= config.get("S3","LOG_JSONPATH")

ARN= config.get("IAM_ROLE","ARN")

# STAGING TABLES

staging_events_copy = ("""
COPY STG_EVENTS from {}
iam_role {} 
region 'us-west-2' FORMAT AS JSON {};""").format(EVENTS_DATA, ARN, LOG_PATH)

staging_songs_copy = ("""
COPY STG_SONGS from {}
iam_role {}
region 'us-west-2' FORMAT AS JSON 'auto';""").format(SONGS_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO SONGPLAYS (SONG_ID, ARTIST_ID, START_TIME, USER_ID, LEVEL, SESSION_ID, LOCATION, USER_AGENT) \
                         SELECT DISTINCT s.SONG_ID, s.ARTIST_ID, e.ts, e.USER_ID, e.LEVEL, e.SESSION_ID, e.LOCATION, e.USER_AGENT \
                         FROM STG_EVENTS e \
                         INNER JOIN stg_songs s \
                         ON e.song = s.title and e.artist = s.artist_name \
                         WHERE e.PAGE='NextSong';""")

user_table_insert = ("""INSERT INTO USERS \
                         SELECT DISTINCT st.user_Id, st.first_Name, st.last_Name, st.gender, st.level \
                         FROM STG_EVENTS st \
                         LEFT JOIN USERS using(user_id) \
                         WHERE USERS.user_id is null \
                         AND PAGE='NextSong';""")

song_table_insert = ("""INSERT INTO SONGS \
                        SELECT DISTINCT st.song_id, st.title, st.artist_id, st.year, st.duration \
                        FROM STG_SONGS st \
                        LEFT JOIN \
                        SONGS using(song_id) \
                        WHERE SONGS.SONG_ID is null;""")

artist_table_insert = ("""INSERT INTO ARTISTS \
                          SELECT DISTINCT st.artist_id, st.artist_name, st.artist_location, st.artist_latitude, st.artist_longitude \
                          FROM STG_SONGS st \
                          LEFT JOIN ARTISTS using(artist_id) \
                          WHERE ARTISTS.artist_id is null;""")


time_table_insert = ("""INSERT INTO TIME (START_TIME, HOUR, DAY, WEEK, MONTH, YEAR, WEEKDAY) \
                      SELECT DISTINCT TIMESTAMP WITH TIME ZONE 'epoch' + (ts/1000) * INTERVAL '1 Second ', \
                      extract(hr from (TIMESTAMP WITH TIME ZONE 'epoch' + (ts/1000) * INTERVAL '1 Second ')), \
                      extract(day from (TIMESTAMP WITH TIME ZONE 'epoch' + (ts/1000) * INTERVAL '1 Second ')), \
                      extract(w from (TIMESTAMP WITH TIME ZONE 'epoch' + (ts/1000) * INTERVAL '1 Second ')), \
                      extract(mon from (TIMESTAMP WITH TIME ZONE 'epoch' + (ts/1000) * INTERVAL '1 Second ')), \
                      extract(y from (TIMESTAMP WITH TIME ZONE 'epoch' + (ts/1000) * INTERVAL '1 Second ')), \
                      extract(weekday from (TIMESTAMP WITH TIME ZONE 'epoch' + (ts/1000) * INTERVAL '1 Second ')) \
                      FROM STG_EVENTS
                      WHERE PAGE='NextSong';""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries= [song_table_insert, user_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
