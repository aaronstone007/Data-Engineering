import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ("""drop table if exists staging_events;""")
staging_songs_table_drop = ("""drop table if exists staging_songs;""")
songplay_table_drop = ("""drop table if exists songplays;""")
user_table_drop = ("""drop table if exists users;""")
song_table_drop = ("""drop table if exists songs;""")
artist_table_drop = ("""drop table if exists artists;""")
time_table_drop = ("""drop table if exists time;""")

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events (
    artist varchar, 
    auth varchar, 
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration bigint,
    sessionId int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs int,
        artist_id text,
        artist_name text,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location text,
        song_id text,
        title text,
        duration numeric,
        year int
    );
""") 

songplay_table_create = ("""
create table if not exists songplays (
    songplay_id int IDENTITY(0,1),
    start_time timestamp not null,
    user_id int not null,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id numeric not null,
    location varchar,
    user_agent varchar
    );
""")

user_table_create = ("""
create table if not exists users(
    user_id int NOT NULL PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
    );
""")

song_table_create = ("""
create table if not exists songs(
    song_id varchar NOT NULL PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year int,
    duration float
    );
""")

artist_table_create = ("""
create table if not exists artists(
    artist_id varchar NOT NULL PRIMARY KEY,
    name varchar,
    location varchar,
    lattitude float,
    longitude float
    );
""")

time_table_create = ("""
create table if not exists time(
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select TIMESTAMP 'epoch' + ts/1000 * interval '1 second',e.userId,e.level,s.song_id,s.artist_id,e.sessionId,e.location,e.userAgent
from staging_events as e
join staging_songs as s
on e.song = s.title
and e.artist = s.artist_name
and e.length = s.duration
where e.page='NextSong';
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct userId,firstName,lastName,gender,level
from staging_events
where page = 'NextSong';
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
select distinct song_id, title, artist_id, year, duration
from staging_songs;
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, lattitude, longitude)
select distinct artist_id,artist_name,artist_location,artist_latitude,artist_longitude
from staging_songs;
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select distinct start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
from songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]