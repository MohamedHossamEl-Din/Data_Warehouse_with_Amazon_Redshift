import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config['IAM_ROLE']['ARN']

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events (
                                    artist text,
                                    
                                    auth text,
                                    
                                    firstName text,
                                    
                                    gender varchar(1),
                                    
                                    itemInSession int,
                                    
                                    lastName text,
                                    
                                    length numeric,
                                    
                                    level text,
                                    
                                    location text,
                                    
                                    method text,
                                    
                                    page text,
                                    
                                    registration numeric,
                                    
                                    sessionId int,
                                    
                                    song text,
                                    
                                    status int,
                                    
                                    ts BIGINT,
                                    
                                    userAgent text,
                                    
                                    userId int

);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (
                                    num_songs int,
                                    
                                    artist_id text,
                                    
                                    artist_latitude double precision,
                                    
                                    artist_longitude double precision,
                                    
                                    artist_location text,
                                    
                                    artist_name text,
                                    
                                    song_id text,
                                    
                                    title text,
                                    
                                    duration numeric,
                                    
                                    year int
);
""")

songplay_table_create = ("""CREATE TABLE songplays (
                                songplay_id int IDENTITY(0,1) PRIMARY KEY, 
                                
                                start_time TIMESTAMP NOT NULL REFERENCES time (start_time), 
                                
                                user_id int NOT NULL REFERENCES users (user_id), 
                                
                                level text, 
                                
                                song_id text REFERENCES songs (song_id), 
                                
                                artist_id text REFERENCES artists (artist_id), 
                                
                                session_id int, 
                                
                                location text, 
                                
                                user_agent text
                                );
""")

user_table_create = ("""CREATE TABLE users (
                            user_id int PRIMARY KEY, 
                            
                            first_name text, 
                            
                            last_name text, 
                            
                            gender varchar(1), 
                            
                            level text
                            );
""")

song_table_create = ("""CREATE TABLE songs (
                            song_id varchar PRIMARY KEY, 
                            
                            title text NOT NULL, 
                            
                            artist_id text, 
                            
                            year int, 
                            
                            duration numeric NOT NULL
                            );
""")

artist_table_create = ("""CREATE TABLE artists (
                              artist_id text PRIMARY KEY, 
                              
                              name text NOT NULL, 
                              
                              location text, 
                              
                              latitude double precision, 
                              
                              longitude double precision
                              );
""")

time_table_create = ("""CREATE TABLE time (
                            start_time TIMESTAMP PRIMARY KEY, 
                            
                            hour int, 
                            
                            day int, 
                            
                            week int, 
                            
                            month int, 
                            
                            year int, 
                            
                            weekday int
                            );
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2' 
format as JSON {}
timeformat as 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2' 
format as JSON 'auto'
timeformat as 'epochmillisecs';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT DISTINCT 
                                    timestamp 'epoch' + se.ts / 1000 * interval '1 second' AS start_time,
                                    
                                    se.userId AS user_id,
                                    
                                    se.level,
                                    
                                    ss.song_id,
                                    
                                    ss.artist_id,
                                    
                                    se.sessionId,
                                    
                                    ss.artist_location AS location,
                                    
                                    se.userAgent AS user_agent
                                    
                            FROM staging_events se
                            LEFT JOIN staging_songs ss
                            ON (se.artist = ss.artist_name) AND (se.song = ss.title) AND (se.length = ss.duration)
                            WHERE se.userId IS NOT NULL;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT 
                                userId AS user_id,
                                
                                firstName AS first_name,
                                
                                lastName AS last_name,
                                
                                gender,
                                
                                level
                        FROM staging_events
                        WHERE user_id IS NOT NULL
                        AND page  =  'NextSong';
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT
                                song_id,
                                
                                title,
                                
                                artist_id,
                                
                                year,
                                
                                duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT
                                  artist_id,
                                  
                                  artist_name AS name,
                                  
                                  artist_location AS location,
                                  
                                  artist_latitude AS latitude,
                                  
                                  artist_longitude AS longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)

                        SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time,

                        extract(hour from (timestamp 'epoch' + ts / 1000 * interval '1 second')),

                        extract(day from (timestamp 'epoch' + ts / 1000 * interval '1 second')),

                        extract(week from (timestamp 'epoch' + ts / 1000 * interval '1 second')),

                        extract(month from (timestamp 'epoch' + ts / 1000 * interval '1 second')),

                        extract(year from (timestamp 'epoch' + ts / 1000 * interval '1 second')),

                        extract(weekday from (timestamp 'epoch' + ts / 1000 * interval '1 second'))

                        FROM staging_events

                        WHERE ts IS NOT NULL;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
