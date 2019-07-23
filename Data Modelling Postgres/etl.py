import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    1. Insert song_id, title, artist_id, year, duration into song_table_insert table.
    2. Insert artist_id, artist_name, artist_location, artist_latitude, artist_longitude into artist_table_insert table.
    
    Args:
        cur : database cursor
        filepath : current filepath
    """
    
    # open song file
    df = pd.read_json(filepath,lines=True)
    song_data = (df[['song_id','title','artist_id','year','duration']].values).tolist()[0]

    # insert song record
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values).tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    1. Insert timestamp, hour, day, week_of_year, month, year, weekday into time_table_insert table.
    2. Insert userId, firstName, lastName, gender, level into user_table_insert table.
    3. Insert ts, userId, level, songid, artistid, sessionId, location, userAgent into songplay_data table.
        
    Args:
        cur : database cursor
        filepath : current filepath      
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    time_data = (t.values.tolist(),t.dt.hour.values.tolist(),t.dt.day.values.tolist(),t.dt.week.values.tolist(),t.dt.month.values.tolist(),t.dt.year.values.tolist(),t.dt.weekday.values.tolist())
    
    column_labels = ('timestamp','hour','day','week_of_year','month','year','weekday')
    
    time_df = pd.DataFrame({column_labels[0]:time_data[0],
                           column_labels[1]:time_data[1],
                           column_labels[2]:time_data[2],
                           column_labels[3]:time_data[3],
                           column_labels[4]:time_data[4],
                           column_labels[5]:time_data[5],
                           column_labels[6]:time_data[6]}
                            )

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """
    Iterate over files.    
    Args:
        cur : database cursor
        conn : database connection object
        filepath : current filepath
        func : which function to run
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    # connection object creation
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    # curson for DB connection
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()