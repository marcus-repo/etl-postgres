import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Retrieves song and artist data from json song file.
    
    - Inserts song and artist data into database tables
    using sql insert statments from sql_queries: artist_table_insert,
    song_table_insert.
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data = df[['artist_id','artist_name', 'artist_location',
                      'artist_latitude', 'artist_longitude']]\
                        .values[0].tolist()
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e: 
        print("Error: Inserting Rows")
        print(e)
    
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']]\
        .values[0].tolist()
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e: 
        print("Error: Inserting Rows")
        print(e)
        
        
def process_log_file(cur, filepath):
    """
    - Retrieves time, user and songplay data from json log file.
    
    - Filters and cleans data into pandas dataframes.
    
    - Loops through each dataframe and inserts data into database 
    tables using sql insert statments from sql_queries: time_table_insert, 
    user_table_insert songplay_table_insert.
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, 
                 t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 
                     'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, 
                                    [x.values for x in time_data])))
    
    tuples_time_df = [tuple(x) for x in time_df.values]
    try:
        cur.executemany(time_table_insert, tuples_time_df)
    except psycopg2.Error as e: 
        print("Error: Inserting rows into times table")
        print(e)
    
    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    tuples_user_df = [tuple(x) for x in user_df.values]
    try:
        cur.executemany(user_table_insert, tuples_user_df)
    except psycopg2.Error as e: 
        print("Error: Inserting rows into user table")
        print(e)
    
    # insert songplay records
    for index, row in df.iterrows():
        
        try:
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None
    
            # insert songplay record
            songplay_data = (pd.to_datetime(row['ts'], unit='ms'), 
                             row['userId'], row['level'], songid, 
                             artistid, row['sessionId'], row['location'], 
                             row['userAgent'])
            
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e: 
            print("Error: Inserting rows into songplay table")
            print(e)


def process_data(cur, conn, filepath, func):
    """
    - Creates list of json-files on given filepath.
    
    - Iterates over json-files and given functions (func) 
    to process the files as defined in func.
    
    - Commits changes to database.
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
    """  
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Retrieves list of songfiles from filepath and inserts it into 
    sparkify database tables.  
    
    - Retrieves list of logfiles from filepath and inserts it into
    sparkify database tables. 
    
    - Finally, closes the connection. 
    """
    
    conn = None
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
                                user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not connect to DB")
        print(e)
    else:
        process_data(cur, conn, filepath='data/song_data', 
                     func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', 
                     func=process_log_file)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()