# Import essential packages
import os

# the glob module is used to retrieve files/pathnames 
# matching a specified pattern. The pattern rules of glob 
# follow standard Unix path expansion rules. It is also predicted 
# that according to benchmarks it is faster than other methods to 
# match pathnames in directories
import glob

import psycopg2 as postgres
import pandas as pd

# Import all queries from sql_queries.py
from sql_queries import *


def process_song_file(cursor, file_path):
    """Process the song file to insert data into the specific data model for 
    analysis.

    Args:
        cursor (postgres object): to execute queries
        file_path (str): The absolute path for the song file to wrangle
    """    
    
    # Convert the song file into a dataframe
    df_song = pd.read_json(file_path, lines=True)
        
    # Select the columns we need for the 'songs' table
    songs_table_df = df_song[['song_id', 'title', 'artist_id', 'year', 
                                    'duration']]
    
    # Loop over the records in 'songs_table_df' dataframe
    for i, row in songs_table_df.iterrows():
    
        # Insert the current row in the 'songs' table
        cursor.execute(songs_table_insert, list(row))
        
    # Select the columns we need for the artists table
    artists_df = df_song[['artist_id', 'artist_name', 'artist_location', 
                          'artist_latitude', 'artist_longitude']]
    
    # Loop over the records in 'artists_df' dataframe
    for i, row in artists_df.iterrows():
    
        # Insert the current row in 'artists' table
        cursor.execute(artists_table_insert, list(row))
        

def process_log_file(cursor, file_path):
    """Process the log file to insert data into the specific data model for 
    analysis.

    Args:
        cursor (postgres object): to execute queries
        file_path (str): The absolute path for the log file to wrangle
    """    
    
    # Convert the log file to a dataframe
    df_log_data = pd.read_json(file_path, lines=True)

    # We filter the dataframe records to extract the records with a value of 
    # 'NextSong' in the 'page' column
    df_log_data = df_log_data[df_log_data.page == 'NextSong']\
        .reset_index(drop = True)

    # Convert the 'ts' column to datetime format
    df_log_data['start_time'] = pd.to_datetime(df_log_data.ts, unit = 'ms')

    # Now we can extract the hour, day, week of year, month, year, and 
    # weekday from 'start_time' column 
    df_log_data['year'] = df_log_data['start_time'].dt.year
    df_log_data['month'] = df_log_data['start_time'].dt.month
    df_log_data['week'] = df_log_data['start_time'].dt.isocalendar().week
    df_log_data['weekday'] = df_log_data['start_time'].dt.dayofweek
    df_log_data['day'] = df_log_data['start_time'].dt.day
    df_log_data['hour'] = df_log_data['start_time'].dt.hour

    # 'time' Table:
    
    # Create `time_df` dataframe containing the time data for this file
    time_df = df_log_data[['start_time', 'hour', 'day', 'week', 'month', 
                           'year', 'weekday']]
    
    # Create `time_df` dataframe containing the time data for this file
    time_df = df_log_data[['start_time', 'hour', 'day', 'week', 'month', 
                           'year', 'weekday']]
    
    # Loop over the rows in 'time_df' dataframe
    for i, row in time_df.iterrows():
        
        # Insert the current row in 'time' table
        cursor.execute(time_table_insert, list(row))

    # 'users' table:
    
    # Extract the columns we want into a new dataframe 'user_df'
    user_df = df_log_data[['userId', 'firstName', 'lastName', 'gender', 
                           'level']]
    
    # Tthere are many duplicated rows per user in the log file
    # However, We only want one record for each user in the 'users' table
    # So, we drop the duplicated rows
    user_df = user_df.drop_duplicates().sort_values(by=['userId'])\
        .reset_index(drop = True)

    # Loop over the rows in 'user_df' and insert each record into 'users' table
    for i, row in user_df.iterrows():
        
        # Execute the INSERT query for the current record
        cursor.execute(users_table_insert, row)
    
    # 'songplays' Table
    
    # Loop over the records in the log file dataframe
    for index, row in df_log_data.iterrows():

        # get song_id and artist_id from song and artist tables
        
        # The query looks like this:
        #################
        # SELECT s.song_id, a.artist_id
        # FROM songs s
        # JOIN artists a
        # ON s.artist_id = a.artist_id
        # WHERE s.title = %s AND a.name = %s AND s.duration = %s
        #################
        # As you see, we already put placeholders in the query;
        # to run the WHERE statements with dynamic values
        # These values, we get it from each record in this loop
        
        # So, we execute the SELECT query with the variables of the
        # current record
        cursor.execute(song_select, (row.song, row.artist, row.length))
        
        # Since we expect one result, we fetch one row from the results
        results = cursor.fetchone()
        
        # We check if there are any results
        if results:
            
            # If so, we unpack the results into two variables;
            # since we only selected two variables in the query
            song_id, artist_id = results
            
        else:
            # If there are no results, we set the two variables to none
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = (row.start_time, row.userId, row.level, song_id, 
                         artist_id, row.sessionId, row.location, row.userAgent)
        
        # Execute the query and insert the data into 'songplays' table
        cursor.execute(songplay_table_insert, songplay_data)


def process_data(cursor, connection, file_path, func):
    """Read all the files in `file_path` and call the `func` function

    Args:
        cursor (postgres object): to execute queries
        connection (Postgres object): Postgres connection variable to 
            a database.
        file_path (str): The absolute path for the log file to wrangle
        func ([type]): function to trigger on the data; 
            either 'process_song_file' or 'process_log_file'.
    """    
    
    # Create empty list to hold all filenames with their absolute path
    # that exist in a specific directory
    all_files = []
    
    # root : directories only from the path we specified.
    # dirs : sub-directories from root.
    # files : Prints out all files from root and directories.
    for root, dirs, files in os.walk(file_path):
        
        # We can use the function glob.glob() or glob.iglob() directly 
        # from glob module to retrieve paths recursively from inside the 
        # directories/files and subdirectories/subfiles.
        files = glob.glob(os.path.join(root,'*.json'))
        
        # loop over the files
        for f in files :
            
            # Append the absolute path of each file to the all_files list
            all_files.append(os.path.abspath(f))

    # Get the total number of files found in the path we specified
    num_files = len(all_files)
    
    # Print the number of total files that were found in the path
    print('{} files found in {}'.format(num_files, file_path))

    # Loop over all the files and execute the process
    # We used enumerate so that we can count the number of files that have
    # processed. 
    # Often, when dealing with iterators, we also get a need to keep a count 
    # of iterations. Python eases the programmers’ task by providing a 
    # built-in function enumerate() for this task. 
    # Enumerate() method adds a counter to an iterable and returns it in a 
    # form of enumerating object. This enumerated object can then be used 
    # directly for loops or converted into a list of tuples using the list() 
    # method.
    for i, datafile in enumerate(all_files, 1):
        
        # Execute the process on the current file
        func(cursor, datafile)
        
        # Commit the action
        connection.commit()
        
        # Print the progress of files processing
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """The main function
    """
    
    # Set connection string variables
    hostname = "127.0.0.1"
    database_name = "sparkifydb"
    username = "postgres"
    password = "123456"
        
    # Create a connection to Sparkify database
    connection = postgres.connect(
        "host={} dbname={} user={} password={}".format(
            hostname, database_name, username, password
        )
    )

    # Create a cursor to execute queries on Sparkify DB
    cursor = connection.cursor()

    # Process the data inside the songs dataset
    process_data(cursor, connection, file_path='data/song_data', 
                 func=process_song_file)
    
    # Process the data inside the logs dataset
    process_data(cursor, connection, file_path='data/log_data', 
                 func=process_log_file)

    # Close the database connection
    connection.close()


# Execute the main function
if __name__ == "__main__":
    main()