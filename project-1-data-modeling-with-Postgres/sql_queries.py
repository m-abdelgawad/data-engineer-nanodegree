######################## DROP TABLES Queries ###########################

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

######################### CREATE TABLES Queries #########################
 
# >>> Create The Fact Table <<<

# Songplays Table

# PostgreSQL has a special kind of database object generator called SERIAL. 
# It is used to generate a sequence of integers which are often used as the 
# Primary key of a table.
# SERIAL: storage size = 4 bytes, Range = 1 to 2, 147, 483, 647
# The timestamp datatype allows you to store both date and time. 
# However, it does not have any time zone data.
songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INTEGER,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    );
"""

# >>> Create Dimension Tables <<<

# Users Table

# In Postgres, CHAR(1) → where n is a positive integer.
# n → defines the length of the string. 
# The length is fixed and indicates the number of characters 
# declared when a table is created.
users_table_create = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR    
    );
"""

# Songs Table

songs_table_create = """
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INTEGER,
        duration DECIMAL    
    );
"""

# Artists Table

# Difference between INT and DECIMAL: 
# If you need numbers with decimals, use decimal (or numeric). 
# if you need numbers without decimals, use integer or bigint. 
# A typical use of decimal as a column type would be a "product price" 
# column or an "interest rate". A typical use of an integer type would 
# be e.g. a column that stores how many products were ordered 
# (assuming you can't order "half" a product).

artists_table_create = """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL    
    );
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER    
    );
"""

############################ INSERT RECORDS Queries #########################

# NOTE THAT: we must not add 'songplay_id' column in the INSERT query, nor  
# make a placeholder for it; because the columns type is SERIAL, 
# and is generated automatically
songplay_table_insert = """
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# if there is a conflict on the user ID, we only expect the updated data
# to have update only in the 'level' column
# so, in this case, we only update the 'level' column from the new record
users_table_insert = """
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE
        SET level = EXCLUDED.level
"""

songs_table_insert = """
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration 
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING;
"""

artists_table_insert = """
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING;
"""

time_table_insert = """
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

############################# FIND SONGS Query ###########################

song_select = """
    SELECT s.song_id, a.artist_id
    FROM songs s
    JOIN artists a
    ON s.artist_id = a.artist_id
    WHERE s.title = %s AND a.name = %s AND s.duration = %s 
"""

############################ QUERY LISTS ###########################

create_table_queries = [songplay_table_create, users_table_create, 
                        songs_table_create, artists_table_create, 
                        time_table_create]

drop_table_queries = [songplay_table_drop, users_table_drop, songs_table_drop,
                      artists_table_drop, time_table_drop]