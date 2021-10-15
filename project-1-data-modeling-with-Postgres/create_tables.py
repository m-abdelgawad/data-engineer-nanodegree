# Import postgres driver 
import psycopg2 as postgres

# Import queries lists
# create_table_queries is a list of strings; where each string is a query
# to create a table. It's purpose is to create all the tables that we need
# in Sparkify DB.
# drop_table_queries is a list of strings; where each string is a query
# to drop a table from the tables we've created. Its purpose is to drop all 
# the tables that we've created; to clean everything; as this is only an
# exercise project.
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the Sparkifydb database
    - Returns the connection and cursor to sparkifydb

    Returns:
        [Postgres cursor]: Cursor to execute queries on the Sparkify database
        [Postgres connection]: Connection to the Sparkify database.
    """    
    
    # Set connection string variables
    hostname = "127.0.0.1"
    database_name = "postgres"
    username = "postgres"
    password = "123456"
    
    # connect to default database
    connection = postgres.connect(
        "host={} dbname={} user={} password={}".format(
            hostname, database_name, username, password
        )
    )
    
    # Set auto-commit to Trues
    connection.set_session(autocommit=True)
    
    # Create a cursor
    cursor = connection.cursor()
    
    # create sparkify database with UTF8 encoding
    
    # > Drop the sparkifydb database if it already exists
    cursor.execute("DROP DATABASE IF EXISTS sparkifydb")
    # > Then recreate the database
    cursor.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' "
                   "TEMPLATE template0")

    # close connection to default database
    connection.close()    
    
    # Change database_name to Sparkify database
    database_name = "sparkifydb"
    
    # connect to sparkify database
    connection = postgres.connect(
        "host={} dbname={} user={} password={}".format(
            hostname, database_name, username, password
        )
    )
    
    # Create a new cursor
    cursor = connection.cursor()
    
    # Return the cursor and connection to Sparkify database.
    return cursor, connection


def drop_tables(cursor, connection):
    """Drops each table using the queries in `drop_table_queries` list.

    Args:
        cursor (Postgres Cursor): Cursor to execute queries on Sparkify DB
        connection (Postgres Connection): Connection to Sparkify DB.
    """    
    
    # loop over all the queries inside the list of strings: drop_table_queries
    for query in drop_table_queries:
        
        # Execute the current query and drop the current table
        cursor.execute(query)
        
        # Commit the change
        connection.commit()


def create_tables(cursor, connection):
    """Creates each table using the queries in `create_table_queries` list.

    Args:
        cursor (Postgres Cursor): Cursor to execute queries on Sparkify DB
        connection (Postgres Connection): Connection to Sparkify DB.
    """    

    # Loop over all CREATE TABLE queries; to create the tables that we need
    # in the Sparkify database
    for query in create_table_queries:
        
        # Execute the current query and create the current table
        cursor.execute(query)
        
        # Commit the action to create the table
        connection.commit()



def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
      cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    
    # Connect to the Sparkify database and get the cursor and the connection
    # variables
    cursor, connection = create_database()
    
    # Drop the needed tables if they exist; as we need to recreate them each
    # time we run this project.
    drop_tables(cursor, connection)
    
    # Create the tables we need in the Sparkify database
    create_tables(cursor, connection)

    # Close the connection with Sparkify DB. 
    connection.close()

    # Print a success message
    print("\nAll tables have been created successfully!\n")


# Execute the main function
if __name__ == "__main__":
    main()