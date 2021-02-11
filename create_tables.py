import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    try:
        # connect to default database
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb \
                                 user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' \
                    TEMPLATE template0")
    
        # close connection to default database
        conn.close() 
            
        # connect to sparkify database
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
                                 user=student password=student")
        cur = conn.cursor()
        
        #raise psycopg2.Error('this is wanted')    
    except psycopg2.Error:
        print("Error: Database creation 'sparkifydb' failed")
        raise 
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error:
            print("Error: Tables not deleted")
            raise


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error:
            print("Error: Tables not created")
            raise


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    
    success = False
    conn = None
    try:
        cur, conn = create_database()
        drop_tables(cur, conn)
        create_tables(cur, conn)
        success = True
    except psycopg2.Error as e: 
        print(e)
    finally:
        if conn is not None:
            conn.close()
        if success:
            print('Process suceeded')
        else:
            print('Process failed')


if __name__ == "__main__":
    main()