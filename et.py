#!/usr/bin/env python3

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function is responsible for loading records in STG tables listed in 'copy_table_queries' list from JSON files in S3 bucket. the mentioned list has         been imported from sql_queries.py script. In other words, this function gets the records from STG tables and insert them for their specified tables.
    
    parameters: 
        cur: The available cursor.
        connection: Connection to Redshift cluster with Psycopg2 library 
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function is responsible for inserting records in tables listed in 'insert_table_queries' list, which has been imported from sql_queries.py script.
    In other words, this function gets the records from STG tables and insert them for their specified tables.
    
    parameters: 
        cur: The available cursor.
        connection: Connection to Redshift cluster with Psycopg2 library 
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This Main procedure create a connection to RedShift Cluster with Psycopg2 library using the credentials mentioned in the dwh.cfg file, and create the 
    cursor. Finally, it calls 'load_staging_tables' and 'insert_tables' functions with specified arguments.
    
    parameters: 
        No parameters for this function.
    Returns:
        None
    """
    config = configparser.ConfigParser()
    #Import configuration in "dwh.cfg" file.
    config.read('dwh.cfg')
    
    #Get values of CLUSTER in dwh.cfg file
    DWH_HOST= config.get("CLUSTER","HOST")
    DWH_DB_NAME= config.get("CLUSTER","DB_NAME")
    DWH_DB_USER= config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
    DWH_DB_PORT = config.get("CLUSTER","DB_PORT")
    
    #Connect to Redshift cluster with Psycopg2 Library
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Call the mentioned functions to be executed
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
