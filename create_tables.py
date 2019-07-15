#!/usr/bin/env python3

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function is responsible for checking on tables if they exist, then it will drop them. All tables have been listed in 'drop_table_queries' which has
    been imported from sql_queries.py script.
    It will drop STG, fact and dimension tables.
    
    parameters: 
        cur: The available cursor.
        connection: Connection to Redshift cluster with Psycopg2 library 
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function is responsible for creating tables, which have been listed in 'create_table_queries' which has been imported from sql_queries.py script.
    It will create STG, fact and dimension tables.
    
    parameters: 
        cur: The available cursor.
        connection: Connection to Redshift cluster with Psycopg2 library 
    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This Main procedure create a connection to RedShift Cluster with Psycopg2 library using the credentials mentioned in the dwh.cfg file, and create the curs
    cursor. Finally, it calls 'drop_tables' and 'create_tables' functions with specified arguments.
    
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
    
    #Connect to Redshift cluster.
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Drop/Create SQL statements to be done on DB.
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()