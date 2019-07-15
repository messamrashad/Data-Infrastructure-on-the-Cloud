# Data-Infrastructure-on-the-Cloud
In this project, I will apply what i have learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.


## SUMMARY:

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## PURPOSE:

The purpose of this project is to build an ETL pipeline for a database hosted on Redshift. To complete the project, I will need to load data from S3 into staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.I choose to build a DB with STAR schema consists of one fact table (SongPlays) and many dimension tables (USERS, ARTISTS, SONGS, TIME).
**This DB with such an organized schema will help the start-up Sparkify to have a DB with high scalability and very useful for analytical purposes**. The Analytics Team of Sparkify with using of simple queries can extract many insights about **songs, users' behaviour, and most important for marketing campings, are the songs users are listening to**.

#### As a Data engineer, I was more interested in building an ETL pipeline for a database hosted on Redshift and helping the technology team of the start-up Sparkify to have a robust ETL pipeline extracting data from S3 Sparkify Bucket on AWS, and insert them into staging tables, and finally, transform the data and choose specific data prior inserting into fact/dimension tables ####


## DESIGN:

1- Before inserting data into fact/dimension tables, i created two staging tables 'STG_EVENTS' &'STG_SONGS', which are tables helping into ETL process. Due to that Redshift don't enforced primary/foregin keys as they are informational only, i used SELECTING DISTINCT COLUMNS ONLY from STG tables to get rid of duplicate records.

2- The hosted database on Redshift consists of **5 tables, one fact table and four dimension tables**. The fact table "SongPlays" consists of 9 columns, which are "**songplay_id**, start_time, user_id, level, **song_id, artist_id**, session_id, location, user_agent". Songplay_id Column is a SERIAL column, and song_id and artist_id Columns are getting their values based on an inner join between ARTISTS table and Songs table with two conditions on the title of the song, name of the artist. Also, i setted 'START_TIME' as sort key in this table.

Furthermore, the four dimension tables are consisting of the following columns respectively: 

- USERS Table "users in the app" - (user_id, first_name, last_name, gender, level), i setted 'USER_ID' as sort key in this table.
- SONGS Table "songs in music database" - (song_id, title, artist_id, year, duration), i setted 'SONG_ID' as sort key in this table.
- ARTISTS Table "artists in music database" - (artist_id, name, location, latitude, longitude), i setted 'ARTIST_ID' as sort key and dist key in this table due to its small size.
- TIME Table "timestamps of records in songplays broken down into specific units" - (start_time, hour, day, week, month, year, weekday), i setted 'START_TIME' as sort key in this table.

The reason behind choosing the Star Schema over Snowflake Schema are the following:
- It's simple for users to write, and databases to process queries which are written with simple inner joins between the facts and a small number of dimensions. 
- Star joins are simpler than possible in a snowflake schema. 
- Star schemas tend to perform the best in decision-support applications.
- Fast Aggregations.

3- The ETL Pipeline design was a straightforward process. The script, etl.py, which have the code to all functions and imported varaibles, runs in the terminal. The script connects to Sparkify database which hosted on Redshift cluster, extracts and processes the **log_data** and **song_data**, and loads data into staging tables, then merging staging tables to insert unique records into the five tables.

4- Finally, Here are a description of each file in the project and how to run these files:

- sql_queries.py --> A Python script contains of all the SQL queries of DROP Table, CREATE Table and INSERT STATEMENTS.

- create_tables.py --> A Python script imports two lists (create_table_queries, drop_table_queries) from sql_queries.py. In other words, this script is responsible for connect to Sparkify DB, and to drop the tables if they are exist, and create them from scratch.

- etl.py --> A Python script responsible to imports two lists (create_table_queries, drop_table_queries) from sql_queries.py and load them into staging tables and then insert unique records into tables of STAR Schema.

- dwh.cfg --> A configuration file, contains all needed info for connecting to the hosted Redshift cluster, and the Sparkify S3 bucket which holds the data to be loaded into the new database. 

5- I used the terminal to run Python scripts in this project. To run Python scripts successfully in Shell environment:
   - Add this line in the first line of your script '#!/usr/bin/env python3'.
   - Go the directory of your files by execute this command: 'cd PATH'.
   - Execute this command to change the permissions of each script file to be executable: 'chmod a+x SCRIPT_NAME'
   - Fianlly, execute this command to run the script: './SCRIPT_NAME'


## Analytics Queries:

#### SELECT level, count(user_id) FROM users group by level;
- This query can show us how may users are free-subscription or paid-subscription users. The result of this query was **62 users with free-subscription, and only 13 paid-subscription users**. The Marketing Team should always up to gain profits more and more through subscriptions.


#### SELECT gender, count(user_id) FROM users group by gender;
- This query shows us the percentage of gender regarding our unique users we have on Sparkify App. the result of the query was 32 Male users, with 43 Female users.
