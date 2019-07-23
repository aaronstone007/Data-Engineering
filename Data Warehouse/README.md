## Requirements
===============

1. pandas
2. psycopg2
3. sql_queries

## Project Brief
================

An imaginary music streaming company Sparkify is interested in understanding what songs users are listening to, they have their datasets on S3 json activity logs and songs collection. Future analytics may require a data warehouse so we are required to use Amazon Redshift for stagging table and analytics tables.

## Dataset Justification
========================

To find what songs users are listening to, we need songs that sparkify hosts, and the event data of user interaction on the app. With this we could at anytime at a specified timestamp find what any user is listening to.

1. Log data: s3://udacity-dend/log_data
2. Song data: s3://udacity-dend/song_data

## Process Justification
========================
1. Since data warehouse is required we dump all the data from data sources into appropriate staging tables in redshift.
2. Now from those staging tables we extract our analytical tables.

## Schema Justification
=======================

To achieve our analytical goal we need multiple dimension table with information like users,songs,artists,time and a fact table to merge contents of those tables. This in essence is a star schema.

1. Fact Table

    songplays - Records in log data associated with song plays

            songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

2. Dimension Tables

    users - Users in the app

            user_id, first_name, last_name, gender, level

    songs - songs in music database

            song_id, title, artist_id, year, duration

    artists - artists in music database

            artist_id, name, location, lattitude, longitude

    time - timestamps of records in songplays broken down into specific units

            start_time, hour, day, week, month, year, weekday
            
## Phase
========
Phase involved in this project:
1. Staging Phase
2. Analytics Phase


## Staging Phase
================
Staging involves creating stagging tables:

1. staging_events:
    1. Contains all the columns from log data

2. staging_songs:
    1. Contains all the columns from song data
    

## Analytics Phase
==================

1. Fact Table

    songplays - Contains Records in log data associated with song plays

            songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

2. Dimension Tables

    users - Contains Users in the app

            user_id, first_name, last_name, gender, level

    songs - Contains Songs in music database

            song_id, title, artist_id, year, duration

    artists - Contains Artists in music database

            artist_id, name, location, lattitude, longitude

    time - Contains Timestamps of records in songplays broken down into specific units

            start_time, hour, day, week, month, year, weekday

## Project Steps
================
### To run this project:

1. Fill in HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT under CLUSTER and ARN under IAM_ROLE in dwh.cfg
2. Run create_tables.py to create your database and tables and drop if it is already present
3. Run etl.py to extract and insert the data into appropriate tables

