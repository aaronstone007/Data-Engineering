## Requirements
===============

1. pyspark.sql
2. configparser
3. datetime

## Project Brief
================

An imaginary music streaming company Sparkify is interested in understanding what songs users are listening to, they have their datasets on S3 json activity logs and songs collection. Their company requires ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

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
            
## ETL Justification
====================
1. Read log and song data from s3a://udacity-dend/ as dataframes
2. Select appropriate columns from log and song data and place it in appropriate dataframe
3. Write these data frame to parquet tables
            
## Project Steps
================
### To run this project:

1. Fill in AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY under IAM_ROLE in dl.cfg
2. Set output_data to location of your choice in etl.py
3. Run etl.py

