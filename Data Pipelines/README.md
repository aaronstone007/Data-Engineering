## Requirements
===============

1. airflow

## Project Brief
================

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow Data pipelines should be dynamic and built from reusable tasks, can be monitored, and allow easy backfills.

* Creating Airflow tasks to :
1. Read data from S3 data store.
2. Copy S3 data to Redshift table replicating a data warehouse.
3. Fetch records from these warehouse to appropriate tables.

## Dataset Justification
========================

To find what songs users are listening to, we need songs that sparkify hosts, and the event data of user interaction on the app. With this we could at anytime at a specified timestamp find what any user is listening to. The datasets reside in Amazon S3 Bucket.

Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

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

## Project Steps
================
### Assumptions:
1. Tables are created already on Redshift (Use create_tables.sql to create tables on Redshift).
2. Airflow is installed.

### Steps to run:
1. Run /opt/airflow/start.sh to get airflow started.
2. Add airflow connections:
===========================
* AWS connection:
Conn Id: Enter aws_credentials.
Conn Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

* Redshift connection:
Conn Id: Enter redshift.
Conn Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
Schema: Enter dev. This is the Redshift database you want to connect to.
Login: Enter awsuser.
Password: Enter the password you created when launching your Redshift cluster.
Port: Enter 5439.

3. Turn udac_example_dag on.
4. Check graphical view of dag is as below:
![dag](dag.png)
5. Run dag.