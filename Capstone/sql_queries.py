import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
pop_staging_drop = ("""drop table if exists pop_staging;""")
temp_staging_drop = ("""drop table if exists temp_staging;""")
city_table_drop = ("""drop table if exists city_table;""")
time_table_drop = ("""drop table if exists time_table;""")
main_table_drop = ("""drop table if exists main_table;""")

# CREATE TABLES
create_pop_staging = ("""create table if not exists pop_staging (Code varchar,City varchar,Population bigint,Latitude varchar,Longitude varchar,Country varchar,city_key int);
""")
create_temp_staging = ("""create table if not exists temp_staging (dt date,AverageTemperature float,AverageTemperatureUncertainty float,City varchar,Country varchar,Latitude varchar,Longitude varchar,city_key int);
""")
create_city_table = ("""create table if not exists city_table (city_key int NOT NULL PRIMARY KEY,Code varchar,City varchar,Latitude varchar,Longitude varchar,Country varchar);
""")
create_time_table = ("""create table if not exists time_table (date date NOT NULL PRIMARY KEY, day int, month int, year int);
""")
create_main_table = ("""create table if not exists main_table (date date NOT NULL PRIMARY KEY,city_key int,temperature float,population bigint);
""")

# COPY TABLES
pop_staging_copy = ("""
    copy pop_staging
    from {}
    iam_role {}
    json {}
    region 'us-west-2';
""").format(config['S3']['POP_DATA'],config['IAM_ROLE']['ARN'],config['S3']['POP_JSONPATH'])

temp_staging_copy = ("""
    copy temp_staging
    from {}
    iam_role {}
    csv
    IGNOREHEADER 1;
""").format(config['S3']['TEMP_DATA'],config['IAM_ROLE']['ARN'])

# INSERT TABLES
city_table_insert = ("""insert into city_table (city_key,Code,City,Latitude,Longitude,Country) select city_key,Code,City,Latitude,Longitude,Country from pop_staging;""")
time_table_insert = ("""insert into time_table (date, day, month, year) select distinct dt, extract (day from dt),extract (month from dt),extract (year from dt) from temp_staging;""")
main_table_insert = ("""insert into main_table (date,city_key,temperature,population) select t.dt,t.city_key,t.averagetemperature,p.population from temp_staging as t join pop_staging as p on t.city_key=p.city_key;""")

drop_table_queries = [pop_staging_drop,temp_staging_drop,city_table_drop,time_table_drop,main_table_drop]
create_table_queries = [create_pop_staging,create_temp_staging,create_city_table,create_time_table,create_main_table,create_main_table]
copy_table_queries = [pop_staging_copy,temp_staging_copy]
insert_table_queries = [city_table_insert,time_table_insert,main_table_insert]