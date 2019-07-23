# %load etl.py
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType as tst

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Insert records into songs and artists tables using data from song files."""
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    songSchema = R([
        Fld("song_id",Str()),
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema)
    
    # extract columns to create songs table
    songs_table = df.select(["song_id","title", "artist_id","year", "duration"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, 'songs/'), 'overwrite')
    
    # extract columns for artist table
    artist_table = df.selectExpr(["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artist_table.write.parquet(os.path.join(output_data, 'artists/'), 'overwrite')
    
def process_log_data(spark, input_data, output_data):
    """Insert records into time, users, and songplays tables using data from log and song files."""
    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    df=df.filter(df.page=='NextSong')
    
    # extract columns for users table    
    users_table = df.selectExpr(["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]).dropDuplicates()
   
    users_table.write.parquet(os.path.join(output_data, 'users/'), 'overwrite')
    
    # create start_time column from ts column and convert it to timestamp
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates().withColumn("hour", hour(col("start_time"))).withColumn("day", dayofmonth(col("start_time"))).withColumn("week", weekofyear(col("start_time"))).withColumn("month", month(col("start_time"))).withColumn("year", year(col("start_time"))).withColumn("weekday", dayofweek(col("start_time")))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, 'time/'), 'overwrite')
    
    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song_data/*/*/*/*.json"))
    
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), 'left_outer').select(df.start_time,df.userId.alias("user_id"),df.level,song_df.song_id,song_df.artist_id,df.sessionId.alias("session_id"),df.location,df.userAgent.alias("user_agent")).withColumn("songplay_id", monotonically_increasing_id()).withColumn("year", year(col("start_time"))).withColumn("month", month(col("start_time")))
    
#     # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'), 'overwrite')
    
def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    os.environ['AWS_ACCESS_KEY_ID']=config['S3']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['S3']['AWS_SECRET_ACCESS_KEY']
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://unibic-1/"
    print("Please wait while processing")
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)    
    print("Done")

if __name__ == "__main__":
    main()