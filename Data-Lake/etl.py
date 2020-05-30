import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data) -> None:
    """
    Method to process the raw songs data with Spark and write it as dimensional tables to an output location
    
    :param spark: Spark session 
    :param input_data: path to song input data in S3
    :param output_data: path to output data in S3
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    songsSchema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", StringType(), True),
        StructField("artist_longitude", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), False),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", DoubleType(), False),
        StructField("year", IntegerType(), False)
    ])
    df = spark.read.json(song_data, schema=songsSchema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/', mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select(
                'artist_id', col('artist_name').alias('name'), col('artist_location').alias('location'),
                col('artist_latitude').alias('latitude'), col('artist_longitude').alias('longitude')).distinct()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', mode='overwrite')

def process_log_data(spark, input_data, output_data) -> None:
    """
    Method to process the raw logs of the Sparkify app with Spark and write it as dimensional tables to an output location
    
    :param spark: Spark session 
    :param input_data: path to song input data in S3
    :param output_data: path to output data in S3
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*.json"
    
    # read log data file
    logsSchema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), False),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), False),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), False),
        StructField("location", StringType(), True),
        StructField("method", StringType(), False),
        StructField("page", StringType(), False),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), False),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), False),
        StructField("ts", DoubleType(), False),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    df = spark.read.json(log_data, schema=logsSchema)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'),col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),col('gender').alias('gender'),col('level').alias('level')).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/', mode='overwrite')

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0), TimestampType())    
    df = df.withColumn("start_time", get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates()\
        .withColumn("hour", hour(col("start_time"))).withColumn("day", dayofmonth(col("start_time"))) \
        .withColumn("week", weekofyear(col("start_time"))).withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))).withColumn("weekday", date_format(col("start_time"), 'E'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/', mode='overwrite')

    # read in song data to use for songplays table
    songs_table = spark.read.parquet(output_data + "songs/")
    artists_table = spark.read.parquet(output_data + "artists/")

    # extract columns from joined song and log datasets to create songplays table 
    songs = songs_table.join(artist_table, "artist_id", "full").select("song_id", "title", "artist_id", "name", "duration")

    songplays_table = df.join(songs, on =[df.song == songs.title, df.artist == songs.name, df.length == songs.duration], how='left')

    songplays_table = songplays_table.join(time_table, on="start_time", how="left")\
                            .withColumn("songplay_id", monotonically_increasing_id())\
                            .select("songplay_id", "start_time", col("userId").alias("user_id"), 
                                    "level", "song_id", "artist_id", col("sessionId").alias("session_id"), 
                                    "location", col("userAgent").alias("user_agent"), "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/',  mode='overwrite')



def main():
    """
    Method to:
    
    1. Create a Spark Session
    2. Process the song data 
    3. Process the event data
    4. Write the result into dimensional tables in S3 in parquet format
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config.get('AWS', 'OUTPUT_S3')
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
