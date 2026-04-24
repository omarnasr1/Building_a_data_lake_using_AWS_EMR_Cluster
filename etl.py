import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["a"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["a"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    creating spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.6.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    processing song data to creat songs_table and artists_table
    """
    # get filepath to song data file
    song_data =  input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_output_path = output_data + "songs"
    songs_table.write\
                .mode('overwrite')\
                .partitionBy("year", "artist_id")\
                .parquet(songs_output_path) 

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])

    
    # write artists table to parquet files
    artists_output_path = output_data + "artists"
    artists_table.write\
            .mode('overwrite')\
            .parquet(artists_output_path)


def process_log_data(spark, input_data, output_data):
    """
    processing log data to create the rest of the tables
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df =  spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"])
    users_table = users_table.drop_duplicates(subset=['userId'])
    # write users table to parquet files
    users_output_path = output_data + "users"
    users_table.write\
            .mode('overwrite')\
            .parquet(users_output_path)

    # create timestamp column from original timestamp column
    get_datetime = f.udf(lambda ts: datetime.fromtimestamp(ts/1000), TimestampType())
    df = df.withColumn("start_time", get_datetime("ts"))
    
    
    # extract columns to create time table
    time_table = df.select(["start_time"])
    time_table = time_table.withColumn("hour", f.hour("start_time"))
    time_table = time_table.withColumn("day", f.dayofmonth("start_time"))
    time_table = time_table.withColumn("week", f.weekofyear("start_time"))
    time_table = time_table.withColumn("month", f.month("start_time"))
    time_table = time_table.withColumn("year", f.year("start_time"))
    time_table = time_table.withColumn("weekday", f.dayofweek("start_time")) 
    
    # write time table to parquet files partitioned by year and month
    time_output_path = output_data + "time"
    time_table.write\
           .mode('overwrite')\
           .partitionBy("year", "month")\
           .parquet(time_output_path)

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)
    songplays_join_table = song_df.join(df, (song_df.artist_name == df.artist) & (song_df.title == df.song) & (song_df.duration == df.length) , how= "inner")
    songplays_join_table = songplays_join_table.withColumn("songplay_id", monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_join_table.select(["songplay_id", "start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"])

    # write songplays table to parquet files partitioned by year and month
    songplays_output_path = output_data + "songplays"
    songplays_table.write\
           .mode('overwrite')\
           .partitionBy("year", "month")\
           .parquet(songplays_output_path)


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://udacity-dend/analysis"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
