import findspark
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear, hour
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType

#Initialize Spark Session and name as Sparkify
def spark_session():
    spark = SparkSession\
        .builder\
            .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2")\
                .config("spark.jars.excludes","com.google.guava:guava")\
                    .config("spark.hadoop.fs.s3.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
                        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
                            .appName("Sparkify")\
                                .getOrCreate()
    return spark

#Define Schema
def song_schema():
    song_schema = StructType(
        [
            StructField("num_songs",IntegerType()),
            StructField("artist_id",IntegerType()),
            StructField("artist_latitude", DoubleType()),
            StructField("artist_longitude", DoubleType()),
            StructField("artist_location", StringType()),
            StructField("artist_name", StringType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("duration", DoubleType()),
            StructField("year", IntegerType())
        ]
    )
    return song_schema

#Extract Song Data and Insert into Dataframe
def extract_song_dt(spark, input):
    song_data = input + 'song-data/song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data, song_schema())
    return song_df

#Process Song Data Columns from Dataframe and write to Parquet file
def proc_song_data(spark, input, output):

    df = extract_song_dt(spark, input)

    songs_tb = df.select(
        [
            'song_id',
            'title',
            'artist_id',
            'year',
            'duration'
        ]
    )\
        .distinct()\
            .where(
                col('song_id').isNotNull()
            )
    song_dt_path = output + 'songs'
    songs_tb.write.mode('overwrite').partitionBy('year','artist_id').parquet(song_dt_path)

#Process Artist Data Columns from Dataframe and write to Parquet file
def proc_artist_data(spark, input, output):
    df = extract_song_dt(spark, input)
    
    artist_tb = df.select(
        [
            'artist_id',
            'artist_name',
            'artist_location',
            'artist_latitude',
            'artist_longitude'
        ]
    )\
        .distinct()\
            .where(
                col('artist_id').isNotNull()
            )

    artist_dt_path = output + 'artists'
    artist_tb.write.mode('overwrite').parquet(artist_dt_path)

def extract_log_dt(spark, input):
    log_data = input + 'log-data/*.json'
    log_df = spark.read.json(log_data)
    return log_df

def proc_log_data(spark, input, output):
    df = extract_log_dt(spark, input)
    
    df = df.filter(df.page == 'NextSong')

    users_tb = df.select(
        [
            'userId',
            'firstName',
            'lastName',
            'gender',
            'level'
        ]
    )\
        .distinct()\
            .where(col('userId').isNotNull()
            )
    users_path = output + 'users'
    users_tb.write.mode('overwrite').parquet(users_path)

    def format_datetime(ts):
        return datetime.fromtimestamp(ts/100.0)
    
    get_ts = udf(lambda x: format_datetime(int(x)), TimestampType())
    df = df.withColumn("start_time", get_ts(df.ts))

    get_dt = udf(lambda x: format_datetime(int(x)), DateType())
    df = df.withColumn("datetime", get_dt(df.ts))

    time_tb = df.select(
        'ts',
        'start_time',
        'datetime',
        hour("datetime").alias('hour'),
        dayofmonth("datetime").alias('day'),
        weekofyear("datetime").alias('week'),
        year('datetime').alias('year'),
        month("datetime").alias('month'),
        dayofweek("datetime").alias('weekday')
    )\
        .dropDuplicates()
    
    time_tb_path = output + 'time'
    time_tb.write.mode('overwrite').partitionBy('year','month').parquet(time_tb_path)

    songs_df = extract_song_dt(spark, input)

    df = df.drop_duplicates(subset=['start_time'])

    songplays_tb = songs_df.alias('s').join(df.alias('l'), (songs_df.title == df.song) & (songs_df.artist_name == df.artist))\
        .where(df.page == 'NextSong')\
            .select(
                [
                    col('l.start_time'),
                    year("l.datetime").alias('year'),
                    month("l.datetime").alias('month'),
                    col('l.userId'),
                    col('l.level'),
                    col('s.song_id'),
                    col('s.artist_id'),
                    col('l.sessionID'),
                    col('l.location'),
                    col('l.userAgent')
                ]
            )
    
    songplays_path = output + 'songplays'
    songplays_tb.write.mode('overwrite').partitionBy('year','month').parquet(songplays_path)

def main():
    findspark.init()
    spark = spark_session()
    input = "s3://sparkifyetl/input/"
    output = "s3://sparkifyetl/output/"

    proc_song_data(spark, input, output)
    proc_artist_data(spark, input, output)
    proc_log_data(spark, input, output)

if __name__ == "__main__":
    main()