from ETL.Extract.extract_data import extract_log_dt, extract_song_dt
from pyspark.sql.functions import col, udf, year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType
from datetime import datetime

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