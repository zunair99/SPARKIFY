from ETL.Extract.song_schema import song_schema

#Extract Song Data and Insert into Dataframe
def extract_song_dt(spark, input):
    song_data = input + 'song-data/song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data, song_schema())
    return song_df

def extract_log_dt(spark, input):
    log_data = input + 'log-data/*.json'
    log_df = spark.read.json(log_data)
    return log_df