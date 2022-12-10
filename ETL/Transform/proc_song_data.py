from ETL.Extract.extract_data import extract_song_dt
from pyspark.sql.functions import col

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
    songs_tb.write.partitionBy('year','artist_id').parquet(song_dt_path)