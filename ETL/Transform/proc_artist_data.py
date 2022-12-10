from ETL.Extract.extract_data import extract_song_dt
from pyspark.sql.functions import col

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
    artist_tb.write.parquet(artist_dt_path)