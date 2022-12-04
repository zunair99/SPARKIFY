from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType

#Set Schema for Songs Table
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