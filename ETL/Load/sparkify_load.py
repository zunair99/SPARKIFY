import findspark
from ETL.Extract.initialize_spark import spark_session
from ETL.Transform.proc_artist_data import proc_artist_data
from ETL.Transform.proc_song_data import proc_song_data
from ETL.Transform.proc_log_data import proc_log_data

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