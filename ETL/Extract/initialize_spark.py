from pyspark.sql import SparkSession

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