import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creating spark session object.

    @return: SparkSession instance.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads song data, creates songs and artists tables out of it and saves them as parquet files.

    @param spark: SparkSession
    @param input_data: path to input folder in the s3 bucket
    @param output_data: output path for parquet files
    @return: None
    """
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(f"{output_data}/songs_table.parquet")

    # extract columns to create artists table
    artist_cols = ['id', 'name', 'location', 'latitude', 'longitude']
    artist_cols = ["artist_" + col for col in artist_cols]
    artists_table = df.selectExpr(*[ f"{col} as {col[7:]}" for col in artist_cols])
    artists_table = artists_table.dropDuplicates(['id'])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(f"{output_data}/artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Loads log data, creates users and time tables out of it and saves them as parquet files.
    Loads song data and joins it with log data.

    @param spark: SparkSession
    @param input_data: path to input folder in the s3 bucket
    @param output_data: output path for parquet files
    @return: None
    """
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(" page = 'NextSong' ")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", 'firstName as first_name',\
                                                            'lastName as last_name', 'gender', 'level')
    users_table = users_table.dropDuplicates(['user_id'])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data}/users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda z: datetime.fromtimestamp(z/1000), TimestampType())
    df = df.select(*df.schema.names, get_timestamp('ts').alias('timestamp'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda z: datetime.fromtimestamp(z/1000), DateType())
    df = df.select(*df.schema.names, get_datetime('ts').alias('date'))
    df = df.withColumn('month', month('timestamp'))
    
    # extract columns to create time table
    time_table = df.select('timestamp', hour('timestamp').alias('hour'), dayofmonth('timestamp').alias('day')\
                      , weekofyear('timestamp').alias('week'), month('timestamp').alias('month')\
                      , dayofweek('timestamp').alias('weekday'), year('timestamp').alias('year'))

    time_table = time_table.withColumnRenamed('timestamp', 'start_time')
    time_table = time_table.dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_data}/time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/A/A/A/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df['song'] == song_df['title'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_data}/songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)  
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
