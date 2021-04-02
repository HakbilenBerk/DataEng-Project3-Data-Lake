import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

#parse the config file
config = configparser.ConfigParser()
config.read('dl.cfg')

#set environment variables, secret and access keys, from config file
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Create a spark session and return it
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Load song data json files from input dir restructure the data and export to output dir as parquet files.
    args:
        spark (obj)= spark session
        input_data (str)= input directory for the json files
        output_data (str)= output directory to save the parquet files
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data,"song-data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id','title','artist_id','year','duration'].dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'songs'), mode = 'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id','artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'].dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artists'),mode = 'overwrite')
    
    print("Processing song data has been completed!")


def process_log_data(spark, input_data, output_data):
    '''
    Load log data json files from input dir restructure the data and export to output dir as parquet files.
    args:
        spark (obj)= spark session
        input_data (str)= input directory for the json files
        output_data (str)= output directory to save the parquet files
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users'),mode = 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
                    col('datetime').alias('start_time'),
                    hour('datetime').alias('hour'),
                    dayofmonth('datetime').alias('day'),
                    weekofyear('datetime').alias('week'),
                    month('datetime').alias('month'),
                    year('datetime').alias('year') 
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'time'),mode = 'overwrite')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data,"song-data/A/A/A/*.json")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    df_song_log = df.join(song_df, song_df.artist_name == df.artist)
    songplays_table = df_song_log.select(
                        col('ts').alias('start_time'),
                        col('userId').alias('user_id'),
                        col('level').alias('level'),
                        col('song_id').alias('song_id'),
                        col('artist_id').alias('artist_id'),
                        col('sessionId').alias('session_id'),
                        col('location').alias('location'),
                        col('userAgent').alias('user_agent'),
                        col('year').alias('year'),
                        month('datetime').alias('month')          
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'songs'), mode = 'overwrite')
    
    print("Processing log data has been completed!")


def main():
    '''
    - Create a spark session
    - Define input and output directories (AWS s3 buckets)
    - Call the process functions to process song and log files
    '''
    #create the spark session
    spark = create_spark_session()
    
    #Determine the s3 directories
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project-data-lake-output/"
    
    #Call the process functions to process song and log data
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    print("Process has been completed!")


if __name__ == "__main__":
    main()
