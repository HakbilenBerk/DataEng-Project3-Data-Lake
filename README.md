# Data Lake

Project by Berk Hakbilen

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We will build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow the analytics team to continue finding insights in what songs their users are listening to.

## Overview
In this project,we will build an ETL pipeline for a data lake hosted on S3. We will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. We'll deploy this Spark process on a cluster using AWS.

## Datasets
We'll be working with two datasets that reside in S3. Here are the S3 links for each:
<ul>
<li>Song data: s3://udacity-dend/song_data</li>
<li>Log data: s3://udacity-dend/log_data</li>
</ul>


### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

**Content of a sample file:**
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset we'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

**Content of a sample file:**
{"artist": null, "auth": "Logged In", "firstName": "Walter", "gender": "M", "itemInSession": 0, "lastName": "Frye", "length": null, "level": "free", "location": "San Francisco-Oakland-Hayward, CA", "method": "GET","page": "Home", "registration": 1540919166796.0, "sessionId": 38, "song": null, "status": 200, "ts": 1541105830796, "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", "userId": "39"}

## Schema for Song Play Analysis
Using the song and log datasets, we'll create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
> songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


### Dimension Tables
users - users in the app
> user_id, first_name, last_name, gender, level

songs - songs in music database
> song_id, title, artist_id, year, duration

artists - artists in music database
> artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units
> start_time, hour, day, week, month, year, weekday

## Project Files

etl.py -> reads data from S3, processes that data using Spark, and writes them back to S3
dl.cfg -> contains your AWS credentials

## How to run
Set up the dl.cfg file with your amazon access and secret keys. Create an S3 bucket to save the output data.

to run the ETL:
<code>python etl.py</code> 
