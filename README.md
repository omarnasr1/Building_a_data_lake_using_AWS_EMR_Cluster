## building an data lake on AWS for `Sparkify`.
I have 2 types of `JSON files` resides in S3 the first one has data about the `song` and the `artist` and the second one has data about the `sessions` itself, I will use those 2 files to create a data lake whiche contains 5 tables 
#### Project Repository files:
- dl.cfg
contains aws credentials.
- etl.py 
designing efficient data model and moving the data from `JSON` files to analytical `parquet` files in s3 


#### ETL Process
I have 2 directory 
- log_data it contains log files in JSON format describes the action happened in the sessions, in the page NextSong you can find some data about the song played by the user 
- song_data it contains log files in JSON format have specific data about the song
- performing read from them to create data frame to each analytical table then save it as parquet file 

#### Database design

###### Fact Table

- songplays - records in log data associated with song plays i.e. records with page NextSong 
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

###### Dimension Tables

- users - users in the app
        user_id, first_name, last_name, gender, level
- ongs - songs in music database
        song_id, title, artist_id, year, duration
- artists - artists in music database
        artist_id, name, location, latitude, longitude
- time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday
