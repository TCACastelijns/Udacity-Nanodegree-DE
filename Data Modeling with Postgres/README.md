# Project: Data Modeling with Postgres


## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Data model: Star Schema optimized for queries on song play analysis
I created a star schema with the following tables:

- Fact Table: songplays - records in log data associated with song plays i.e. records with page NextSong
    - start_time
    - user_id
    - level
    - song_id
    - artist_id
    - session_id
    - location
    - user_agent  
    
- Dimension Tables
    - users - users in the app
        - user_id
        - first_name
        - last_name
        - gender
        - level
    - songs - songs in music database
        - song_id
        - title
        - artist_id
        - year
        - duration
    - artists - artists in music database
        - artist_id
        - name
        - location
        - latitude
        - longitude
    - time - timestamps of records in songplays broken down into specific units
        - start_time
        - hour
        - day
        - week
        - month
        - year
        - weekday
        
## Code structure
The three main scripts are:

1. `sql_squeries.py`: contains all sql queries, and is imported into the files below. It contains basic queries to create the tables, the attributes and their datatypes.
2. `create_tables.py`: This script contains the functions to create the Sparkify database, drop existing tables and create tables from scratch based on the basic queries above. 
3. `etl.py`: Reads and processes files from `song_data` and `log_data` folders and loads them into the created tables from previous script. The resulting tables can be easily queried by the Sparkify analytics team.

        
## How to use the resulting tables?

```python

## General setup
import psycopg2
import pandas as pd
conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")


# Show the artists and songs in the songplays fact in Sparkify database
songplay_df = pd.read_sql("SELECT * FROM songplays WHERE song_id IS NOT NULL AND artist_id IS NOT NULL", conn)
songplay_df
```

| start_time          | user_id | level | song_id            | artist_id          | session_id | location                           | user_agent                                                                                                                                |
|---------------------|---------|-------|--------------------|--------------------|------------|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| 2018-11-21 21:56:48 | 15      | paid  | SOZCTXZ12AB0182364 | AR5KOSW1187FB35FF4 | 818        | Chicago-Naperville-Elgin, IL-IN-WI | "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36" |


```python
# Show the artists and songs in the songplays fact in Sparkify database
count_played_songs = pd.read_sql("SELECT count(*), song_id FROM songplays GROUP BY song_id", conn)
count_played_songs
```

|   | count | song_id            |
|---|-------|--------------------|
| 0 | 6819  | None               |
| 1 | 1     | SOZCTXZ12AB0182364 |