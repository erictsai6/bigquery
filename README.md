Extract Solr Data and Save it to .csv files

About
-------
This project is to extract log data from a Solr server via REST api call and save that data 
as .csv files.  These files are placed into directories filtered by day with the following syntax
YYYYMMDD.  

Configuration
--------
Open config.py and set SOLR_SERVER_HOST and SOLR_SERVER_PORT

Usage
---------
    python main.py [--begin TIME] [--end TIME]

This will read a maximum of a million log messages from begin time to end time.  TIME should be set
to UTC time in seconds.  

    python upload_bigquery.py [--shard NUM]

This process is sharded to improve performance since uploading data to Google's BigQuery takes a while from
the commandline.  
