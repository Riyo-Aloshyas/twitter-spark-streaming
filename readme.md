This project deals with real-time streaming data arriving from Twitter streams.

Flow of the app:
Scrapper ----> Sentiment Analyzer ----> Data Visualization

Scrapper: collects tweets, preprocesses them for analytics
Sentiment Analyzer: determines whether the tweet is positive, negative or neutral
Data Visualization: visualizes real-time collected tweet data

Instructions for running:

1. python3 stream.py

2. python3 spark.py

3. use following command on tweets.json
	cat tweets.json | tr -d " \t\n\r"
***** given json file has already been removed of whitespace *****

4. upload tweets.json to databricks

5. perform any query using Spark SQL
