The following lists the functional capabilities of the Twitter Spark Streaming Application:

Functional Specifications:

Scrapper:
1. Collect tweets in real-time with a specific hashtag using Twitter API
2. Filter tweets by removing emoji symbols and special characters
3. Discard any tweets that do not contain specified hashtag
4. Return tweet metadata (location) and tweet's text contents
5. Convert location metadata of each tweet back to its geolocation info using geopy
6. Send text and geolocation info to spark streaming
7. Execute infinitely while continously taking hashtags as input parameters

Sentiment Analyzer:
1. Use a third-party sentiment analyzer to perform sentiment analysis
2. Output tweet sentiment and geolocation to some external base (JSON file)

Data Visualization:
1. Save tweet data in an external JSON file
2. Perform queries on collected data using SparkSQL
