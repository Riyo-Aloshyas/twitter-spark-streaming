# Databricks notebook source
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext, functions

# sc.stop()
sc=SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

df = sqlContext.read.json("/FileStore/tables/tweets.json")
df.show()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select("country").show()

# COMMAND ----------

df.select(df['country']).where(df.sentiment == "negative").show()
df.select(df['country']).where(df.sentiment == "neutral").show()
df.select(df['country']).where(df.sentiment == "positive").show()

# COMMAND ----------

df.groupBy("sentiment").count().show()

# COMMAND ----------

df.groupBy("country").count().show()
df.groupBy("state").count().show()

# COMMAND ----------

df.createOrReplaceTempView("tweet")

sqlDF = spark.sql("SELECT country as Country, sentiment FROM tweet")
spark.sql("SELECT country as Country FROM tweet GROUP BY country").show()

sqlDF.show()

# COMMAND ----------

#show sentiment for countries
df.groupBy("country").agg(functions.count('sentiment')).show()

#show sentiment for state
df.groupBy("state").agg(functions.count('sentiment')).show()



df.groupBy("country", "state").agg(functions.count('sentiment')).show()

# COMMAND ----------

#Trump
df.groupBy("tweet").agg(functions.count('tweet')).filter(df.tweet.contains("Trump")).show()

df.groupBy("tweet").agg(functions.count('tweet')).filter(df.tweet.contains("fakenews")).show()
