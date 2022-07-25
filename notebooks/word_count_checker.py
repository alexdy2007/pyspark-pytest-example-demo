# Databricks notebook source
from pyspark.sql import functions as SF

def do_word_counts(df):

  df_word_count = (df.withColumn('word', SF.explode(SF.split(SF.col('sentences'), ' ')))
    .groupBy('word')
    .count()
    .sort('count', ascending=False)
 )
  
  return df_word_count


# COMMAND ----------

from pyspark.sql import types as T

data =  [
    'hello spark',
    'hello again spark spark'
 ]

df = spark.createDataFrame(data, T.StringType()).toDF('sentences')

results = do_word_counts(df)


    

# COMMAND ----------

expected_results = {'hello':2, 'spark':3, 'again':1}  
actual_results = results.rdd.collectAsMap()
expected_results==actual_results
