from pyspark.sql import functions as SF

def multiply_by_n(df, col, n):
  df = df.withColumn(col, SF.col(col)*n)
  return df