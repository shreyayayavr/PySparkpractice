from pyspark.sql import SparkSession
import pandas as pd
spark = SparkSession.builder.master("local").appName("SparkPractice").getOrCreate()
df = spark.createDataFrame([],"id INT, name STRING, count INT")
new_data = [(1,'a',10),(2,'b',20),(3,'c',30)]
new_df = spark.createDataFrame(new_data,["id","name","count"])
df = df.union(new_df)
pandas = df.toPandas()
print(pandas)