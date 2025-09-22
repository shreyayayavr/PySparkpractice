from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("SparkPractice").getOrCreate()
empty_df = spark.createDataFrame([],"id INT, name STRING")
empty_df.show()

