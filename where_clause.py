from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local").appName("SparkPractice").getOrCreate()
rdd = spark.sparkContext.parallelize([(1,), (2,), (3,), (4,), (5,)])
columns = ["num"]
rdd_to_df = rdd.toDF(columns)
df_with_square = rdd_to_df.withColumn("num_square", col("num") * col("num"))
df_with_square.show()

df_filtered = df_with_square.filter(df_with_square["num_square"] > 9)
df_filtered.show()

df_filtered2 = df_with_square.where(df_with_square["num_square"] < 5)

df_filtered2.show()