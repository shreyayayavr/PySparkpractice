from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local").appName("SparkPractice").getOrCreate()

# Create an RDD of tuples
rdd = spark.sparkContext.parallelize([(1,), (2,), (3,), (4,), (5,)])
columns = ["num"]

# Convert to DataFrame
rdd_to_df = rdd.toDF(columns)

# Add a new column with square
df_with_square = rdd_to_df.withColumn("num_square", col("num") * col("num"))

df_with_square.show()

df_renamed_df = df_with_square.withColumnRenamed("num_square", "squared")

df_renamed_df.show()