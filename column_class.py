import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 🔹 Force Spark to use your virtual environment's Python
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Shreya\PycharmProjects\PySparkpractice\.venv311\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Shreya\PycharmProjects\PySparkpractice\.venv311\Scripts\python.exe"

# Start Spark session
spark = SparkSession.builder.master("local").appName("SparkPractice").getOrCreate()

# Create RDD
rdd = spark.sparkContext.parallelize([
    ('1', 'Shreya'),
    ('2', 'Anu'),
    ('3', 'Aishu')
])

# Convert to DataFrame
columns = ["id", "name"]
rdd_to_df = rdd.toDF(columns)

# Add uppercase column
df_with_column = rdd_to_df.withColumn("upper_name", F.upper(rdd_to_df['name']))

# Show result
df_with_column.show()

