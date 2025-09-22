from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkPractice") \
    .config("spark.python.worker.faulthandler.enabled", "true") \
    .getOrCreate()


list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]

rdd_A = spark.sparkContext.parallelize(list_A)
rdd_B = spark.sparkContext.parallelize(list_B)


rdd_sub = rdd_A.subtract(rdd_B)
rdd_result = rdd_sub.collect()
print(rdd_result)





