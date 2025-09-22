#list1 = ["a", "b", "c", "d"]
#list2 = [1, 2, 3, 4]

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("SparkPractice").getOrCreate()
list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]

rdd = spark.sparkContext.parallelize(list(zip(list1, list2)))
columns = ["list1","list2"]
rdd_to_df = rdd.toDF(columns)
rdd_to_df.show()








