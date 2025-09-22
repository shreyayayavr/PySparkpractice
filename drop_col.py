from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("Spark").getOrCreate()
rdd = spark.sparkContext.parallelize([('1','Shreya'),('2','Anu'),('3','Aishu'),('4','Supritha')])
columns = ["id","name"]
rdd_to_df = rdd.toDF(columns)
df_dropped = rdd_to_df.drop("id")
df_dropped.show()