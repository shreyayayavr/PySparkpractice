from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
spark = SparkSession.builder.master("local").appName("SparkPractice").getOrCreate()

schema = StructType([StructField("id",IntegerType(),True),
                     StructField("name",StringType(),True),
                     StructField("count",IntegerType(),True)])
df_with_schema = spark.createDataFrame([(1,'A',10),(2,'B',20),(3,'C',30)],schema)
df_with_schema.show()
