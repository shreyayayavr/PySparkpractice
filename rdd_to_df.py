from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("SparkPractice").getOrCreate()
rdd = spark.sparkContext.parallelize([('1','Shreya'),('2','Anu'),('3','Aishu')])
columns = ["id","name"]
rdd_to_df = rdd.toDF(columns)
rdd_to_df.show(1)