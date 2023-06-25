import findspark
print(findspark.init('C:\spark/'))

from pyspark.sql import *


spark=SparkSession.builder.master('local').appName('local').getOrCreate()

spark.conf.set("spark,ui,port",4041)
df=spark.read.option('header',True).option('inferschema',True).csv("D:\Databricks\ind_nifty50list.csv")
df.show()



