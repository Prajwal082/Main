# import os,sys
# import datetime
# import pandas as pd
# # import Initializespark as sp
# # spk=sp.Sparksetup()
# # spark=spk.spark_session()

# dir= os.listdir("D:\Databricks\SRC\Delivery_data")

# for files in dir:

#     timestamp=os.path.getmtime(f"D:\Databricks\SRC\Delivery_data/{files}")
#     modifiedtime=datetime.datetime.fromtimestamp(timestamp)
#     print(files,modifiedtime)
# ct = datetime.datetime.now()
# print(ct)
# # df=spark.read.format('csv').option('header',True).option('sep',',').option('inferschema',True).load("D:\Databricks\SRC\Delivery_data/12-05-2023-TO-12-08-2023-AMARAJABAT-ALL-N.csv")
# # df.show()
import sys
sys.path.append("d:\\Databricks\\Vsprojects\\")

from config.Initializespark import Sparksetup
# from Utils.Stockutils import Utils

# obj=Sparksetup()
# spark=obj.create_spark()
url ="jdbc:sqlserver://DESKTOP-0A2HT13;databaseName=gold;"

# DriverManager = spark._sc._gateway.jvm.java.sql.DriverManager
import findspark
findspark.init()
from pyspark.sql import SparkSession
from delta import * 

# spark=SparkSession.builder.master('local[5]')\
#                 .appName('local')\
#                 .config('spark.driver.extraClassPath', "C:\spark\jars\sqljdbc_4.2\enu\jre8\sqljdbc42.jar")\
#                 .config('spark.executor.extraClassPath', "C:\spark\jars\sqljdbc_4.2\enu\jre8\sqljdbc42.jar")\
#                 .getOrCreate()
#         spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs","false")
#         spark.conf.set('spark.driver.cores',4)
#         spark.conf.set('spark.driver.executor',4)
#         spark.conf.set('spark.driver.executor',8)
#         spark.conf.set('spark.driver.memory','56g')
#         spark.conf.set('spark.executor.memory','56g')
#         spark.conf.set("spark.eventLog.enabled",True)
#         # spark.conf.set("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") 
#         # # spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
#         # spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
# 

import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate() 