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

obj=Sparksetup()
spark=obj.create_spark()
from pyspark.sql.functions import current_date,month
from pyspark.sql.types import *
# DriverManager = spark._sc._gateway.jvm.java.sql.DriverManager

# .withColumn("Avg_Change",df_new["Percentage_DlyQt_to_TradedQty"].sum())
