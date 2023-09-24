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

from  Initializespark import Sparksetup

obj=Sparksetup()
spark=obj.config()
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType,TimestampType,DateType
schema= ''' StructType([
StructField('Symbol',StringType(),False),
StructField('Series',StringType(),False),
StructField('Date',DateType(),False),
StructField('Prev_Close',DecimalType(),False),
StructField('Open_Price',DecimalType(),False),
StructField('High_Price',DecimalType(),False),
StructField('Low_Price',DecimalType(),False),
StructField('Last_Price',DecimalType(),False),
StructField('Close_Price',DecimalType(),False),
StructField('Average_Price',DecimalType(),False),
StructField('Total_Traded_Quantity',IntegerType(),False),
StructField('Turnover',IntegerType(),False),
StructField('No_of_Trades',IntegerType(),False),
StructField('Deliverable_Qty',IntegerType(),False),
StructField('Percentage_DlyQt_to_TradedQty',DecimalType(),False)
]) '''
df=spark.read.format('csv').option("header",True).option('sep',',').option('inferschema',True).schema(eval(schema)).load("D:\Databricks\SRC\Delivery_data/09-06-2023-TO-09-09-2023-RAJESHEXPO-ALL-N.csv")

df.show()

# df=spark.read\
#     .format("jdbc")\
#     .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
#     .option("url", "jdbc:sqlserver://DESKTOP-0A2HT13;databaseName=Databricks;")\
#     .option("dbtable", "dbo.infy")\
#     .option("user", "prajwal")\
#     .option("password", 456)\
#     .load()

# for col in df.columns:
#     print(f"(StructField('{col}',StringType(),False)")
