import sys
sys.path.append("d:\\Databricks\\Vsprojects\\")
from config.Initializespark import Sparksetup
obj=Sparksetup()
spark=obj.create_spark()

from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType,TimestampType,DateType
lst =['dbo.RAJESHEXPO','dbo.INFY','dbo.ATGL','dbo.JPPOWER','dbo.AMARAJABAT','dbo.HDFCBANK','dbo.SURYODAY']
for item in lst:
    df = spark.read\
            .format("jdbc")\
            .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .option("url", "jdbc:sqlserver://DESKTOP-0A2HT13;databaseName=silver;")\
            .option("dbtable",item)\
            .option("user", "prajwal")\
            .option("password", 789)\
            .load()

    df.write\
        .format("jdbc")\
        .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("url", "jdbc:sqlserver://DESKTOP-0A2HT13;databaseName=gold;")\
        .option("dbtable", "Stockdata")\
        .option("user", "prajwal")\
        .option("password", 789)\
        .mode('append')\
        .save()

