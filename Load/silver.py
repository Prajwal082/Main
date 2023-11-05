import sys
sys.path.append("d:\\Databricks\\Vsprojects\\")
from config.Initializespark import Sparksetup
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import logging
Log_format='[%(asctime)s] : [%(lineno)s] : [%(levelname)s] : %(message)s'
level = logging.INFO
logging.basicConfig(level=level,format=Log_format)

class Silvertransformation(Sparksetup):

    Logger=logging.getLogger()  
    logging.getLogger("py4j").setLevel(logging.INFO)
    Logger.info("Process started \n")

    def __init__(self) -> None:
        self.Logger.info("Setting up Spark...!\n")
        self.spark =super().create_spark()
        self.Logger.info("Spark setup Done...!\n")


    def readsql_tables(self) -> DataFrame:
        self.Logger.info(f"Reading table.. {self.table_name}")
        try:
            df = self.spark.read\
                .format("jdbc")\
                .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                .option("url", "jdbc:sqlserver://DESKTOP-0A2HT13;databaseName=Databricks;")\
                .option("dbtable", self.table_name)\
                .option("user", "prajwal")\
                .option("password", 789)\
                .load()
            return df
        except Exception as e:
            self.Logger.info(e)

    
    def transform_tables(self,table_name:str) -> DataFrame:

        self.table_name = table_name

        df=self.readsql_tables()

        self.Logger.info("Transforming Data..")

        self.df_new = df.withColumn("Day",f.split(col("Date"),'\-').getItem(0))\
                   .withColumn("Month",f.split(col("Date"),'\-').getItem(1))\
                   .withColumn("Year",f.split(col("Date"),'\-').getItem(2))
        
        self.df_new = self.df_new.withColumn("Monthnumber",date_format(to_date(col('Month'), 'MMM'), 'MM'))\
               .withColumn("date_int",concat("Year","Monthnumber","Day"))\
               .withColumn("Script_ID",concat("Symbol","Day"))\
               .withColumn("Percentage_DlyQt_to_TradedQty",concat("Percentage_DlyQt_to_TradedQty",f.lit('%')))\
               .withColumnRenamed('Symbol','Scriptname')\
               .drop("Year","Monthnumber","Month","Day","Series","Date")
        
        self.df_new = self.df_new.select([self.df_new.columns[-1]] + self.df_new.columns[:1] + [self.df_new.columns[-2]] + self.df_new.columns[2:13]) 

        self.writeto_db()
    
    def writeto_db(self):
        self.Logger.info(f"Writing Dataframe to SQL table for: {self.table_name}")
        try:
            self.df_new.write\
            .format("jdbc")\
            .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .option("url", "jdbc:sqlserver://DESKTOP-0A2HT13;databaseName=silver;")\
            .option("dbtable", self.table_name)\
            .option("user", "prajwal")\
            .option("password", 789)\
            .mode('overwrite')\
            .save()
           
            self.Logger.info(f"Data inserted to silver SSMS table for {self.table_name}\n")
        except Exception as e:
            self.Logger.info(e)


obj=Silvertransformation()
lst =['dbo.RAJESHEXPO','dbo.INFY','dbo.ATGL','dbo.JPPOWER','dbo.AMARAJABAT','dbo.HDFCBANK','dbo.SURYODAY']
for item in lst:
    obj.transform_tables(item)


