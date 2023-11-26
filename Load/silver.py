import sys
sys.path.append("d:\\Databricks\\Vsprojects\\")
from  config.Initializespark import Sparksetup
from Utils.Stockutils import Utils
from pyspark.sql.functions import *
import pyspark.sql.functions as f

class Silvertransformation(Utils):

    def __init__(self) -> None:
        super().__init__()
        
        self.Logger.info("Setting up Spark...!\n")
        self.spark = Sparksetup.create_spark(self)
        self.Logger.info("Spark setup Done...!\n")

        self._initconfigs()
        
    def _initconfigs(self):
        self.driver = self.get_leafconfig(self.config_data,"ssms.cred.driver")
        self.user = self.get_leafconfig(self.config_data,"ssms.cred.user")
        self.password = self.get_leafconfig(self.config_data,"ssms.cred.password")

    @property
    def url(self):
        return self.get_leafconfig(self.config_data,"ssms.cred.url")

    def readsql_tables(self) -> DataFrame:
        url = self.url.format(Database= "Databricks")
        self.Logger.info(f"Reading table.. {self.table_name}")
        try:
            df = self.spark.read\
                .format("jdbc")\
                .option("driver",self.driver)\
                .option("url", url)\
                .option("dbtable", self.table_name)\
                .option("user", self.user)\
                .option("password", self.password)\
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
               .withColumn("Script_ID",concat("Symbol","Monthnumber","Day"))\
               .withColumn("Percentage_DlyQt_to_TradedQty",concat("Percentage_DlyQt_to_TradedQty",f.lit('%')))\
               .withColumnRenamed('Symbol','Scriptname')\
               .drop("Year","Monthnumber","Month","Day","Series","Date")
        
        self.df_new = self.df_new.select([self.df_new.columns[-1]] + self.df_new.columns[:1] + [self.df_new.columns[-2]] + self.df_new.columns[2:13]) 

        self.writeto_db()
    
    def writeto_db(self):
        url = self.url.format(Database = "silver")
        self.Logger.info(f"Writing Dataframe to SQL table for: {self.table_name}")
        try:
            self.df_new.write\
            .format("jdbc")\
            .option("driver",self.driver)\
            .option("url", url)\
            .option("dbtable", self.table_name)\
            .option("user", self.user)\
            .option("password", self.password)\
            .mode('overwrite')\
            .save()
           
            self.Logger.info(f"Data inserted to silver SSMS table for {self.table_name}\n")
        except Exception as e:
            self.Logger.info(e)


obj=Silvertransformation()
# print(obj.url)
lst =['dbo.RAJESHEXPO','dbo.INFY','dbo.ATGL','dbo.JPPOWER','dbo.AREM','dbo.HDFCBANK','dbo.SURYODAY','dbo.SHYAMCENT']
for item in lst:
    obj.transform_tables(item)


