import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas

class Sparksetup():

    def __init__(self) -> None:
        self.spark=SparkSession.builder.master('local[5]')\
             .appName('local')\
             .getOrCreate()
        # Function to read the files
        self.read_file()
        
        
    def read_file(self):
        self.table_name='orgdata'
        self.df=self.spark.read.option('header',True).option('inferschema',True).csv("D:\Databricks\src\orgdata.csv")
        self.df.show()
        self.df=self.df.toPandas()
        # Function call to write to SQL server
        self.sqlsetup()

    
    def sqlsetup(self):
        from sqlalchemy import create_engine
        import pyodbc
        engine = create_engine('mssql+pyodbc://prajwal:Prajwal083@DESKTOP-0A2HT13/Databricks?driver=ODBC Driver 17 for SQL Server')
        try:
            conn=engine.connect()
            print("Connection Sucessfull...")
            self.df.to_sql(self.table_name,con=conn,if_exists='replace')
            print("Data inserted...")
        except Exception as e:
            print("Connection failed....{}".format(e))

sprk=Sparksetup()



