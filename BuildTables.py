# >>>>USE Initializespark MODULE TO IMPORT AND SETUP SPARK<<<<

import Initializespark as sp
spk=sp.Sparksetup()
spark=spk.spark_session()


def read_file(self):
    self.table_name='orgdata'
    self.df=self.spark.read.option('header',True).option('inferschema',True).csv("D:\Databricks\src\orgdata.csv")
    self.df.show()
    self.df=self.df.drop("Index")
    self.df=self.df.toPandas()
    # Function call to write to SQL server
    self.sqlsetup()


def pandas_sqlsetup(self):
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

try:
    df = spark.read\
        .format("jdbc")\
        .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("url", "jdbc:sqlserver://DESKTOP-0A2HT13;databaseName=Databricks;")\
        .option("dbtable", "file_list")\
        .option("user", "prajwal")\
        .option("password", 123)\
        .load().show()
    
except Exception as e:
    print(e)

