# >>>>USE Initializespark MODULE TO IMPORT AND SETUP SPARK<<<<
import logging
from  config.Initializespark import Sparksetup
Log_format='[%(asctime)s] : [%(lineno)s] : [%(levelname)s] : %(message)s'
level = logging.INFO
logging.basicConfig(level=level,format=Log_format)

class Buildtables(Sparksetup):
    
    Logger=logging.getLogger()  
    logging.getLogger("py4j").setLevel(logging.INFO)
    Logger.info("Process started \n")

    def __init__(self) -> None:
        self.Logger.info("Setting up Spark...!\n")
        self.spark =super().config()
        self.Logger.info("Spark setup Done...!\n")
        self.schema=self.tableschema()
        self.list_dir()
        self.jdbc_read()

    def tableschema(self):
        from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType,TimestampType,DateType
        schema= ''' StructType([
        StructField('Symbol',StringType(),False),
        StructField('Series',StringType(),False),
        StructField('Date',StringType(),False),
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
        return schema
    
    def read_file(self,path,name):
        from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType,TimestampType,DateType
        import datetime
        self.table_name=name
        self.path=path
        self.Logger.info(msg=f"Reading file from location: {path}")
        self.df=self.spark.read.format('csv').option('sep',',').option('header',True).schema(eval(self.schema)).load(self.path)
        self.jdbc_write(name)


    def pandas_sqlsetup(self):
        from sqlalchemy import create_engine
        import pyodbc
        engine = create_engine('mssql+pyodbc://prajwal:456@DESKTOP-0A2HT13/Databricks?driver=ODBC Driver 17 for SQL Server')
        try:
            conn=engine.connect()
            self.Logger.info("Connection Sucessfull...")
            self.df.to_sql(self.table_name,con=conn,if_exists='replace')
            self.Logger.info(msg=f"Data inserted...to table:{self.table_name}")
        except Exception as e:
            self.Logger.critical("Connection failed....{}".format(e))


    def jdbc_read(self):
        try:
            self.file_listdf = self.spark.read\
                                .format("jdbc")\
                                .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                                .option("url", "jdbc:sqlserver://DESKTOP-0A2HT13;databaseName=Databricks;")\
                                .option("dbtable", "dbo.file_list")\
                                .option("user", "prajwal")\
                                .option("password", 456)\
                                .load()
        except Exception as e:
            print(e)
        self.file_nameextractor()

    def jdbc_write(self,name):
        self.name=name
        self.Logger.info(f"Writing Dataframe to SQL table for: {self.name}")
        try:
            self.file_listdf = self.df.write\
                                .format("jdbc")\
                                .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                                .option("url", "jdbc:sqlserver://DESKTOP-0A2HT13;databaseName=Databricks;")\
                                .option("dbtable", self.name)\
                                .option("user", "prajwal")\
                                .option("password", 456)\
                                .mode('overwrite')\
                                .save()
            self.Logger.info(f"Data inserted to SSMS table for {self.name}\n")
        except Exception as e:
            print(e)

    def list_dir(self):
        import pandas as pd
        import os,sys
        import datetime
        self.table_name='file_list'
        self.dir= os.listdir("D:\Databricks\SRC\Delivery_data")
        ct = datetime.datetime.now()
        self.Logger.info("Directory List done...!\n")
        lst=[]
        for files in self.dir:
            name=files.split('-')[7:8]
            timestamp=os.path.getmtime(f"D:\Databricks\SRC\Delivery_data/{files}")
            modifiedtime=datetime.datetime.fromtimestamp(timestamp)
            case={'file_name':files,'d_update':modifiedtime,'enabled':1}
            lst.append(case)
        self.df = pd.DataFrame(lst)
        self.pandas_sqlsetup()

    def file_nameextractor(self):
        lst=self.file_listdf.select('file_name').collect()
        for item in lst:
            name=item[0].split("-")[7]
            path=f"D:\Databricks\SRC\Delivery_data/{item[0]}"
            self.read_file(path,name)

Buildtables()




