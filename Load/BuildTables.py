# >>>>USE Initializespark MODULE TO IMPORT AND SETUP SPARK<<<<
import pandas as pd
import os,sys
import datetime
sys.path.append("d:\\Databricks\\Vsprojects\\")
from  config.Initializespark import Sparksetup
from Utils.Stockutils import Utils

class Buildtables(Utils):
    

    def __init__(self) -> None:
        super().__init__()
        
        self.Logger.info("Setting up Spark...!\n")
        self.spark = Sparksetup.create_spark(self)
        self.Logger.info("Spark setup Done...!\n")

        self.schema = self.tableschema()

        self._initconfigs()

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
        StructField('Total_Traded_Quantity',DecimalType(),False),
        StructField('Turnover',DecimalType(),False),
        StructField('No_of_Trades',DecimalType(),False),
        StructField('Deliverable_Qty',DecimalType(),False),
        StructField('Percentage_DlyQt_to_TradedQty',DecimalType(),False)
        ]) '''
        return schema
    
    def _initconfigs(self):
        self.driver = self.get_leafconfig(self.config_data,"ssms.cred.driver")
        url = self.get_leafconfig(self.config_data,"ssms.cred.url")
        self.url = url.format(Database= "Databricks")
        self.user = self.get_leafconfig(self.config_data,"ssms.cred.user")
        self.password = self.get_leafconfig(self.config_data,"ssms.cred.password")
        pyodbc = self.get_leafconfig(self.config_data,"ssms.pyodbc")
        self.pyodbc = pyodbc.format(user=self.user,password=self.password,Database="Databricks")
        self.dbtable = self.get_leafconfig(self.config_data,"ssms.dbtable")
    
    def read_file(self,path,name):
        from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType,TimestampType,DateType
        self.table_name=name
        self.path=path
        self.Logger.info(msg=f"Reading file from location: {path}")
        self.df=self.spark.read.format('csv').option('sep',',').option('header',True).schema(eval(self.schema)).load(self.path)
        self.jdbc_write(name)


    def pandas_sqlsetup(self):
        from sqlalchemy import create_engine
        import pyodbc
        engine = create_engine(self.pyodbc)
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
                                .option("driver",self.driver)\
                                .option("url", self.url)\
                                .option("dbtable", self.dbtable)\
                                .option("user", self.user)\
                                .option("password", self.password)\
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
                                .option("driver",self.driver)\
                                .option("url", self.url)\
                                .option("dbtable", self.name)\
                                .option("user", self.user)\
                                .option("password", self.password)\
                                .mode('overwrite')\
                                .save()
            self.Logger.info(f"Data inserted to SSMS table for {self.name}\n")
        except Exception as e:
            print(e)

    def list_dir(self):
        self.table_name='file_list'
        self.dir= os.listdir("D:\\Databricks\\SRC\\Delivery_data")
        ct = datetime.datetime.now()
        lst=[]
        for files in self.dir:
            name=files.split('-')[7:8]
            timestamp=os.path.getmtime(f"D:\\Databricks\\SRC\\Delivery_data/{files}")
            modifiedtime=datetime.datetime.fromtimestamp(timestamp)
            case={'file_name':files,'table':name[0],'d_update':modifiedtime,'enabled':1}
            lst.append(case)
        self.Logger.info("Directory List done...!\n")
        self.df = pd.DataFrame(lst)
        self.pandas_sqlsetup()

    def file_nameextractor(self):
        lst=self.file_listdf.select('file_name').collect()
        for item in lst:
            name=item[0].split("-")[7]
            path=f"D:\Databricks\SRC\Delivery_data/{item[0]}"
            self.read_file(path,name)

    def launchBronze(self):
        self.list_dir()
        self.jdbc_read()

if __name__ == "__main__":
    
    Bronze = Buildtables()

    Bronze.launchBronze()




