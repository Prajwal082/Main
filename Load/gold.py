import sys
sys.path.append("d:\\Databricks\\Vsprojects\\")
from config.Initializespark import Sparksetup
from Utils.Stockutils import Utils

from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType,TimestampType,DateType


class Gold(Utils):

    def __init__(self) -> None:
        super().__init__()

        self.Logger.info("Setting up Spark...!\n")
        self.spark = Sparksetup.create_spark(self)
        self.Logger.info("Spark setup Done...!\n")

        self._initconfigs()
        self.list_ofTables = self.fetch_tablestoLoad(url=self.__url,user=self.__user,password=self.__password,driver=self.__driver)


    def _initconfigs(self):
        self.__driver =  self.get_leafconfig(self.config_data,"ssms.cred.driver")
        self.__user = self.get_leafconfig(self.config_data,"ssms.cred.user")
        self.__password = self.get_leafconfig(self.config_data,"ssms.cred.password")
        self.__url = self.get_leafconfig(self.config_data,"ssms.cred.url")
        pyodbc = self.get_leafconfig(self.config_data,"ssms.pyodbc")
        self.__pyodbc = pyodbc.format(user = self.__user,password = self.__password,Database='gold')

    def __truncate_GoldTables(self):
        self.Logger.info("Truncate gold table..")
        self.truncate_table_sql_server(table_name='Stockdata',pyodbc=self.__pyodbc)

    def construct_gold(self,lst:list):
        silver_url  = self.__url.format(Database= "silver")
        gold_url = self.__url.format(Database= "gold")

        for item in lst:
            self.Logger.info(f"reading {item}")

            df = self.spark.read\
                    .format("jdbc")\
                    .option("driver",self.__driver)\
                    .option("url", silver_url)\
                    .option("dbtable",item)\
                    .option("user", self.__user)\
                    .option("password", self.__password)\
                    .load()
            

            df.write\
                .format("jdbc")\
                .option("driver",self.__driver)\
                .option("url", gold_url)\
                .option("dbtable", "Stockdata")\
                .option("user", self.__user)\
                .option("password",self.__password)\
                .mode('append')\
                .save()
            
    def launchGold(self) -> None:
        self.__truncate_GoldTables()
        result = list(map(self.construct_gold,self.list_ofTables))


if __name__ == "__main__":

    gold_obj = Gold()

    gold_obj.launchGold()



