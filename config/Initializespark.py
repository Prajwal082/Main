import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas

class Sparksetup():

    def config(self):
        self.spark=SparkSession.builder.master('local[5]')\
                .appName('local')\
                .config('spark.driver.extraClassPath', "C:\spark\jars\sqljdbc_4.2\enu\jre8\sqljdbc42.jar")\
                .config('spark.executor.extraClassPath', "C:\spark\jars\sqljdbc_4.2\enu\jre8\sqljdbc42.jar")\
                .getOrCreate()
        self.spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs","false")
        self.spark.conf.set('spark.driver.cores',4)
        self.spark.conf.set('spark.driver.executor',4)
        self.spark.conf.set('spark.driver.executor',8)
        self.spark.conf.set('spark.driver.memory','56g')
        self.spark.conf.set('spark.executor.memory','56g')
        self.spark.conf.set("spark.eventLog.enabled",True)
        # self.spark.conf.set("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") 
        # # self.spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        # self.spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 

        return self.spark
        self.spark.stop()


# .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
# .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
# .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
