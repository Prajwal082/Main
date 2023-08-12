import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas

class Sparksetup():

    def spark_session(self):
        spark=SparkSession.builder.master('local[5]')\
                .appName('local')\
                .config('spark.driver.extraClassPath', "C:\spark\jars\sqljdbc_4.2\enu\jre8\sqljdbc42.jar")\
                .config('spark.executor.extraClassPath', "C:\spark\jars\sqljdbc_4.2\enu\jre8\sqljdbc42.jar")\
                .getOrCreate()
        spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs","false")
        spark.conf.set('spark.driver.cores',4)
        spark.conf.set('spark.driver.executor',4)
        spark.conf.set('spark.driver.executor',8)
        spark.conf.set('spark.driver.memory','56g')
        spark.conf.set('spark.executor.memory','56g')
        spark.conf.set("spark.eventLog.enabled",True)

        return spark



