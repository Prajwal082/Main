

df=spark.read.option('header',True).option('inferschema',True).csv("D:\Databricks\ind_nifty50list.csv")
df.show()



