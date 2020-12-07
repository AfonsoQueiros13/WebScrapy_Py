from pyspark.sql import SparkSession
import datetime
import mysql.connector
from pyspark import SparkContext, SQLContext
sc = SparkContext('local')
sqlContext = SQLContext(sc)

#mydb = mysql.connector.connect(
#  host="localhost",
#  user="hiveuser",
#  password="hive123",
#  auth_plugin='mysql_native_password'
#)



#mycursor = mydb.cursor()

#mycursor.execute("CREATE DATABASE testepbi")
#mydb.close()

mydb = mysql.connector.connect(
  host="192.168.1.50",
  user="afonso",
 password="afonso",
  auth_plugin='mysql_native_password',
  database="metastore_db"
)
mycursor = mydb.cursor()
#mycursor.execute("CREATE TABLE AAPL_data_December (price VARCHAR(6), open VARCHAR(6),high VARCHAR(6),low VARCHAR(7), marketcap VARCHAR(10),peratio VARCHAR(5),dividend VARCHAR(10),close VARCHAR(6),high52 VARCHAR(6),low52 VARCHAR(6),timestamp VARCHAR(7))


mycursor.execute("SELECT * FROM AAPL_data_December")
for x in mycursor:
  print(x) 


spark = SparkSession.builder.appName("TestesPowerBIImportCSVtoDB").getOrCreate()


df = spark.read.csv(path="scraping/gf/AAPL_GF_12_2020.csv")
df = df.withColumnRenamed('_c0','Price')
df = df.withColumnRenamed('_c1','Open')
df = df.withColumnRenamed('_c2','High')
df = df.withColumnRenamed('_c3','Low')
df = df.withColumnRenamed('_c4','MarketCap')
df = df.withColumnRenamed('_c5','PERatio')
df = df.withColumnRenamed('_c6','Dividend')
df = df.withColumnRenamed('_c7','Close')
df = df.withColumnRenamed('_c8','High52')
df = df.withColumnRenamed('_c9','Low52')
df = df.withColumnRenamed('_c10','TimeStamp')
df.show(1000)
# write it to the table
#url = "jdbc:mysql://localhost/testepbi"
#properties = {
#    "user": "hiveuser",
#    "password": "hive123",
#    "driver":"com.mysql.cj.jdbc.Driver"
#}

#save = df.write.jdbc(url=url, table="AAPL_data_December",mode = "append", properties=properties)

df.write.format('jdbc').options(
      url='jdbc:mysql://192.168.1.50/metastore_db',
      driver='com.mysql.cj.jdbc.Driver',
      dbtable='AAPL_data_December',
      user='afonso',
      password='afonso').mode('append').save()