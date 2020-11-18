#!/usr/bin/env python3.7
from pyspark.sql import SparkSession
import pandas
import datetime
import pandas_datareader.data as web
from pandas import Series, DataFrame
from pyspark.sql.functions import col, countDistinct

# Adjusting the size of matplotlib
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import style


spark = SparkSession.builder.appName("Teste100mLinhasWithPandas").getOrCreate()

df = spark.read.csv(path="csvtest/MCD_yahoo100m.csv")


print(df.dtypes)


#try to count number of occorrences from each value

#dataCollect = df.collect()


df = df.withColumnRenamed('_c0','Date')
df = df.withColumnRenamed('_c1','Open')
df = df.withColumnRenamed('_c2','High')
df = df.withColumnRenamed('_c3','Low')
df = df.withColumnRenamed('_c4','Close')
df = df.withColumnRenamed('_c5','AdjClose')
df = df.withColumnRenamed('_c6','Volume')

df = df.withColumn("Open", col("Open").cast('float'))
df = df.withColumn("High", col("High").cast('float'))
df = df.withColumn("Low", col("Low").cast('float'))
df = df.withColumn("Close", col("Close").cast('float'))
df = df.withColumn("AdjClose", col("AdjClose").cast('float'))
df = df.withColumn("Volume", col("Volume").cast('float'))
print(df.dtypes)
#df.show(5)

#date = df.select('Date').collect()
#print(date)


#op = df.select('Open').collect()
#print(op)

#num_open = df.select('Open').describe()
#print(num_open)

#df.agg(countDistinct(col("Open")).alias("count")).show()
df.write.parquet('csvtest/MCD_yahoo100m.parquet')