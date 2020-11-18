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


spark = SparkSession.builder.appName("Teste100MLinhasWithPandas").getOrCreate()

df = spark.read.csv(path="csvtest/MCD_yahoo100M.csv")

df.show(100000)
df.printSchema()


#try to count number of occorrences from each value

#dataCollect = df.collect()


#df = df.withColumnRenamed('_c0','Date')
df = df.withColumnRenamed('_c1','Open')
#df = df.withColumnRenamed('_c6','Volume')
#df = df.withColumnRenamed('_c4','Close')
#df.show(5)

#date = df.select('Date').collect()
#print(date)


#op = df.select('Open').collect()
#print(op)

#num_open = df.select('Open').describe()
#print(num_open)

df.agg(countDistinct(col("Open")).alias("count")).show()
df.write.parquet('csvtest/MCD_yahoo100M.parquet')