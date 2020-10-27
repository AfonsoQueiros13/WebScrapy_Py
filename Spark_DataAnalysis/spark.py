#!/usr/bin/env python3.7
from pyspark.sql import SparkSession
import pandas
spark = SparkSession.builder.appName("CVSLOADTEST").getOrCreate()

df = spark.read.csv(path="csvtest/MCD_yahoo.csv")

#df.show(5)
#df.printSchema()

#try to count number of occorrences from each value

dataCollect = df.collect()


#df = df.withColumnRenamed('_c0','Date')
df = df.withColumnRenamed('_c1','Open')
df = df.withColumnRenamed('_c6','Volume')

#df.show(5)

#date = df.select('Date').collect()
#print(date)


#op = df.select('Open').collect()
#print(op)

#num_open = df.select('Open').describe()
#print(num_open)

pandas_df = df.toPandas()

print("PANDAS DATAFRAME")

print (pandas_df)

pandas_df['Open'] = pandas.to_numeric(pandas_df['Open'],errors='coerce')
pandas_df['Volume'] = pandas.to_numeric(pandas_df['Volume'],errors='coerce')
print(pandas_df['Open'].nlargest(3))
print(pandas_df['Volume'].nlargest(5))