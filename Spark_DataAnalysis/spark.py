#!/usr/bin/env python3.7
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CVSLOADTEST").getOrCreate()

df = spark.read.csv(path="csvtest/MCD_yahoo.csv")

df.show(5)
df.printSchema()

#try to count number of occorrences from each value

dataCollect = df.collect()

df = df.withColumnRenamed('_c0','Date')
df = df.withColumnRenamed('_c1','Open')

df.show(5)

date = df.select('Date').collect()
print(date)


op = df.select('Open').collect()
print(op)

