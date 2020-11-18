#!/usr/bin/env python3.7
from pyspark.sql import SparkSession
import pandas as pd
import datetime
import pandasql as pdsql

from pandas import Series, DataFrame
from pyspark.sql.functions import col, countDistinct

import psutil

from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("10MinParquetFileWithPandasDF").getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)


#parqDF = spark.read.csv(path="csvtest/MCD_yahoo1000M.csv")
#parqDF = parqDF.withColumnRenamed('_c1','Open')

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
parqDF = spark.read.parquet("csvtest/MCD_yahoo10M.parquet") 
#parqDF.memory_usage()
print("AQUI")
pandas_df = parqDF.toPandas()

print(pandas_df.memory_usage())

print(pandas_df.dtypes)
#print (pandas_df)
#pandas_df.registerTempTable("mcdyahoo")
#pandas_df = spark.sql("SELECT avg(Open) from mcdyahoo")
#pandas_df.show()

pandas_df.info(memory_usage='deep')
del parqDF
q1 = """SELECT AVG(Open) FROM pandas_df """
print("here")
print( psutil.virtual_memory().percent)
print(pdsql.sqldf(q1, locals()))

print( psutil.virtual_memory().percent)
#parqDF.registerTempTable("mcdyahoo")
#parkSQL = spark.sql("SELECT avg(Open) from mcdyahoo")
#parkSQL.show()
#parkSQL.printSchema()