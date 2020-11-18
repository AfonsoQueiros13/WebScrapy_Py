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

from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("1BinParquetFileWithPandas").getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)


#parqDF = spark.read.csv(path="csvtest/MCD_yahoo1000M.csv")
#parqDF = parqDF.withColumnRenamed('_c1','Open')
parqDF = spark.read.parquet("csvtest/MCD_yahoo1B.parquet")



parqDF.registerTempTable("mcdyahoo")
parqDF = spark.sql("SELECT avg(Open) from mcdyahoo")
parqDF.show()

#parqDF.registerTempTable("mcdyahoo")
#parkSQL = spark.sql("SELECT avg(Open) from mcdyahoo")
#parkSQL.show()
#parkSQL.printSchema()