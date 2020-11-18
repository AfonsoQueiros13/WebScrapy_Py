#!/usr/bin/env python3.7
from pyspark.sql import SparkSession
import pandas
import datetime
import pandas_datareader.data as web
from pandas import Series, DataFrame

# Adjusting the size of matplotlib
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import style


spark = SparkSession.builder.appName("TestesDATAFRAME").getOrCreate()

df = spark.read.csv(path="csvtest/MCD_yahoo.csv")

#df.show(5)
#df.printSchema()

#try to count number of occorrences from each value

dataCollect = df.collect()


#df = df.withColumnRenamed('_c0','Date')
df = df.withColumnRenamed('_c1','Open')
df = df.withColumnRenamed('_c6','Volume')
df = df.withColumnRenamed('_c4','Close')
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

#pandas_df['Open'] = pandas.to_numeric(pandas_df['Open'],errors='coerce')
#pandas_df['Volume'] = pandas.to_numeric(pandas_df['Volume'],errors='coerce')
#print(pandas_df['Open'].nlargest(3))
#print(pandas_df['Volume'].nlargest(5))

pandas_df['Close'] = pandas.to_numeric(pandas_df['Close'],errors='coerce')

close_px = pandas_df['Close']
pandas_df['Close_ROLL'] = pandas_df['Close'].rolling(window=30,center=False).mean()

close_roll = pandas_df['Close_ROLL']


print (close_roll)

print("PANDAS DATAFRAME W/Roll AVG")
print (pandas_df)


mpl.rc('figure', figsize=(8, 7))
mpl.__version__


# Adjusting the style of matplotlib
style.use('ggplot')


plt.subplot(1, 2, 1)
close_px.plot(label='MCD Yahoo')
close_roll.plot(label='close roll')

plt.legend()

plt.subplot(1, 2, 2)
rets = close_px / close_px.shift(1) - 1
rets.plot(label='return')

plt.legend()

plt.tight_layout()
plt.show()