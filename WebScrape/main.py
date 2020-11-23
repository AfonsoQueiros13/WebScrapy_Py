####################################WEBSCRAPING SCRIPT MAIN############################################
####################################AFONSO   QUEIROS    B2F#######################################################
#sys.argv[1] (FIRST ARGUMENT) : GF -> Google Finance ; YF -> Yahoo Finance ; MKTW -> MarketWatch 
#WSJ ->  The Wall Street Journal
#sys.argv[2] (SECOND ARGUMENT) : P.E: MCD (MACDONNALDS); AAPL(APPLE)...


##########################REQUIRED LIB IMPORTS############################################
import sys
from lib.googlefinance_scraper import gf_scrape
from lib.yahoo_finance_scraper import yf_scrape
from lib.mw_webscraper import mktw_scrape
from lib.wallstreet_scraper import wsj_scrape
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import subprocess
from subprocess import Popen, PIPE
import datetime
from pyspark.sql import SQLContext


################################SPARK JOB INITAILIZATION###############################
spark = SparkSession.builder.appName("Scraping_5min4sites").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

#atributes of stock (String to dataframe schema)
attributes = ["Price","Open", "High","Low","MarketCap","PERatio","Dividend","Close","High52","Low52"]

field = [StructField("Price",StringType(), True),StructField("Open", StringType(), True),\
StructField("High", StringType(), True),StructField("Low", StringType(), True),\
StructField("MarketCap", StringType(), True),\
StructField("PERatio", StringType(), True),StructField("Dividend", StringType(), True),\
StructField("Close", StringType(), True), StructField("High52", StringType(), True),\
StructField("Low52", StringType(), True)]
schema = StructType(field)



#MAIN FUNCTION
def main():

    if len(sys.argv) > 2:
        market = sys.argv[1]
        stock = sys.argv[2]

        

        if(market == "GF"):
            values = gf_scrape(stock)

        
        elif(market == "YF"):
            yf_scrape(stock)
        
        elif(market == "MKTW"):
            mktw_scrape(stock)

        elif(market == "WSJ"): 
            wsj_scrape(stock)
        
        else:
            print("Stock Market not recognized")
    
    else:
        raise ValueError("USAGE: python3 main.py ""INITIALS FOR STOCK"" ""INITIALS FOR MARKET"", for example : python3 AAPL WSJ")
    
    #print("tou aqui")
    #cat = Popen(["hdfs", "dfs", "-cat", path], stdout=PIPE)
    #print("tou aqui1")
    #df = spark.createDataFrame([values])
    #print("tou aqui2")  
    #path = 'scraping_tmp/'+stock+'_'+ market+'_'+str(data.month)+'_'+str(data.year)+'.csv'
    #print("tou aqui3")
    #df.write.csv(path)
    #print("tou aqui4")
    data = datetime.datetime.today()

    path = 'scraping/'+stock+'_'+ market+'_'+str(data.month)+'_'+str(data.year)+'.csv'
    proc = subprocess.Popen(['hdfs', 'dfs', '-test', '-e', path])
    proc.communicate()
    if proc.returncode != 0: #file not exist > new month incoming or first record, create new csv
        print ('%s does not exist' % path)
        path = 'scraping/'+stock+'_'+ market+'_'+str(data.month)+'_'+str(data.year)+'.csv'
        

        #proc = subprocess.Popen(['hdfs', 'dfs', '-touchz', path])

        df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
        df.show()
        df.write.csv(path)
    else :  #file  exist > READ csv and write new record
        print ('%s exists' % path)
        df = spark.read.csv(path = 'scraping/'+stock+'_'+ market+'_'+str(data.month)+'_'+str(data.year)+'.csv')
            
    print("estou aqui1")
    print(values)
    df_data = spark.createDataFrame([values])
    df_data.show()
    print("estou aqui2")
    df_new = df.union(df_data)
    print("estou aqui3")
    df_new.show()
    print("estou aqui4")
    proc = subprocess.Popen(['hdfs', 'dfs', '-rm', '-R', path])
    print("estou aqui5")
    proc.communicate()

    df_new.write.csv(path)
    print("estou aqui6")
    #put = subprocess.Popen(['hdfs', 'dfs', '-put', '-',path], stdin=cat.stdout)
    #print("tou aqui5")
    #put.communicate()   
    

#INVOKE MAIN
if __name__ == "__main__":
    main()
