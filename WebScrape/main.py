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
import csv
from pyspark.sql import SQLContext


################################SPARK JOB INITAILIZATION###############################
spark = SparkSession.builder.appName("Scraping_5min4sites").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

#MAIN FUNCTION
def main():

    if len(sys.argv) > 2:
        market = sys.argv[1]
        stock = sys.argv[2]

        if(market == "GF"):
            values = gf_scrape(stock)

        
        elif(market == "YF"):
            values = yf_scrape(stock)
        
        elif(market == "MKTW"):
            values = mktw_scrape(stock)

        elif(market == "WSJ"): 
            values = wsj_scrape(stock)
        
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
    path_new = 'scraping/'+stock+'_'+ market+'_'+str(data.month)+'_'+str(data.year)+'new.csv'    
    path_old = 'scraping/'+stock+'_'+ market+'_'+str(data.month)+'_'+str(data.year)+'old.csv'
    
    process1 = subprocess.Popen(['hdfs', 'dfs', '-test', '-e', path_old])
    process1.communicate()

    if process1.returncode == 0:
        print("HEREEEEEEEEEE")
        proc4 = subprocess.Popen(['hdfs', 'dfs', '-rm', '-R', path_old])   
        proc4.communicate()

    process2 = subprocess.Popen(['hdfs', 'dfs', '-test', '-e', path_new])
    process2.communicate()

    if process2.returncode == 0:
        proc5 = subprocess.Popen(['hdfs', 'dfs', '-rm', '-R', path_new])   
        proc5.communicate()


    proc = subprocess.Popen(['hdfs', 'dfs', '-test', '-e', path])
    proc.communicate()




    if proc.returncode != 0: #file not exist > new month incoming or first record, create new csv
        print ('%s does not exist' % path)
        with open('/home/hadoop/csv/template.csv','w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(values)
        file.close()
        path = 'scraping/'+stock+'_'+ market+'_'+str(data.month)+'_'+str(data.year)+'.csv'
        proc2 = subprocess.Popen(['hdfs', 'dfs', '-put', '/home/hadoop/csv/template.csv', path])   
        proc2.communicate()
        

    else :  #file  exist > READ csv and write new record
        print ('%s exists' % path)
        with open('/home/hadoop/csv/template.csv','w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(values)
        file.close()

        
        proc3= subprocess.Popen(['hdfs', 'dfs', '-mv', path,path_old])   
        proc3.communicate()
        df_old = spark.read.csv(path_old)
        df_old = df_old.withColumnRenamed('_c0','Price')
        df_old = df_old.withColumnRenamed('_c1','Open')
        df_old = df_old.withColumnRenamed('_c2','High')
        df_old = df_old.withColumnRenamed('_c3','Low')
        df_old = df_old.withColumnRenamed('_c4','MarketCap')
        df_old= df_old.withColumnRenamed('_c5','PERatio')
        df_old = df_old.withColumnRenamed('_c6','Dividend')
        df_old = df_old.withColumnRenamed('_c7','Close')
        df_old = df_old.withColumnRenamed('_c8','High52')
        df_old = df_old.withColumnRenamed('_c9','Low52')
        df_old.show()
       
    
        proc4 = subprocess.Popen(['hdfs', 'dfs', '-put', '/home/hadoop/csv/template.csv', path_new])
        proc4.communicate()
        df_new = spark.read.csv(path_new)
        df_new = df_new.withColumnRenamed('_c0','Price')
        df_new = df_new.withColumnRenamed('_c1','Open')
        df_new = df_new.withColumnRenamed('_c2','High')
        df_new = df_new.withColumnRenamed('_c3','Low')
        df_new = df_new.withColumnRenamed('_c4','MarketCap')
        df_new = df_new.withColumnRenamed('_c5','PERatio')
        df_new = df_new.withColumnRenamed('_c6','Dividend')
        df_new = df_new.withColumnRenamed('_c7','Close')
        df_new = df_new.withColumnRenamed('_c8','High52')
        df_new = df_new.withColumnRenamed('_c9','Low52')
        df_new.show()
        df1 = df_old.unionByName(df_new)
        df1.show()
       
        # Save file to HDFS
        df1.write.csv(path)
        #df2.show(10)
    

#INVOKE MAIN
if __name__ == "__main__":
    main()
