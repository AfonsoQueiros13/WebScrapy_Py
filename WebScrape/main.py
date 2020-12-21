
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
import subprocess
import datetime
import csv
from pyspark.sql import SQLContext


################################SPARK JOB INITAILIZATION###############################
spark = SparkSession.builder.appName("Scraping_5min4sites").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

#MAIN FUNCTION
def main():
    #GET DATE MONTH AND YEAR        
   
    stock = sys.argv[1]
    print(stock)
    count = 1
    while (count <=4):
        date = datetime.datetime.today()
        data = str(date.month) + "/"+ str(date.day) +  "/" + str(date.year) + " "+ (str('{:02d}'.format(date.hour)))+":" + (str('{:02d}'.format(date.minute)))

        if  (len(sys.argv) > 1):
            if  (count == 1):  
                values = gf_scrape(stock,data)

            if  (count == 2):
                values = yf_scrape(stock,data)
            
            if  (count == 3):
                values = mktw_scrape(stock,data)

            if  (count == 4):
                values = wsj_scrape(stock,data)
       
        #PATHS FOR SCRAPED FILES AND AUX

        path = 'scraping/'+stock+'_'+str(date.month)+'_'+str(date.year)+'.csv'
        print(path)
        path_new = 'scraping/'+stock+'_'+str(date.month)+'_'+str(date.year)+'new.csv'    
        path_old = 'scraping/'+stock+'_'+str(date.month)+'_'+str(date.year)+'old.csv'
        
        proc = subprocess.Popen(['/home/hadoop/hadoop/bin/hdfs', 'dfs', '-test', '-e', path] ,stdout=subprocess.PIPE)
        proc.communicate()
        


        if proc.returncode != 0: #file not exist > new month incoming or first record, create new csv
            with open('/home/hadoop/csv/template.csv','w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(values)
            file.close()
           
            proc2= subprocess.Popen(['/home/hadoop/hadoop/bin/hdfs', 'dfs', '-put', '/home/hadoop/csv/template.csv',path])   
            proc2.communicate()
            

        else :  #file  exist > READ csv and write new record
            with open('/home/hadoop/csv/template.csv','w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(values)
            file.close()

            
            proc3= subprocess.Popen(['/home/hadoop/hadoop/bin/hdfs', 'dfs', '-mv', path,path_old])   
            proc3.communicate()
            df_old = spark.read.csv(path= "hdfs://node-master:9000/user/hadoop/"+path_old)
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
            df_old = df_old.withColumnRenamed('_c10','Font')
            df_old = df_old.withColumnRenamed('_c11','TimeStamp')
            df_old.show()
        
        
            proc4 = subprocess.Popen(['/home/hadoop/hadoop/bin/hdfs', 'dfs', '-put', '/home/hadoop/csv/template.csv', path_new])
            proc4.communicate()
            df_new = spark.read.csv(path= "hdfs://node-master:9000/user/hadoop/"+path_new)
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
            df_new = df_new.withColumnRenamed('_c10','Font')
            df_new = df_new.withColumnRenamed('_c11','TimeStamp')
            df_new.show()
            df1 = df_old.unionByName(df_new)
            df1.show(df1.count(), False)
        
            # Save file to HDFS
            df1.write.csv(path= "hdfs://node-master:9000/user/hadoop/"+path)
            #df2.show(10)
            #TEST IF PATH OLD EXISTS 
            process1 = subprocess.Popen(['/home/hadoop/hadoop/bin/hdfs', 'dfs', '-test', '-e', path_old]) 
            process1.communicate()
            if process1.returncode == 0:
                process2= subprocess.run(['/home/hadoop/hadoop/bin/hdfs', 'dfs', '-rm', '-R', path_old])   

            process2 = subprocess.Popen(['/home/hadoop/hadoop/bin/hdfs', 'dfs', '-test', '-e', path_new])
            process2.communicate()
            if process2.returncode == 0:
                subprocess.run(['/home/hadoop/hadoop/bin/hdfs', 'dfs', '-rm', '-R', path_new])   

        count = count +1

#INVOKE MAIN
if __name__ == "__main__":
    main()
