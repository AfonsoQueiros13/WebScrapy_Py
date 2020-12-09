#!/home/hadoop/anaconda3/bin/python3.7
from bs4 import BeautifulSoup
import urllib.request
import csv
import sys
import datetime

date = datetime.datetime.today()
data = str(date.month) + "/"+ str(date.day) +  "/" + str(date.year) + " "+ (str('{:02d}'.format(date.hour)))+":" + (str('{:02d}'.format(date.minute)))
print(data)
def create_common_attributes(summarry,key_data,attr):   #Moving elements craped to  common output format
   for item in summarry:
      attr.append(item)
   for item in key_data:
      attr.append(item)
   attr.pop(1)
   print(attr)
   #del attr[2:7]
   attr[1], attr[5] = attr[5], attr[1]
   #attr[2], attr[5] = attr[5], attr[2]
   attr[3], attr[2] = attr[3].split("-")
   attr[4], attr[9] = attr[9], attr[4]
   attr[7], attr[5] = attr[5], attr[7]
   attr[5]= attr[5].split('(')[0]
   attr[13], attr[6] = attr[6], attr[13]
   attr[6]= attr[6].split('(')[0]
   attr[14], attr[7] = attr[7], attr[14]
   print(attr)
   attr[7]= attr[13].split('(')[0]
   attr[9], attr[8] = attr[9].split("-")
   attr[10] = data
   del attr[11:25]
   res = []
   for x in attr:
        res.append(x)
   return res




def wsj_scrape(stock):
   now = datetime.datetime.now()
   data = now.strftime("%Y-%m-%d")



   opener = urllib.request.build_opener()
   opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36')]
   response = opener.open('https://www.wsj.com/market-data/quotes/'+stock)

   soup = BeautifulSoup(response, 'html.parser')
   #price = soup.find ('h3',{'class':'intraday__price'})
   
   price = soup.find('span',{'id':'quote_val'})
   while price == None:
      print("sou none!!!!!!")
      response = opener.open('https://www.wsj.com/market-data/quotes/'+stock)
      soup = BeautifulSoup(response, 'html.parser')
   #price = soup.find ('h3',{'class':'intraday__price'})
      price = soup.find('span',{'id':'quote_val'})
   print(price)

   summarry = soup.find_all('span',{'class':'WSJTheme--data_data--2QuzEiZE'}) 
   summarry_list = []
   for i in summarry:
      summarry_list.append(i.text)
   key_data = soup.find_all('span',{'class':'WSJTheme--data_data--3CZkJ3RI'}) 
   key_data_list = []
   for i in key_data:
      key_data_list.append(i.text)
   summarry_atb = []
   summarry_atb.append(price.text)


   
   summarry_atb = create_common_attributes(summarry_list,key_data_list,summarry_atb)
   return summarry_atb