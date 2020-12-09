
from bs4 import BeautifulSoup
import urllib.request
import csv
import sys
import datetime
date = datetime.datetime.today()
data = str(date.month) + "/"+ str(date.day) +  "/" + str(date.year) + " "+ (str('{:02d}'.format(date.hour)))+":" + (str('{:02d}'.format(date.minute)))
print(data)
def gf_scrape(stock):
    
  
    
    
    opener = urllib.request.build_opener()
    opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36')]
    response = opener.open('https://www.google.com/search?q='+stock)

    soup = BeautifulSoup(response, 'html.parser')
    price = soup.find ('span',{'class':'IsqQVc NprOob XcVN5d'})
    summarry = soup.find_all('td',{'class':'iyjjgb'}) 
    

    summarry_atb = []
    summarry_atb.append(price.text)


    for item in summarry:
        summarry_atb.append(item.text)
    summarry_atb.append(data)   
    return summarry_atb
