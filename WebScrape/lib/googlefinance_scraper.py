
from bs4 import BeautifulSoup
import urllib.request
import csv
import sys
import datetime

def gf_scrape(stock,data):
    
    opener = urllib.request.build_opener()
    opener.addheaders = [('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0')]
    
    response = opener.open('https://www.google.com/search?q='+stock)
    soup = BeautifulSoup(response, 'html.parser')
    price = soup.find ('span',{'class':'IsqQVc NprOob XcVN5d'})
    summarry = soup.find_all('td',{'class':'iyjjgb'}) 
    while price == None:
        price = soup.find ('span',{'class':'IsqQVc NprOob XcVN5d'})
        summarry = soup.find_all('td',{'class':'iyjjgb'}) 
    

    summarry_atb = []
    summarry_atb.append(price.text)


    for item in summarry:
        summarry_atb.append(item.text)

    summarry_atb = [x.replace(",", ".")for x in summarry_atb] 
    summarry_atb.append("gf") 
    summarry_atb.append(data)   
    print(summarry_atb)
    return summarry_atb

