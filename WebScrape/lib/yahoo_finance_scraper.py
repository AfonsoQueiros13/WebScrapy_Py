
from bs4 import BeautifulSoup
import urllib.request
import csv
import sys
import datetime

def create_common_attributes(summarry,data_intervals,attr):   #Moving elements craped to  common output format
    for item in summarry:
        attr.append(item.text)
    for item in data_intervals:
        attr.append(item.text)
    del attr[2:13]
    attr[1], attr[3] = attr[3], attr[1]
    attr[3], attr[2] = attr[17].split("-")    
    
    attr[4], attr[8] = attr[8], attr[4]
    attr[10], attr[5] = attr[5], attr[10]
    attr[6]= attr[26].split('(')[0]
    attr[13], attr[7] = attr[7], attr[13]
    attr[9], attr[8] = attr[18].split("-")
    del attr[10:32]
    return attr

def yf_scrape(stock):

    now = datetime.datetime.now()
    data = now.strftime("%Y-%m-%d")


    #atributes of stock
    attributes = ["Price","Open", "High","Low","MarketCap","PERatio","Dividend","Close","High52","Low52"]

    opener = urllib.request.build_opener()
    opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36')]
    response = opener.open('https://finance.yahoo.com/quote/'+stock)

    soup = BeautifulSoup(response, 'html.parser')
    price = soup.find ('h3',{'class':'intraday__price'})
    price = soup.find ('span',{'class':'Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)'})
    #print(price)
    summarry = soup.find_all('span',{'class':'Trsdu(0.3s)'}) 

    data_intervals = soup.find_all('td',{'class':'Ta(end) Fw(600) Lh(14px)'}) 

    summarry_atb = []
    summarry_atb.append(price.text)
    #summarry_atb.append(highlow.text)

    with open('scraping_yf/'+ data + '_'+ stock +'_yahoo_finance.csv','w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(attributes)
        summarry_atb = create_common_attributes(summarry,data_intervals,summarry_atb)
        writer.writerow(summarry_atb)
    file.close()


