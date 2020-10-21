
from bs4 import BeautifulSoup
import urllib.request
import csv
import sys

stock = sys.argv[1]

#atributes of stock
#attributes = ["Price","Day Range", "Open","52 Week","MarketCap",
#"Racio","Shares Outst.","Public Float","Beta",
#"Rev per empl.","PE Ratio","EPS","Yield","Dividend","Ex Divid Rate","Short Interest",
#"Percent of Float Shorted","Avg volume"]

opener = urllib.request.build_opener()
opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36')]
response = opener.open('https://www.marketwatch.com/investing/stock/'+stock)

soup = BeautifulSoup(response, 'html.parser')
#price = soup.find ('h3',{'class':'intraday__price'})
price = soup.find ('bg-quote',{'class':'value'})
attributes = soup.find_all ('small',{'class':'label'})
attributes_txt= ["Price"]
for item in attributes:
    attributes_txt.append(item.text)
del attributes_txt[1]

#print(stock_open)
summarry = soup.find_all('span',{'class':'primary'}) 

summarry_atb = []
summarry_atb.append(price.text)
#summarry_atb.append(stock_open.text)


with open('scraping/'+ stock +'_marketwatch.csv','w', newline='') as file:
    writer = csv.writer(file)
    print(attributes_txt)
    writer.writerow(attributes_txt)
    for item in summarry:
        summarry_atb.append(item.text)
    del summarry_atb[1:7]
    writer.writerow(summarry_atb)
    print(summarry_atb)
file.close()
    
