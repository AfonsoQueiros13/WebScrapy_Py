from bs4 import BeautifulSoup
import urllib.request
import csv
import sys

stock = sys.argv[1]

#atributes of stock
attributes = ["Price","Volume", "65 Day Volume Avg","Day Range","52 Week Range","Open",
"Close","PE Ratio","EPS", "MarketCap", "Shares Outstanding", "Public Float", 
"Yield","Lastest Dividend", "Ex-Dividend Rate", "Shares Sold Short", "Changes from last", "Percent of float"]

opener = urllib.request.build_opener()
opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36')]
response = opener.open('https://www.wsj.com/market-data/quotes/'+stock)

soup = BeautifulSoup(response, 'html.parser')
#price = soup.find ('h3',{'class':'intraday__price'})
price = soup.find ('span',{'id':'quote_val'})

print(price.text)
summarry = soup.find_all('span',{'class':'WSJTheme--data_data--2QuzEiZE'}) 

key_data = soup.find_all('span',{'class':'WSJTheme--data_data--3CZkJ3RI'}) 
#print (summarry)
summarry_atb = []
summarry_atb.append(price.text)


with open('scraping/'+ stock +'_thewallstreet.csv','w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(attributes)
    #writer.writerow(price)
    for item in summarry:
       summarry_atb.append(item.text)
    for item in key_data:
       summarry_atb.append(item.text)
    summarry_atb.pop(1)
    print(summarry_atb)
    writer.writerow(summarry_atb)
file.close()
