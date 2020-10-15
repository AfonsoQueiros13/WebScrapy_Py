from bs4 import BeautifulSoup
import urllib.request
import csv
import sys

stock = sys.argv[1]

#atributes of stock
attributes = ["Price","Open", "Day Range","52 Week","MarketCap",
"Racio","Shares Outst.","Public Float","Beta",
"Rev per empl.","PE Ratio","EPS","Yield","Dividend","Ex Divid Rate","Short Interest",
"Percent of Float Shorted","Avg volume"]

opener = urllib.request.build_opener()
opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36')]
response = opener.open('https://www.marketwatch.com/investing/stock/'+stock)

soup = BeautifulSoup(response, 'html.parser')
#price = soup.find ('h3',{'class':'intraday__price'})
price = soup.find ('bg-quote',{'class':'value'})

print(price.text)
summarry = soup.find_all('span',{'class':'primary'}) 
print (summarry)
summarry_atb = []
summarry_atb.append(price.text)


with open('scraping/'+ stock +'_marketwatch.csv','w', newline='') as file:
    writer = csv.writer(file,delimiter="|")
    writer.writerow(attributes)
    #writer.writerow(price)
    for item in summarry:
       summarry_atb.append(item.text)
    del summarry_atb[1:3]
    writer.writerow(summarry_atb)
file.close()
