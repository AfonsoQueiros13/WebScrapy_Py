from bs4 import BeautifulSoup
import urllib.request
#atributes of stock
attributes = ["Price","High","Low","MarketCap","Racio","Dividend","Close","High52","Close52"]

opener = urllib.request.build_opener()
opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36')]
response = opener.open('https://www.google.com/search?q=AAPL')

soup = BeautifulSoup(response, 'html.parser')
price = soup.find_all('span',{'class':'IsqQVc NprOob XcVN5d'})
high = soup.find_all('td[0]',{'class':'iyjjgb'}) 

with open('AAPL_googlefinance.csv', newline='') as csvfile:

print(price)
print(high)
