from lxml \
import html #
import requests #import librarie
import xlrd, xlwt 
import sys


print('Stock ticket: ', sys.argv[1:])
url = ("https://www.marketwatch.com/investing/stock/%s" %sys.argv[1:])
urlopen = requests.get(url)
tree = html.fromstring(urlopen.content)
price = tree.xpath("//div[@class='quotedisplay tenwidequote pre']/table/div[5]/div/p/p/text()")
print(price)
description = tree.xpath('//p[@class="description__text"]/text()')


low_high = tree.xpath('//span[@class="kv__value kv__primary "]/text()')
#print('Description: ', description[0])
print('Current stock price: $',price[0])
print('Open stock price: $', low_high[0])
print('Todays high and low prices: Low', low_high[1], 'High')


