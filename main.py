####################################WEBSCRAPING SCRIPT MAIN############################################
#sys.argv[1] : GF -> Google Finance ; YF -> Yahoo Finance ; MKTW -> MarketWatch ; INV -> Investing;
#WSJ ->  The Wall Street Journal

import sys
from lib import googlefinance_scrape
from lib import wallstreet_scrape
from lib import invest_scraper
from lib import yahoo_finance_scraper
from lib import mw_webscrape



def main():
    
    if len(sys.argv) > 2:
        
        stock = sys.argv[1]
        market = sys.argv[2]
        
        if(market == "GF"):
            gf_scrape(stock)
        
        if(market == "YF"):
            print("teste")
        if(market == "MKTW"):
            print("teste")
        if(market == "INV"): 
            print("teste")
        if(market == "WSJ"): 
            print("teste")
    else:
        raise ValueError("USAGE: python3 main.py GF MCD")

#INVOKE MAIN
main()