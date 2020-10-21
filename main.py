####################################WEBSCRAPING SCRIPT MAIN############################################
#sys.argv[1] : GF -> Google Finance ; YF -> Yahoo Finance ; MKTW -> MarketWatch ; INV -> Investing;
#WSJ ->  The Wall Street Journal

import sys
from lib.googlefinance_scrape import gf_scrape
from lib.yahoo_finance_scraper import yf_scrape
from lib.mw_webscrape import mktw_scrape
from lib.invest_scraper import inv_scrape
from lib.wallstreet_scrape import wsj_scrape

def main():
    
    if len(sys.argv) > 2:
        
        stock = sys.argv[1]
        market = sys.argv[2]
        
        if(market == "GF"):
            gf_scrape(stock)
        
        if(market == "YF"):
            yf_scrape(stock)
        
        if(market == "MKTW"):
            mktw_scrape(stock)
        
        if(market == "INV"): 
            inv_scrape(stock)

        if(market == "WSJ"): 
            wsj_scrape(stock)
    
    else:
        raise ValueError("USAGE: python3 main.py GF MCD")

#INVOKE MAIN
if __name__ == "__main__":
    main()
