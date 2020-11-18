####################################WEBSCRAPING SCRIPT MAIN############################################
####################################AFONSO   QUEIROS    B2F#######################################################
#sys.argv[1] (FIRST ARGUMENT) : GF -> Google Finance ; YF -> Yahoo Finance ; MKTW -> MarketWatch ; INV -> Investing;
#WSJ ->  The Wall Street Journal
#sys.argv[2] (SECOND ARGUMENT) : P.E: MCD (MCDONNALDS); AAPL(APPLE)...


#REQUIRED LIB IMPORTS
import sys
from lib.googlefinance_scrape import gf_scrape
from lib.yahoo_finance_scraper import yf_scrape
from lib.mw_webscrape import mktw_scrape
from lib.invest_scraper import inv_scrape
from lib.wallstreet_scrape import wsj_scrape

#MAIN FUNCTION
def main():
    if len(sys.argv) > 2:
        
        market = sys.argv[1]
        stock = sys.argv[2]
        
        if(market == "GF"):
            gf_scrape(stock)
        
        elif(market == "YF"):
            yf_scrape(stock)
        
        elif(market == "MKTW"):
            mktw_scrape(stock)
        
        elif(market == "INV"): 
            inv_scrape(stock)

        elif(market == "WSJ"): 
            wsj_scrape(stock)
        
        else:
            print("Stock Market not recognized")
    
    else:
        raise ValueError("USAGE: python3 main.py ""INITIALS FOR STOCK"" ""INITIALS FOR MARKET"", for example : python3 AAPL WSJ")

#INVOKE MAIN
if __name__ == "__main__":
    main()
