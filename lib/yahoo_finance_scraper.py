from lib.functions import *
import sys

def yf_scrape(stock):

    # dates management....
    today, date_1, code_1, date_2, code_2 = Dates_man()

    # URL generation for querying web....
    url=urlGenerator(code_1, code_2, stock)

    #print(url[0])
    #print(url[1])
    #print(url[2])


    # web scraping & HTML parsing
    data=scraping(url[0])

    # web scraping & HTML f.seek(0)
    # convert dates from original codes
    code2Dates(date_1, code_1, data)

    #all=np.chararray.replace(all,".0\n","\n")
    #all=np.chararray.replace(all,",  ",", ")

    # adjust file for better viewing & save final data
    saveResult(today, date_2, stock)
    print("done!") 