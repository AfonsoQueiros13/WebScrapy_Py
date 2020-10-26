import investpy
import sys
i = 0

def inv_scrape(stk):
    df = investpy.get_stock_historical_data(stock=stk,
                                        country='United States',
                                        from_date='30/09/2020',
                                        to_date='01/10/2020')
    print(df.head().to_string()) 
    output_file = "scraping_inv/" + stk + "_invest.csv"
    print(df.head().to_csv(output_file))


