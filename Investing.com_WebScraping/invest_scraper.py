import investpy
import sys
i = 0

if len(sys.argv) > 1:
    df = investpy.get_stock_historical_data(stock=' '.join(sys.argv[1:]),
                                        country='United States',
                                        from_date='30/09/2020',
                                        to_date='01/10/2020')
    print(df.head().to_string()) 
    output_file = "scraping/" + sys.argv[1] + "_invest.csv"
    print(df.head().to_csv(output_file))

else:
    raise ValueError("After runnig type prefix of stock ( Apple : AAPL)!")


