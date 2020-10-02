import requests
from bs4 import BeautifulSoup

base_url = 'https://finviz.com/screener.ashx?v=152&o=price&t=MSFT,AAPL,SBUX,S,GOOG&o=price&c=0,1,2,3,4,5,6,7,8,9,25,63,64,65,66,67'
html = requests.get(base_url)
soup = BeautifulSoup(html.content, "html.parser")
main_div = soup.find('div', attrs = {'id':'screener-content'})

light_rows = main_div.find_all('tr', class_="table-light-row-cp")
dark_rows = main_div.find_all('tr', class_="table-dark-row-cp")
values = [light_rows.text.strip() for ele in light_rows]
print(values)
data = []
for rows_set in (light_rows, dark_rows):
    for row in rows_set:
        row_data = []
        for cell in row.find_all('td'):
            val = cell.a.get_text()
            row_data.append(val)
        data.append(row_data)

#   sort rows to maintain original order
data.sort(key=lambda x: int(x[0]))

import pandas
pandas.DataFrame(data).to_csv("C:\\your_path\\AAA.csv", header=False)