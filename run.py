import requests
import pandas as pd
from bs4 import BeautifulSoup
from pprint import pprint

url = 'https://en.wikipedia.org/wiki/List_of_mass_shootings_in_the_United_States'
req = requests.get(url)
page = BeautifulSoup(req.content, 'lxml')

tables = page.find_all('table', attrs={'class' : 'wikitable'})

shooting_list = []
for i, table in enumerate(tables):
    if i == 0:
        headings = []
        for heading in  table.find_all('th'):
            headings.append(heading.text.strip())
    
    for tr in table.find_all('tr'):
        data_row = []
        for td in tr.find_all('td'):
            data_row.append(td.text.strip())
        shooting_list.append(data_row)

df = pd.DataFrame(shooting_list, columns=[headings])
pprint(df.isna().sum())
        
