import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from pprint import pprint
import plotly.express as px



pd.options.plotting.backend = "plotly"
pd.set_option('display.max_rows', 500)

# Scrape Wikipedia for list of mass shootings in the US. 
url = 'https://en.wikipedia.org/wiki/List_of_mass_shootings_in_the_United_States'
req = requests.get(url)
page = BeautifulSoup(req.content, 'lxml')

df = pd.DataFrame()

def clean_date(date_str):
    """
    Returns date with only starting date of the event, removes bad formatting. 
    """
    date_str = date_str.replace('–', '-')
    pattern = '\s{0,1}((\-.*,)|(\-\d{0,2}))|(\-\s\w*\s)'
    date_str = re.sub(pattern, ',', date_str)
    pattern = '(?<=\d{4}), \d{4}'
    date_str = re.sub(pattern, '', date_str)
    return date_str

def clean_number(number):
    """
    Strips citation text and returns first number where more than 1 figure is present. 
    """
    pattern = r'\[\w\s\d*\]'
    number = re.sub(pattern, '', str(number))
    number = number.split('–')[0]
    number = number.split('+')[0]
    number = float(number)
    return number

# Select tables from all years containing the shooting. 
tables = page.find_all('table', attrs={'class' : 'wikitable'})

for table in tables:
    new_df = pd.read_html(str(table))
    new_df = new_df[0]
    df = pd.concat([df, new_df], ignore_index=True)

# Data cleanup
df = df.drop(['Year', 'Events', 'Victims'], axis='columns')
df = df.dropna()
df['Date'] = df['Date'].apply(clean_date)
df = df.query("Date != 'January 1923'")
df['Date'] = pd.to_datetime(df['Date'])
df['Dead'] = df['Dead'].apply(clean_number)
df = df.query("Injured != 'unknown'")
df['Injured'] = df['Injured'].apply(clean_number)
df['Total'] = df['Total'].apply(clean_number)

# figure = px.scatter(x=df['Date'], y=df['Dead'])
# figure.show()
print(df.head())