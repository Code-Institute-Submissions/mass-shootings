import os 

import requests
import time
import re
import pandas as pd
from bs4 import BeautifulSoup
from pprint import pprint
import plotly.express as px
import colorcet as cc
from geopy.geocoders import Nominatim

pd.options.plotting.backend = "plotly"
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 6)

dir = './data'
file_name = 'shootings.parquet'
file_path = os.path.join(dir, file_name)

df = pd.DataFrame()

def get_population():
    """
    Returns a dataframe containing population data from latest US Census (2019) per State.
    """
    census = pd.read_excel('https://www2.census.gov/programs-surveys/popest/tables/2010-2019/state/totals/nst-est2019-01.xlsx')
    census = census[8:59]
    census = census.reset_index(drop=True)
    census.columns=['State', 'a', 'b', 'c', 'd', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'Population']
    census = census.drop(['a', 'b', 'c', 'd', 'f', 'g', 'h', 'i', 'j', 'k', 'l'], axis=1)
    census['State'] = census['State'].str.strip('.')

    return census

def get_state (location):
    """
    Extracts and returns the State name for a given location. 
    Returns None if an error occurs
    """
    try:
        state = re.search(r'\w* \w+$', location).group(0).strip()
        return state
    except Exception as e:
        return None

def clean_date(date_str):
    """
    Returns date with only starting date of the event, removes bad formatting. 
    """
    try:
        date_str = date_str.replace('–', '-')
        pattern = '\s{0,1}((\-.*,)|(\-\d{0,2}))|(\-\s\w*\s)'
        date_str = re.sub(pattern, ',', date_str)
        pattern = '(?<=\d{4}), \d{4}'
        date_str = re.sub(pattern, '', date_str)
    except Exception as e:
        # print(e)
        pass
    
    return date_str

def clean_number(number):
    """
    Strips citation text and returns first number where more than 1 figure is present. 
    """
    pattern = r'\[\w\s\d*\]'
    number = re.sub(pattern, '', str(number))
    number = number.split('–')[0]
    number = number.split('+')[0]
    number = int(number)
    return number

def get_location(search):
    """
    Returns a tuple containting the latitude and longitude of the searched address
    or returns a tuple (None, None) if no location is found.
    """
    try:
        geolocator = Nominatim(user_agent="shootings")
        location = geolocator.geocode(search)
        print(location)
        time.sleep(1.2)
        return (location.latitude, location.longitude)
    except Exception as e:
        return (None, None)

def scrape_wikipedia(df):
    """
    Scrapes wikipedia page for list of US mass shootings.
    Stores the information in the dataframe passed as argument.
    """
    # Scrape Wikipedia for list of mass shootings in the US. 
    url = 'https://en.wikipedia.org/wiki/List_of_mass_shootings_in_the_United_States'
    req = requests.get(url)
    page = BeautifulSoup(req.content, 'lxml')
    # Select tables from all years containing the shooting. 
    tables = page.find_all('table', attrs={'class' : 'wikitable'})

    for table in tables:
        new_df = pd.read_html(str(table))
        new_df = new_df[0]
        if 'Events' not in new_df:
            df = pd.concat([df, new_df], ignore_index=True)

    # Data cleanup
    ## Format date column
    df['Date'] = df['Date'].apply(clean_date)
    df = df.query("Date != 'January 1923'")
    df['Date'] = pd.to_datetime(df['Date'])
    
    ## Remove wikipedia citation from numbers
    df['Dead'] = df['Dead'].apply(clean_number)
    df = df.query("Injured != 'unknown'")
    df['Injured'] = df['Injured'].apply(clean_number)
    df['Total'] = df['Total'].apply(clean_number)

    ## Exclude shootings with less than 3 victims and drop NaNs
    df = df.query('Total > 2')
    df = df.dropna()
    
    # Get latitude and longitude with geopy package
    df[['Latitude','Longitude']] = df['Location'].apply(get_location).apply(pd.Series)

    # Save dataframe to file
    df.to_parquet(file_path)
    return df

def calculate_rates_per_state(df):
    census = get_population()
    df['State'] = df['Location'].apply(get_state)
    df = df.dropna()
    result = df.groupby('State')[['Total', 'Dead']].sum()
    result = pd.merge(result, census, on=['State'], how='left')
    result = result.dropna()
    result['Victims_Per_1M'] = result['Total'] * 1_000_000 / result['Population']
    result['Deaths_Per_1M']  =  result['Dead'] * 1_000_000 / result['Population']
    return result

if os.path.exists(file_path):
    df = pd.read_parquet(file_path)
else:
    df = scrape_wikipedia(df)

print(calculate_rates_per_state(df))

