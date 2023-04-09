# Filesystem and Dashboard App
import os
import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc

# Requests, time and regex
import requests
import time
import re

from pprint import pprint

# Scraping and Data Structures
import pandas as pd
from bs4 import BeautifulSoup

# Plotting
import plotly.express as px
from geopy.geocoders import Nominatim

# Basic Settings
pd.options.plotting.backend = "plotly"
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 10)

COLOR_SCALE = ['#f48c06', '#e85d04', '#dc2f02','#d00000', '#9d0208', '#6a040f']

# File path to save scraped data to disk
dir_name = './data'
file_name = 'shootings.parquet'
file_path = os.path.join(dir_name, file_name)

shootings_df = pd.DataFrame()

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

    # census = pd.read_csv('data/apportionment.csv')
    # census = census[['Name', 'Year', 'Resident Population']]
    # census = census.groupby('Year').value_counts().reset_index()
    # census = census.rename(columns={'Name': 'State', 'Resident Population': 'Population'})
    return census

def get_state (location):
    """
    Extracts and returns the State name for a given location. 
    Returns None if an error occurs
    """
    try:
        state = re.search(r'\w* \w+$', location).group(0).strip()
        return state
    except AttributeError:
        return None

def clean_date(date_str):
    """
    Returns date with only starting date of the event, removes bad formatting. 
    """
    try:
        date_str = date_str.replace('–', '-')
        pattern = r'\s{0,1}((\-.*,)|(\-\d{0,2}))|(\-\s\w*\s)'
        date_str = re.sub(pattern, ',', date_str)
        pattern = r'(?<=\d{4}), \d{4}'
        date_str = re.sub(pattern, '', date_str)
    except Exception as e:
        print(e)

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
    except Exception:
        return (None, None)

def scrape_wikipedia(shootings_df):
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
            shootings_df = pd.concat([shootings_df, new_df], ignore_index=True)

    # Data cleanup
    ## Format date column
    shootings_df['Date'] = shootings_df['Date'].apply(clean_date)
    shootings_df = shootings_df.query("Date != 'January 1923'")
    shootings_df['Date'] = pd.to_datetime(shootings_df['Date'])
    
    ## Remove wikipedia citation from numbers
    shootings_df['Dead'] = shootings_df['Dead'].apply(clean_number)
    shootings_df = shootings_df.query("Injured != 'unknown'")
    shootings_df['Injured'] = shootings_df['Injured'].apply(clean_number)
    shootings_df['Total'] = shootings_df['Total'].apply(clean_number)

    ## Exclude shootings with less than 3 victims and drop NaNs
    shootings_df = shootings_df.query('Total > 2')
    shootings_df = shootings_df.dropna()
    
    # Get latitude and longitude with geopy package
    shootings_df[['Latitude','Longitude']] = shootings_df['Location'].apply(get_location).apply(pd.Series)

    # Save dataframe to file
    shootings_df.to_parquet(file_path)
    return shootings_df

def get_rates_per_state(shootings_df):
    census = get_population()
    shootings_df['State'] = shootings_df['Location'].apply(get_state)
    shootings_df = shootings_df.dropna()
    result = shootings_df.groupby('State')[['Total', 'Dead']].sum()
    result = pd.merge(result, census, on=['State'], how='left')
    result = result.dropna()
    # print(result.head(20))
    # exit()
    result['Victims_Per_1M'] = result['Total'] * 1_000_000 / result['Population']
    result['Deaths_Per_1M']  =  result['Dead'] * 1_000_000 / result['Population']
    result.sort_values(by=['State'])
    return result

def get_shootings_by_month(df):
    """
    Returns a DataFrame containing a count of shooting incidents grouped by month.
    """
    df['Month'] = df['Date'].dt.month_name()
    df['Month_Number'] = df['Date'].dt.month

    result = df.groupby(['Month', 'Month_Number']).size().to_frame('Shootings').reset_index()
    result = result.sort_values(by=['Month_Number']).reset_index(drop=True)
    
    return result  

if os.path.exists(file_path):
    shootings_df = pd.read_parquet(file_path)
else:
    shootings_df = scrape_wikipedia(shootings_df)

rates = get_rates_per_state(shootings_df)

month = get_shootings_by_month(shootings_df)

scatter_map = px.scatter_mapbox(shootings_df, 
                                lat="Latitude", 
                                lon="Longitude", 
                                color="Dead",
                                color_continuous_scale=COLOR_SCALE,
                                range_color=[0, 65],
                                hover_name='Location',
                                hover_data={'Latitude': False,
                                            'Longitude': False,
                                            'Dead': True,
                                            'Total': True },
                                labels={
                                    'Total': 'Total Victims',
                                    "Dead": "Fatal Victims",
                                    "Injured": "Non-Fatal Victims"
                                },
                                zoom=2.5, 
                                mapbox_style='open-street-map', 
                                size="Dead",
                                height=500)

rates_plot = px.bar(rates, 
                    x='Deaths_Per_1M',
                    y='State',
                    hover_name='State',
                    color='Deaths_Per_1M',
                    color_continuous_scale=COLOR_SCALE,
                    range_color=[0, 30],
                    # range_y=[-2, 51],
                    labels={
                     "Deaths_Per_1M": "Deaths Per 1M",
                     "State": "State"
                    },
                    height=700)

rates_plot.update_layout(yaxis=dict(automargin=True))

month_plot = px.bar(month[::-1], 
                    y='Month',
                    x='Shootings',
                    text_auto='.2s',
                    color='Shootings',
                    color_continuous_scale=COLOR_SCALE,
                    range_color=[20, 40],
                    range_y=[-0.5, 11.5],
                    labels={
                     "Month": "Month",
                     "Shootings": " Number of Shooting Incidents"
                    }, 
                    height=700)

plots = [scatter_map, rates_plot, month_plot]

for plot in plots:
    plot.update(layout_coloraxis_showscale=False)
    plot.update_layout(margin=dict(l=10, t=10, r=10, b=10))

# Create dashboard with all three plots using
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

start_year = shootings_df['Date'].tail(1).dt.year.item()
end_year = shootings_df['Date'].head(1).dt.year.item()


layout = dbc.Container(
    [
        html.Div([
            html.Div([
                html.H1(f'Mass shootings in the US from {start_year} to {end_year} From Wikipedia', className='display-6 mb-4'),
                html.Div([
                    html.Div([
                        html.Div([
                            html.H5('Map of shootings\' approximate locations', className='card-title'),
                            dcc.Graph(id='scatter-map', figure=scatter_map)
                        ], className='card-body')
                    ], className='card')
                ], className='col-12')                
            ], className='row p-3'),
            html.Div([
                html.Div([
                    html.Div([
                        html.Div([
                            html.H5('Death rate per 1M', className='card-title'), 
                            dcc.Graph(id='rates-plot', figure=rates_plot)
                        ], className='card-body')
                    ], className='card')
                ], className='col-6'),

                html.Div([
                    html.Div([
                        html.Div([
                            html.H5('Shooting Incidents by Month', className='card-title'), 
                            dcc.Graph(id='month-plot', figure=month_plot)
                        ], className='card-body')
                    ], className='card')
                ], className='col-6')
            ], className='row p-3')
        ])
        
    ], 
    fluid=True
)


# layout = html.Div([
#                         html.H1(f'Mass shootings in the US from {start_year} to {end_year}'),
#                         html.Div([
#                             html.H2('Map of shootings\' approximate locations'), 
#                             dcc.Graph(id='scatter-map', figure=scatter_map)
#                         ]),

#                         html.Div([
#                             html.H2('Deaths Per Million By State'), 
#                             dcc.Graph(id='rates-plot', figure=rates_plot)
#                         ]),

#                         html.Div([
#                             html.H2('Shooting Incidents by Month of occurrence'), 
#                             dcc.Graph(id='month-plot', figure=month_plot)
#                         ])
#                     ])

app.layout = layout

if __name__ == '__main__':
    app.run_server(debug=True)