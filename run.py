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
import calendar

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
# pd.set_option('display.max_columns', 6)

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

def clean_address(address):
    pattern = r'\d+.\W*([B|b]lock of)\W'
    print(address)
    try:
        address = re.sub(pattern, '', address)
    except Exception as e:
        print(address)

    return address

def get_location(search):
    """
    Returns a tuple containting the latitude and longitude of the searched address
    or returns a tuple (None, None) if no location is found.
    """
    if search != 'nan':
        geolocator = Nominatim(user_agent="shootings")
        
        try:
            location = geolocator.geocode(search)
            time.sleep(1)
            print((location.latitude, location.longitude))
            return (location.latitude, location.longitude)
        except Exception as error:
            try:
                search = search.split(',')[1:3]
                return get_location(search)
            except Exception as error:
                print(error)
    return (None, None)

def get_shootings(df):
    """
    Returns a pandas DataFrame containning mass shootings in the US. 
    1. Loads data from 'data/mass_shootings.csv' file. 
    2. Searches coordinates for locations of the incidents. 
    3. Cleans up and prepares the data for analysis
    """

    df = pd.read_csv('data/gun_violence.csv')

    # Filter shootings with more than 3 victims, drop unnecessary columns and missing values.
    df = df.query('n_killed + n_injured > 3')
    df = df.drop(['incident_id', 'incident_url', 'source_url','incident_url_fields_missing',
                  'congressional_district', 'gun_stolen','incident_characteristics',
                  'notes', 'participant_name', 'participant_relationship', 'participant_status',
                  'participant_type', 'sources', 'state_house_district', 'state_senate_district', 
                  'location_description', 'participant_age', 'participant_age_group',
                  'participant_gender', 'n_guns_involved', 'gun_type' ],
                  axis=1)
    df = df.dropna()
    
    # Data cleanup and pre processing
    df['date'] = pd.to_datetime(df['date']).dt.date
    df = df.sort_values(by='date')
    df['total'] = df['n_killed'] + df['n_injured']
    
    # Clean address string and get latitude and longitude with geopy package
    df['full_address'] = df['address'] + ', ' + df['city_or_county'] + ', ' + df['state']

    # Save dataframe to file
    df.to_parquet(file_path)
    return df

def get_shootings_by_state(df):
    """
    Returns a DataFrame containing counting of shootings grouped by state.
    """
    result = df.groupby(['state']).size().to_frame('shootings').reset_index()
    # result = result.sort_values(by=['Month_Number']).reset_index(drop=True)
    return result

def get_shootings_by_month(df):
    """
    Returns a DataFrame containing a count of shooting incidents grouped by month.
    """
    
    df['month_name'] = df['date'].apply(lambda x: calendar.month_name[x.month])
    df['month_number'] = df['date'].apply(lambda x: x.month)

    result = df.groupby(['month_name', 'month_number']).size().to_frame('shootings').reset_index()
    result = result.sort_values(by=['month_number']).reset_index(drop=True)
    
    return result


if os.path.exists(file_path):
    shootings_df = pd.read_parquet(file_path)
else:
    shootings_df = get_shootings(shootings_df)


state_df = get_shootings_by_state(shootings_df)

month_df = get_shootings_by_month(shootings_df)

scatter_map = px.scatter_mapbox(shootings_df, 
                                lat="latitude", 
                                lon="longitude", 
                                color="n_killed",
                                color_continuous_scale=COLOR_SCALE,
                                range_color=[shootings_df['n_killed'].min(), shootings_df['n_killed'].max()],
                                hover_name='full_address',
                                hover_data={'latitude': False,
                                            'longitude': False,
                                            'n_killed': True,
                                            'total': True },
                                labels={
                                    'total': 'Total Victims',
                                    "n_killed": "Fatal Victims",
                                    "n_injured": "Non-Fatal Victims"
                                },
                                zoom=3, 
                                mapbox_style='open-street-map', 
                                size="total",
                                height=500)

state_plot = px.bar(state_df, 
                    x='shootings',
                    y='state',
                    hover_name='state',
                    color='shootings',
                    color_continuous_scale=COLOR_SCALE,
                    range_color=[state_df['shootings'].min(), state_df['shootings'].max()],
                    # range_y=[-2, 51],
                    labels={
                     "shootings": "Shootings by State",
                    },
                    height=800)

state_plot.update_layout(yaxis=dict(automargin=True))

month_plot = px.bar(month_df[::-1], 
                    y='month_name',
                    x='shootings',
                    text_auto='.2s',
                    color='shootings',
                    color_continuous_scale=COLOR_SCALE,
                    range_color=[month_df['shootings'].min(), month_df['shootings'].max()],
                    range_y=[-0.5, 11.5],
                    labels={
                     "month_name": "Month",
                     "shootings": " Number of Shooting Incidents"
                    }, 
                    height=800)

plots = [scatter_map, state_plot, month_plot]

for plot in plots:
    plot.update(layout_coloraxis_showscale=False)
    plot.update_layout(margin=dict(l=10, t=10, r=10, b=10))

# Create dashboard with all three plots using
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

start_year = shootings_df['date'].head(1).item().year
end_year = shootings_df['date'].tail(1).item().year


layout = dbc.Container(
    [
        html.Div([
            html.Div([
                html.H1(f'Mass shootings in the US from {start_year} to {end_year}', className='display-6 mb-4'),
                html.Div([
                    html.Div([
                        html.Div([
                            html.H5('Shootings Map', className='card-title'),
                            dcc.Graph(id='scatter-map', figure=scatter_map)
                        ], className='card-body')
                    ], className='card')
                ], className='col-12')                
            ], className='row p-3'),
            html.Div([
                html.Div([
                    html.Div([
                        html.Div([
                            html.H5('Shootings by State', className='card-title'), 
                            dcc.Graph(id='rates-plot', figure=state_plot)
                        ], className='card-body')
                    ], className='card')
                ], className='col-6'),

                html.Div([
                    html.Div([
                        html.Div([
                            html.H5('Shooting by Month', className='card-title'), 
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