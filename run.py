# Filesystem and Dashboard App
import os
import dash
from dash import html, dcc, Input, Output, State

import dash_bootstrap_components as dbc

# Requests, time and regex
import requests
import time
from datetime import date
import re
import calendar

from pprint import pprint

# Scraping and Data Structures
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# Plotting
import plotly.express as px
from geopy.geocoders import Nominatim

# Basic Settings
pd.options.plotting.backend = "plotly"
pd.set_option('display.max_rows', 500)

COLOR_SCALE = ['#e85d04', '#dc2f02','#d00000', '#9d0208', '#6a040f', '#370617']

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
    geolocator = Nominatim(user_agent="shootings")
    try:
        location = geolocator.geocode(search)
    except Exception as e:
        print(e)    
    
    geolocator.geocode(search)
    return location


def get_shootings():
    """
    Returns a pandas DataFrame containning mass shootings in the US. 
    1. Loads data from 'data/mass_shootings.csv' file. 
    2. Searches coordinates for locations of the incidents. 
    3. Cleans up and prepares the data for analysis
    """
    if os.path.exists(file_path):
        return pd.read_parquet(file_path)
    
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

# PLOTS
## MAP PLOT
def get_map_plot(df):
    plt = px.scatter_mapbox(
        df, 
        lat="latitude", 
        lon="longitude", 
        color="n_killed",
        color_continuous_scale=COLOR_SCALE,
        range_color=[df['n_killed'].min(), df['n_killed'].max()],
        hover_name='full_address',
        hover_data={
            'latitude': False,
            'longitude': False,
            'n_killed': True,
            'total': True 
        },
        labels={
            'total': 'Total Victims',
            "n_killed": "Fatal Victims",
            "n_injured": "Non-Fatal Victims"
        },
        zoom=3, 
        mapbox_style='open-street-map', 
        size="total",
        height=500
    )
    plt.update_layout(mapbox_style="carto-darkmatter")
    return plt

## BAR PLOT BY STATE
def get_state_plot(df):
    plt = px.bar(
        df, 
        x='shootings',
        y='state',
        hover_name='state',
        color='shootings',
        color_continuous_scale=COLOR_SCALE,
        range_color=[df['shootings'].min(), df['shootings'].max()],
        labels={
            "shootings": "Shootings by State",
        },
        height=800
    )
    plt.update_layout(yaxis=dict(automargin=True))
    return plt    

## BAR PLOT BY MONTH
def get_month_plot(df):
    plt = px.bar(
        df[::-1], 
        y='month_name',
        x='shootings',
        text_auto='.2s',
        color='shootings',
        color_continuous_scale=COLOR_SCALE,
        range_color=[df['shootings'].min(), df['shootings'].max()],
        range_y=[-0.5, 11.5],
        labels={
            "month_name": "Month",
            "shootings": " Number of Shooting Incidents"
        }, 
        height=800
    )
    return plt


# BUILD DATAFRAMES
shootings_df = get_shootings()
state_df = get_shootings_by_state(shootings_df)
month_df = get_shootings_by_month(shootings_df)


# BUILD PLOTS
state_plot = get_state_plot(state_df)
scatter_map = get_map_plot(shootings_df)
month_plot = get_month_plot(month_df)

plots = [scatter_map, state_plot, month_plot]

for plot in plots:
    plot.update(layout_coloraxis_showscale=False)
    plot.update_layout(margin=dict(l=10, t=10, r=10, b=10))

# Creates a dashboard app using Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server


start_year = shootings_df['date'].head(1).item().year
end_year = shootings_df['date'].tail(1).item().year

today = date.today()
day = int(today.strftime('%d'))
month = int(today.strftime('%m'))
year = int(today.strftime('%Y'))

# USER INPUT FORM COMPONENTS
date_picker = dcc.DatePickerSingle(
    id='form-date',
    display_format='DD/MM/YYYY',
    min_date_allowed=date(1900, 1, 1),
    max_date_allowed=date(
        year, 
        month, 
        day
    ),
    initial_visible_month=date(
        year, 
        month, 
        day    
    )
)

input_address = dbc.FormFloating([
    dbc.Input(id='form-address', type='text', placeholder='Address', size='sm', required=True),
    dbc.Label('Address', size='sm'),
])

input_injured = dbc.FormFloating([
    dbc.Input(id='form-injured', type='number', placeholder='Injured', size='sm', step='1', required=True),
    dbc.Label('Injured', size='sm'),
])

input_killed = dbc.FormFloating([
    dbc.Input(id='form-killed', type='number', placeholder='Killed', size='sm', step='1', required=True),
    dbc.Label('Killed', size='sm')
])

btn_save = dbc.Button('Save', color='danger', outline=True, className='me-1 btn-lg', id='form-save')


# DASHBOARD LAYOUT
layout = dbc.Container(
    [
        html.Div([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Record Shooting", className="card-title d-inline"),
                    
                    dbc.Row([
                        dbc.Col([
                            date_picker
                            
                        ], className='col-12')
                    ], className='mb-3 mt-3'),
                    dbc.Row([
                        dbc.Col([
                            input_address
                        ], className='d-flex align-items-center col-2'),
                        dbc.Col([
                            input_injured
                        ], className='d-flex align-items-center justify-content-center col-4'),
                        dbc.Col([
                            input_killed
                        ], className='d-flex align-items-center justify-content-center col-4'),
                        dbc.Col([
                            btn_save
                        ], className='d-flex align-items-center justify-content-end col-2')
                    ])   
                ]),
                dbc.Alert(children='', color='warning', id='form-alert', is_open=False, duration=4000, className='m-2' ),
            ], className='m-3 d-inline-block'),
            

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
                            html.H5('Number of Shootings by State', className='card-title'), 
                            dcc.Graph(id='state-plot', figure=state_plot)
                        ], className='card-body')
                    ], className='card')
                ], className='col-6'),

                html.Div([
                    html.Div([
                        html.Div([
                            html.H5('Number of Shooting by Month', className='card-title'), 
                            dcc.Graph(id='month-plot', figure=month_plot)
                        ], className='card-body')
                    ], className='card')
                ], className='col-6')
            ], className='row p-3')
        ])
        
    ], fluid=True)

app.layout = layout

# USER INPUT HANDLER
@app.callback(
    Output(component_id='form-alert', component_property='children'),
    Output(component_id='form-alert', component_property='is_open'),
    
    Output(component_id='scatter-map', component_property='figure'),
    Output(component_id='state-plot',  component_property='figure'),
    Output(component_id='month-plot',  component_property='figure'),

    Input(component_id='form-save', component_property='n_clicks'),
    [State(component_id='form-date', component_property='date')],
    [State(component_id='form-address', component_property='value')],
    [State(component_id='form-injured', component_property='value')],
    [State(component_id='form-killed', component_property='value')],
    prevent_initial_call=True
)
def validate_form (n_clicks, date_value, address_value, injured_value, killed_value):

    fields = [date_value, address_value, injured_value, killed_value]

    for field in fields:
        if field is None:
            return 'All fields are required.', True
    
    location = get_location(address_value)

    location_arr = location[0].split(',')
    
    address = location_arr[0] + location_arr[1]
    city_or_county = location_arr[-4].strip()
    state = location_arr[-3].strip()
    country = location_arr[-1].strip()
    full_address = f'{address}, {city_or_county}, {state}, {country}'

    if(country.lower() != 'united states' and 
       country.lower() != 'usa' and
       country.lower() != 'u.s.a.' and 
       country.lower() != 'united states of america'):
       return 'Address must be in the United States.', True

    injured_value = int(injured_value)
    killed_value = int(killed_value)
    total = injured_value + killed_value

    date_value = pd.to_datetime(date_value).date()

    month_value = calendar.month_name[date_value.month]
    month_number_value = date_value.month

    row = {
        'date': [date_value], 
        'state': [state], 
        'city_or_county': [city_or_county], 
        'address': [address], 
        'n_injured': [injured_value], 
        'n_killed': [killed_value],
        'latitude': [location.latitude], 
        'longitude': [location.longitude], 
        'total': [total], 
        'full_address': [full_address], 
        'month_name': [month_value], 
        'month_number': [month_number_value]
    }

    row = pd.DataFrame(row)
    df = pd.concat([shootings_df, row], ignore_index=True)
    df.reset_index()

    print(row)
    print(file_path)
    df.to_parquet(file_path)

    print(df.tail(2))
    state_df = get_shootings_by_state(df)
    month_df = get_shootings_by_month(df)

    return '', False, get_map_plot(df), get_state_plot(state_df), get_month_plot(month_df)

if __name__ == '__main__':
    app.run_server(debug=True)