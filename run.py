# Filesystem and Dashboard App
import os
import dash
from dash import html, dcc, Input, Output, State, dash_table

import dash_bootstrap_components as dbc

# Date time and calendar
from datetime import date
import calendar

# Data Manipulation
import pandas as pd

# Plotting
import plotly.express as px
from geopy.geocoders import Nominatim

# Basic Settings
pd.options.plotting.backend = "plotly"

COLOR_SCALE = [
    '#e85d04',
    '#dc2f02',
    '#d00000',
    '#9d0208',
    '#6a040f',
    '#370617'
]

# File path to save scraped data to disk
dir_name = './data'
file_name = 'shootings.csv'
file_path = os.path.join(dir_name, file_name)

global shootings_df


def get_population():
    """
    Returns a dataframe containing population
    data from latest US Census (2019) per State.
    """
    census = pd.read_excel('https://www2.census.gov/programs-surveys/popest/\
                            tables/2010-2019/state/totals/nst-est2019-01.xlsx')
    census = census[8:59]
    census = census.reset_index(drop=True)
    census.columns = ['State', 'a', 'b', 'c', 'd', 'f',
                      'g', 'h', 'i', 'j', 'k', 'l',
                      'Population']
    census = census.drop(['a', 'b', 'c', 'd', 'f', 'g',
                          'h', 'i', 'j', 'k', 'l'], axis=1)
    census['State'] = census['State'].str.strip('.')

    return census


def get_location(search):
    """
    Returns a tuple containting the
    latitude and longitude of the searched address
    or returns a tuple (None, None) if no location is found.
    """
    geolocator = Nominatim(user_agent="shootings")
    try:
        location = geolocator.geocode(search)
    except Exception as e:
        pass

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
        df = pd.read_csv(file_path)
        df = remove_unnamed_column(df)
    else:
        df = pd.read_csv('data/gun_violence.csv')
        df = df.query('n_killed + n_injured > 3')
        df = df[['date', 'state', 'city_or_county', 'address',
                'n_killed', 'n_injured', 'latitude', 'longitude']]
        df = df.dropna()
        df = df.reset_index(drop=True)
        df.to_csv(file_path)

    # Data cleanup and pre processing
    df['date'] = pd.to_datetime(df['date']).dt.date
    df = df.sort_values(by='date')
    df['total'] = df['n_killed'] + df['n_injured']

    # Clean address string and get latitude and longitude with geopy package
    df['full_address'] = df['address'] + ', ' +\
        df['city_or_county'] + ', ' + df['state']

    # Save dataframe to file
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
    Returns a DataFrame containing a count
    of shooting incidents grouped by month.
    """
    df['month_name'] = df['date'].apply(lambda x: calendar.month_name[x.month])
    df['month_number'] = df['date'].apply(lambda x: x.month)

    result = df.groupby(['month_name', 'month_number']).size().\
        to_frame('shootings').reset_index()
    result = result.sort_values(by=['month_number']).reset_index(drop=True)

    return result


def remove_unnamed_column(df):
    if df.columns[0].find('Unnamed') >= 0:
        df = df.iloc[:, 1:]
    return df


# PLOTS
# MAP PLOT
def get_map_plot(df):
    """
    Generates a map plot using the dataframe given.
    """
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
        height=600
    )
    plt.update_layout(mapbox_style="carto-darkmatter")
    plt.update(layout_coloraxis_showscale=False)
    plt.update_layout(margin=dict(l=0, t=0, r=0, b=0))
    return plt


# LINE PLOT BY DAY
def get_day_plot(df):
    """
    Generates a line plot by day using the dataframe given.
    """
    grouped_df = df.groupby('date')['date'].size()\
        .reset_index(name='count')
    plt = px.scatter(
        grouped_df,
        'date',
        'count',
        color='count',
        color_continuous_scale=COLOR_SCALE,
        range_color=[grouped_df['count'].min(), grouped_df['count'].max()],
        range_y=[0, grouped_df['count'].max() + 1],
        labels={
            "date": "Days",
            "count": "Number of Shooting Incidents"
        },
        height=600)
    plt.update_layout(margin=dict(l=0, t=0, r=0, b=0))
    plt.update(layout_coloraxis_showscale=False)
    return plt


# BAR PLOT BY MONTH
def get_month_plot(df):
    """
    Generates a bar plot by month using the dataframe given.
    """
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
        height=600
    )
    plt.update(layout_coloraxis_showscale=False)
    plt.update_layout(margin=dict(l=0, t=0, r=0, b=0))
    return plt


# BAR PLOT BY STATE
def get_state_plot(df):
    """
    Generates a bar plot by state using the dataframe given.
    """
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
            "state": "State",
        },
        height=800
    )
    plt.update_layout(yaxis=dict(automargin=True))
    plt.update(layout_coloraxis_showscale=False)
    plt.update_layout(margin=dict(l=0, t=0, r=0, b=0))
    return plt


# BUILD DATAFRAMES
shootings_df = get_shootings()
state_df = get_shootings_by_state(shootings_df)
month_df = get_shootings_by_month(shootings_df)

# BUILD PLOTS
scatter_map = get_map_plot(shootings_df)
day_plot = get_day_plot(shootings_df)
month_plot = get_month_plot(month_df)
state_plot = get_state_plot(state_df)
# CREATES DASH APP
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.PULSE])
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
    dbc.Input(id='form-address', type='text',
              placeholder='Address', required=True),
    dbc.Label('Address'),
])

input_injured = dbc.FormFloating([
    dbc.Input(id='form-injured', type='number',
              placeholder='Injured', step='1', required=True),
    dbc.Label('Injured'),
])

input_killed = dbc.FormFloating([
    dbc.Input(id='form-killed', type='number',
              placeholder='Killed', step='1', required=True),
    dbc.Label('Killed')
])

btn_save = dbc.Button('Save', color='danger',
                      className='me-1 btn-lg', id='form-save')


# DASHBOARD PAGE LAYOUT
def create_tab(components):
    """
    Generates a Dash tab component with the
    list of components passed as argument.
    """
    return (
        dbc.Card([
            dbc.CardBody([
                html.Div(components)
            ], className='card-body')
        ], className='card')
    )


def create_table(df):
    return dash_table.DataTable(
            df[::-1].to_dict('records'),
            [{"name": i, "id": i} for i in df.columns],
            style_cell={
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': 0
            },
            row_deletable=True,
            id='shootings-table'
        )


# MAP TAB
map_tab = create_tab([
    html.H5('Shooting Locations', className='card-title'),
    dcc.Graph(id='scatter-map', figure=scatter_map)
])

# TIMELINE TAB
day_tab = create_tab([
    html.H5('By Day', className='card-title'),
    dcc.Graph(id='day-plot', figure=day_plot)
])

# STATE TAB
state_tab = create_tab([
    html.H5('Number of Shootings by State', className='card-title'),
    dcc.Graph(id='state-plot', figure=state_plot)
])

# MONTH TAB
month_tab = create_tab([
    html.H5('Number of Shootings by Month', className='card-title'),
    dcc.Graph(id='month-plot', figure=month_plot)
])

# RECORDS TAB
records_tab = html.Div([
    dbc.Card([
        dbc.CardBody([
            html.H4("Add New Shooting", className="card-title"),

            dbc.Row([
                dbc.Col([
                    date_picker
                ], className='col-12')
            ], className='mb-3 mt-3'),

            dbc.Row([
                dbc.Col([
                    input_address
                ], className='col-7'),
                dbc.Col([
                    input_injured
                ], className='d-flex align-items-center \
                              justify-content-center col-2'),
                dbc.Col([
                    input_killed
                ], className='d-flex align-items-center \
                              justify-content-center col-2'),
                dbc.Col([
                    btn_save
                ], className='d-flex align-items-center \
                              justify-content-end col-1')
            ]),

            dbc.Alert(
                children='',
                color='warning',
                id='form-alert',
                is_open=False,
                duration=4000,
                className='m-2'
            )
        ]),
    ]),

    dbc.Card([
        dbc.CardBody([
            html.Div([dcc.Store(id='shootings_storage')]),
            html.H4("Shootings", className="card-title"),
            html.Div([
                create_table(shootings_df)
            ], id='table-container')
        ])
    ], className='card mt-3')
], className='m-3')

tabs = dbc.Tabs(
    [
        dbc.Tab(map_tab, label='Locations', tab_id='map-tab',
                activeTabClassName='dark'),
        dbc.Tab(day_tab, label='By Day', tab_id='day-tab'),
        dbc.Tab(month_tab, label='By Month', tab_id='month-tab'),
        dbc.Tab(state_tab, label='By State', tab_id='state-tab'),
        dbc.Tab(records_tab, label='List', tab_id='records-tab')
    ],
    id='tabs',
    active_tab='map-tab'
)

layout = dbc.Container([
        dbc.Card([
            dbc.CardBody([
                html.H1(f'Mass shootings in the US from \
                         {start_year} to {end_year}',
                         className='card-title mb-4'),
                tabs
            ])
        ], className='card mt-3'),
        html.Footer([
            dcc.Markdown('''
                Powered by [Dash / Plotly](https://dash.plotly.com/).
                ''')
        ], className='d-flex justify-content-center m-5')
    ], fluid=True)

app.layout = layout


# NEW SHOOTING FORM HANDLER
@app.callback(
    Output(component_id='form-alert', component_property='children'),
    Output(component_id='form-alert', component_property='is_open'),
    Output(component_id='table-container', component_property='children'),

    Input(component_id='form-save',     component_property='n_clicks'),
    [State(component_id='form-date',    component_property='date')],
    [State(component_id='form-address', component_property='value')],
    [State(component_id='form-injured', component_property='value')],
    [State(component_id='form-killed',  component_property='value')],
    prevent_initial_call=True
)
def record_shooting(n_clicks,
                    date_value,
                    address_value,
                    injured_value,
                    killed_value):
    """
    Validates user inpout, fetch geo coordinates and adds shooting to dataset.
    """
    fields = [date_value, address_value, injured_value, killed_value]

    for field in fields:
        if field is None :
            return (
                'All fields are required and\
                negative numbers are not allowed.',
                True,
                create_table(shootings_df)
            )

    # GET GEOCODE AND EXTRACT VALUES TO ADD TO DATASET
    try:
        location = get_location(address_value)
        location_arr = location[0].split(',')

        address = location_arr[0] + location_arr[1]
        city_or_county = location_arr[-4].strip()
        state = location_arr[-3].strip()
        country = location_arr[-1].strip()
        full_address = f'{address}, {city_or_county}, {state}, {country}'
    except Exception as e:
        return (
            'Address must be a valid Google Maps\
            address in the United States.',
            True,
            create_table(shootings_df)
        )

    if (country.lower() != 'united states' and
            country.lower() != 'usa' and
            country.lower() != 'u.s.a.' and
            country.lower() != 'united states of america'):
        return (
            'Address must be in the United States.',
            True,
            create_table(shootings_df)
        )
    try:
        injured_value = int(injured_value)
        killed_value = int(killed_value)
        total = injured_value + killed_value
    except ValueError:
        return (
            'Injured and Killed are required and\
            must be non-negative integers.',
            True,
            create_table(shootings_df)
        )

    # FORMATTING DATE
    try:
        date_value = pd.to_datetime(date_value).date()
        month_value = calendar.month_name[date_value.month]
        month_number_value = date_value.month
    except Exception:
        return (
            'Invalid date format.',
            True,
            create_table(shootings_df)
        )

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
    df.reset_index(drop=True)
    df = remove_unnamed_column(df)

    df.to_csv(file_path)

    return (
        '',
        False,
        create_table(df)
    )


# EDIT TABLE HANDLER
@app.callback(
    Output('shootings_storage', 'data'),
    Output(component_id='form-date',    component_property='date'),
    Output(component_id='form-address', component_property='value'),
    Output(component_id='form-injured', component_property='value'),
    Output(component_id='form-killed',  component_property='value'),

    Input('shootings-table', 'data'),
    prevent_initial_call=True
)
def update_dateframe(table):

    df = pd.DataFrame(table)
    df = df.reset_index(drop=True)
    df = df[::-1]

    df['date'] = pd.to_datetime(df['date']).dt.date
    df.to_csv(file_path)

    return (
        table,
        today,
        '',
        '',
        ''
    )


if __name__ == '__main__':
    app.run_server(debug=True)
