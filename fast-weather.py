import openmeteo_requests

import pandas as pd
import numpy as np
import tabulate as tb
import json
import requests_cache
from retry_requests import retry

class WeatherCol():
    def __init__(self, header, format_function=None, color="blue"):
        self.header = header
        self.format_function = format_function
        self.color = color
        self.color_header()
    
    def color_header(self):
        if (self.color is None):
            return

        reset = "\033[0m"        
        if (self.color == 'blue'):
            self.header = f"\033[36m{self.header}{reset}"

    def format(self, data):
        if (self.format_function is not None):
            return self.format_function(data)
        else:
            return data

class WMOWeatherCodes():
    '''
    Class for storing WMO Weather Code -> Description translation
    (Credit to https://gist.github.com/stellasphere/9490c195ed2b53c707087c8c2db4ec0c)
    '''
    def __init__(self, fp="wmo.json"):
        raw = ""
        with open(fp, 'r') as file:
            raw = file.read()
        self.code_lut = json.loads(raw)
    
    def get_desc(self, code):
        '''Easy accessor for description from a wmo_code'''
        return self.code_lut[str(int(code))]['night']['description']

class FastWeatherConfig():
    def __init__(self, fp=".fastweathercfg.json"):
        raw = ""
        with open(fp, 'r') as file:
            raw = file.read()
        self.cfg = json.loads(raw)
        self.latitude = self.cfg['latitude']
        self.longitude = self.cfg['longitude']
        self.timezone = self.cfg['timezone']

class FastWeather():
    def get(self):
        '''
        Requests and formats weather data
        '''

        wmo_codes = WMOWeatherCodes()
        cfg = FastWeatherConfig()

        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)

        # Side note about "2m": 2 meters from ground is where weather is generally
        # measured from

        # |------------------------------|
        # | Declare cols here (ordered)  |
        # |------------------------------|
        wmocode_to_desc = np.vectorize(lambda code: wmo_codes.get_desc(code))
        round_2dec = np.vectorize(lambda x: f"{round(x, 2):.2f}")
        hourly_cols = {
            # Map {api_key : column_header}
            "temperature_2m" : WeatherCol("Temp (F)", format_function=round_2dec),
            "apparent_temperature" : WeatherCol("Feels Like (F)", format_function=round_2dec),
            "weather_code" : WeatherCol("Weather", format_function=wmocode_to_desc), 
            "precipitation_probability" : WeatherCol("Precip (%)"), 
            "precipitation" : WeatherCol("Precip (in)"), 
            "relative_humidity_2m" : WeatherCol("Humidity (%)"),
            "dew_point_2m" : WeatherCol("Dew Point", format_function=round_2dec), 
            "uv_index" : WeatherCol("UV Index"), 
        }

        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": cfg.latitude,
            "longitude": cfg.longitude,
            "forecast_hours": 12,
	        "past_hours": 0,
            "hourly": hourly_cols.keys(),
            "timezone": cfg.timezone,
            "wind_speed_unit": "mph",
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch"
        }
        responses = openmeteo.weather_api(url, params=params)

        # Process hourly data
        hourly = responses[0].Hourly()
        hourly_data = {"\033[36mTime\033[0m": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit="s").tz_localize("UTC").tz_convert(cfg.timezone),
            end = pd.to_datetime(hourly.TimeEnd(), unit="s").tz_localize("UTC").tz_convert(cfg.timezone),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left")
            # Format to be more readable
            .strftime("[%a %b %d] %I:%M %p")
        }

        for idx, key in enumerate(hourly_cols):
            col = hourly_cols[key]
            data = col.format(hourly.Variables(idx).ValuesAsNumpy())
            hourly_data[col.header] = data

        df = pd.DataFrame(data = hourly_data)
        return df


weatherdf = FastWeather().get()
table = tb.tabulate(weatherdf, headers="keys", tablefmt="github", showindex=False)
print(table)