import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = "https://api.open-meteo.com/v1/forecast?latitude=38.71&longitude=-9.13&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
    
    response = requests.get(url)
    data = response.json()
    current = data['current']

    # O Mage adora Pandas DataFrames
    df = pd.DataFrame([{
        'city': 'Lisboa',
        'temperature': current['temperature_2m'],
        'humidity': current['relative_humidity_2m'],
        'wind_speed': current['wind_speed_10m'],
        'extracted_at': current['time']
    }])
    
    return df