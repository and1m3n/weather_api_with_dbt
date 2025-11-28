import requests
import os
from dotenv import load_dotenv

load_dotenv('/opt/airflow/dags/.env') 
api_key = os.getenv("api_key")
api_url=f"http://api.weatherstack.com/current?access_key={api_key}&query=Moscow"

def fetch_data():
    print('featching weather data')
    try:
        response = requests.get(api_url,timeout=10)
        response.raise_for_status()
        print('API response succsess')
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'an error {e}')
        raise
    
def mock_feth_data():
    return {'request': {'type': 'City', 'query': 'Moscow, Russia', 'language': 'en', 'unit': 'm'}, 'location': {'name': 'Moscow', 'country': 'Russia', 'region': 'Moscow City', 'lat': '55.752', 'lon': '37.616', 'timezone_id': 'Europe/Moscow', 'localtime': '2025-11-27 15:28', 'localtime_epoch': 1764257280, 'utc_offset': '3.0'}, 'current': {'observation_time': '12:28 PM', 'temperature': 4, 'weather_code': 266, 'weather_icons': ['https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0017_cloudy_with_light_rain.png'], 'weather_descriptions': ['Light Drizzle, Mist'], 'astro': {'sunrise': '08:28 AM', 'sunset': '04:06 PM', 'moonrise': '01:27 PM', 'moonset': '10:55 PM', 'moon_phase': 'Waxing Crescent', 'moon_illumination': 37}, 'air_quality': {'co': '705.85', 'no2': '61.85', 'o3': '1', 'so2': '61.65', 'pm2_5': '61.25', 'pm10': '72.85', 'us-epa-index': '3', 'gb-defra-index': '3'}, 'wind_speed': 12, 'wind_degree': 183, 'wind_dir': 'S', 'pressure': 1016, 'precip': 0, 'humidity': 100, 'cloudcover': 100, 'feelslike': 1, 'uv_index': 0, 'visibility': 3, 'is_day': 'yes'}}