import requests
import pandas as pd
from datetime import datetime
import s3fs
from pandas import json_normalize
import json

import requests
import pandas as pd

def get_weather_data(api_key, cities):
    def flatten_json(data, prefix=''):
        flattened = {}
        for key, value in data.items():
            if isinstance(value, dict):
                flattened.update(flatten_json(value, prefix + key + '_'))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    flattened.update(flatten_json(item, prefix + key + f'_{i}_'))
            else:
                flattened[prefix + key] = value
        return flattened

    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    weather_data = []

    for city in cities:
        complete_url = f"{base_url}q={city}&appid={api_key}&units=metric"
        response = requests.get(complete_url)
        data = response.json()

        if data["cod"] == "404":
            print(f"City {city} not found. Skipping.")
        else:
            weather_description = data["weather"][0]["description"]
            temperature = data["main"]["temp"]
            humidity = data["main"]["humidity"]
            wind_speed = data["wind"]["speed"]

            print(f"Weather in {city}:")
            print(f"Description: {weather_description}")
            print(f"Temperature: {temperature}°C")
            print(f"Humidity: {humidity}%")
            print(f"Wind Speed: {wind_speed} m/s")

            flattened_data = flatten_json(data)
            weather_data.append(flattened_data)

    if weather_data:
        df = pd.DataFrame(weather_data)
        return df
    else:
        return None

if __name__ == "__main__":
    # Replace 'YOUR_API_KEY' with your actual API key from OpenWeather
    api_key = "415eadbf15ec4bd22e8290c01deb6222"
    cities = ["Mumbai", "London", "New York", "Tokyo", "Sydney", "Paris", "Berlin", "Moscow", "Singapore", "Dubai"]
    
    weather_df = get_weather_data(api_key, cities)

    if weather_df is not None:
        weather_df.to_csv(r"s3://open-weather-aflow-de/Weather_data.csv", mode='a', index=False,header=False)
