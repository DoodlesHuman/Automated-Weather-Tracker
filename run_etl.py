"""
The combined ETL program for extracting weather forecast data
from Open Weather API, and transform to a pandas dataframe
in order to load to clean csv file.
"""

import sys
import os
from datetime import datetime, timezone
import requests 
import pandas as pd



# Extract
API_KEY = os.environ.get("OPENWEATHER_API_KEY")

if API_KEY:
    print(f"✅ API Key found! Length: {len(API_KEY)}")
else:
    print("❌ CRITICAL: API Key is None. Check GitHub Secrets.")

CITIES = [
    (52.5200, 13.4050, "Berlin")] # (lat, lon, city_name)
DATA_FILE_PATH = 'data/weather_forecast.csv'

def fetch_weather_data(api_key, cities):
    """
    Fetches 5-day/3-hour forecast data for a list of cities.
    Returns a list of all the raw data entries.
    """
    print("Fetching data from OpenWeatherMap...")
    all_forecast_data = []
    
    if not api_key:
        raise ValueError("API_KEY not found. Set OPENWEATHER_API_KEY secret.")
    base_url = "https://api.openweathermap.org/data/2.5/forecast"
    
    for lat, lon, city_name in cities:
        print(f"Fetching data for: {city_name}")
        params = {
            'lat': lat,
            'lon': lon,
            'appid': api_key,
            'units': 'metric'  
        }
    
        try:
            response = requests.get(base_url, params = params, timeout = 5)
            response.raise_for_status() 
            data = response.json()
            
            # Adding our city_name to each forecast entry
            for item in data['list']:
                item['city_name'] = city_name
                
            all_forecast_data.extend(data['list'])
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city_name}: {e}")
            
    print(f"Successfully fetched {len(all_forecast_data)} forecast entries.")
    return all_forecast_data

# Transform
def transform_data(raw_data_list):
    """
    Transforms the complex, nested JSON list into a flat, tidy DataFrame.
    """
    print("Transforming raw JSON data...")
    transformed = []
    
    for entry in raw_data_list:
        transformed.append({
            'city': entry['city_name'],
            'forecast_time': pd.to_datetime(entry['dt_txt']),
            'temp_celsius': entry['main']['temp'],
            'feels_like_celsius': entry['main']['feels_like'],
            'humidity_percent': entry['main']['humidity'],
            'weather_condition': entry['weather'][0]['main'],
            'weather_description': entry['weather'][0]['description'],
            'wind_speed_ms': entry['wind']['speed'],
            'fetch_timestamp': datetime.now(timezone.utc).isoformat()
        })
    
    if not transformed:
        print("No data to transform.")
        return pd.DataFrame()
        
    df = pd.DataFrame(transformed)
    print(f"Transformed {len(df)} rows into a DataFrame.")
    return df
"""
# Load
def load_data(new_data, file_path=DATA_FILE_PATH):
    """
    Loads the new forecast data, appending it to the existing CSV file.
    """
    if new_data.empty:
        print("No new data to load.")
        return

    print(f"Loading data to {file_path}...")
    try:
        existing_data = pd.read_csv(file_path)
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        
        # De-duplication, as the 5-day forecast will overlap each day
        # We keep the latest fetched forecast for any given city/time
        combined_data = combined_data.drop_duplicates(
            subset=['city', 'forecast_time'], 
            keep='last'
        )
        
    except FileNotFoundError:
        print("File not found. Creating new file.")
        combined_data = new_data
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Sort data for readability
    combined_data.sort_values(by=['city', 'forecast_time'], inplace=True)
    
    combined_data.to_csv(file_path, index=False)
    print(f"Successfully saved data to {file_path}.")
"""
# --- LOAD (Debug Version) ---
def load_data(new_data, file_path=DATA_FILE_PATH):
    """
    Verbose version to debug saving issues.
    """
    print("--- STARTING LOAD STEP ---")
    
    if new_data.empty:
        print("❌ New data is empty. Nothing to save.")
        return

    # 1. Print current working directory
    import os
    print(f"📂 Current Working Directory: {os.getcwd()}")
    
    # 2. Make sure the path is absolute so we know EXACTLY where it goes
    abs_path = os.path.abspath(file_path)
    print(f"🎯 Target File Path: {abs_path}")

    # 3. Prepare the data
    combined_data = new_data
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        print("   Found existing file. Appending...")
        try:
            existing_data = pd.read_csv(file_path)
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            combined_data = combined_data.drop_duplicates(
                subset=['city', 'forecast_time'], 
                keep='last'
            )
        except Exception as e:
            print(f"   ⚠️ Error reading existing file: {e}. Starting fresh.")
    else:
        print("   Creating new file (File not found or empty).")
    
    # 4. Ensure directory exists
    directory = os.path.dirname(abs_path)
    if not os.path.exists(directory):
        print(f"   Directory {directory} did not exist. Creating it...")
        os.makedirs(directory, exist_ok=True)
    
    # 5. SORT AND SAVE
    print(f"   Sorting {len(combined_data)} rows...")
    combined_data.sort_values(by=['city', 'forecast_time'], inplace=True)
    
    print("   💾 Attempting to write to CSV...")
    try:
        combined_data.to_csv(abs_path, index=False)
        print("   ✅ Python says: Write successful.")
    except PermissionError:
        print("   ❌ CRITICAL ERROR: Permission Denied!")
        print("   👉 Is the CSV file open in Excel? Close it and try again.")
        return
    except Exception as e:
        print(f"   ❌ CRITICAL ERROR: Could not write file: {e}")
        return

    # 6. Verify it actually happened
    if os.path.exists(abs_path):
        size = os.path.getsize(abs_path)
        print(f"   🔎 Verification: File exists. Size: {size} bytes.")
    else:
        print("   ❌ Verification Failed: File still does not exist.")

    print("--- LOAD STEP FINISHED ---")

if __name__ == "__main__":
    print("Starting Weather ETL process...")
    
    try:
        # 1. Extract
        raw_forecasts = fetch_weather_data(API_KEY, CITIES)
        
        # STOP IF EMPTY
        if not raw_forecasts:
            print("❌ CRITICAL: No data fetched. Check API Key or Network.")
            
        
        # 2. Transform
        forecast_df = transform_data(raw_forecasts)
        
        # 3. Load
        load_data(forecast_df)
        print("✅ ETL process completed successfully.")
        
    except Exception as e:
        print(f"❌ Pipeline failed with error: {e}")
        
        