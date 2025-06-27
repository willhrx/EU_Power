import requests
import pandas as pd
from datetime import datetime, timedelta
import time

class OpenMeteoHistoricalCollector:
    def __init__(self):
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        
    def get_historical_weather(self, lat, lon, start_date, end_date, variables=None):
        """
        Get historical weather data from Open-Meteo API
        """
        if variables is None:
            variables = [
                "temperature_2m",
                "relative_humidity_2m", 
                "precipitation",
                "wind_speed_10m",
                "wind_direction_10m",
                "surface_pressure",
                "cloud_cover"
            ]
        
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': start_date,
            'end_date': end_date,
            'hourly': ','.join(variables),
            'timezone': 'UTC'
        }
        
        try:
            print(f"Fetching data for coordinates ({lat}, {lon}) from {start_date} to {end_date}")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
    
    def process_weather_data(self, weather_response, city_name, country):
        """
        Process Open-Meteo API response into structured data
        """
        if not weather_response or 'hourly' not in weather_response:
            return None
        
        hourly_data = weather_response['hourly']
        times = hourly_data['time']
        
        processed_data = []
        
        for i, timestamp in enumerate(times):
            record = {
                'city': city_name,
                'country': country,
                'latitude': weather_response.get('latitude', ''),
                'longitude': weather_response.get('longitude', ''),
                'datetime': timestamp,
                'temperature_celsius': hourly_data.get('temperature_2m', [None] * len(times))[i],
                'humidity_percent': hourly_data.get('relative_humidity_2m', [None] * len(times))[i],
                'precipitation_mm': hourly_data.get('precipitation', [None] * len(times))[i],
                'wind_speed_kmh': hourly_data.get('wind_speed_10m', [None] * len(times))[i],
                'wind_direction_degrees': hourly_data.get('wind_direction_10m', [None] * len(times))[i],
                'pressure_hpa': hourly_data.get('surface_pressure', [None] * len(times))[i],
                'cloud_cover_percent': hourly_data.get('cloud_cover', [None] * len(times))[i]
            }
            processed_data.append(record)
        
        return processed_data
    
    def collect_europe_historical_data(self, start_date, end_date, output_filename='C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data and Collection/europe_historical_weather.csv'):
        """
        Collect historical weather data for major European cities
        """
        # Major European cities
        european_cities = [
            {'name': 'London', 'country': 'UK', 'lat': 51.5074, 'lon': -0.1278},
            {'name': 'Paris', 'country': 'France', 'lat': 48.8566, 'lon': 2.3522},
            {'name': 'Berlin', 'country': 'Germany', 'lat': 52.5200, 'lon': 13.4050},
            {'name': 'Madrid', 'country': 'Spain', 'lat': 40.4168, 'lon': -3.7038},
            {'name': 'Rome', 'country': 'Italy', 'lat': 41.9028, 'lon': 12.4964},
            {'name': 'Amsterdam', 'country': 'Netherlands', 'lat': 52.3676, 'lon': 4.9041},
            {'name': 'Vienna', 'country': 'Austria', 'lat': 48.2082, 'lon': 16.3738},
            {'name': 'Stockholm', 'country': 'Sweden', 'lat': 59.3293, 'lon': 18.0686},
            {'name': 'Warsaw', 'country': 'Poland', 'lat': 52.2297, 'lon': 21.0122},
            {'name': 'Prague', 'country': 'Czech Republic', 'lat': 50.0755, 'lon': 14.4378},
            {'name': 'Budapest', 'country': 'Hungary', 'lat': 47.4979, 'lon': 19.0402},
            {'name': 'Brussels', 'country': 'Belgium', 'lat': 50.8503, 'lon': 4.3517},
            {'name': 'Zurich', 'country': 'Switzerland', 'lat': 47.3769, 'lon': 8.5417},
            {'name': 'Oslo', 'country': 'Norway', 'lat': 59.9139, 'lon': 10.7522},
            {'name': 'Copenhagen', 'country': 'Denmark', 'lat': 55.6761, 'lon': 12.5683},
            {'name': 'Helsinki', 'country': 'Finland', 'lat': 60.1699, 'lon': 24.9384},
            {'name': 'Dublin', 'country': 'Ireland', 'lat': 53.3498, 'lon': -6.2603},
            {'name': 'Lisbon', 'country': 'Portugal', 'lat': 38.7223, 'lon': -9.1393},
            {'name': 'Athens', 'country': 'Greece', 'lat': 37.9838, 'lon': 23.7275},
            {'name': 'Bucharest', 'country': 'Romania', 'lat': 44.4268, 'lon': 26.1025}
        ]
        
        all_weather_data = []
        
        print(f"Starting historical data collection for {len(european_cities)} cities")
        print(f"Date range: {start_date} to {end_date}")
        
        for i, city in enumerate(european_cities):
            print(f"\nProcessing {city['name']} ({i+1}/{len(european_cities)})")
            
            # Get weather data for this city
            weather_data = self.get_historical_weather(
                city['lat'], 
                city['lon'], 
                start_date, 
                end_date
            )
            
            if weather_data:
                processed_data = self.process_weather_data(
                    weather_data, 
                    city['name'], 
                    city['country']
                )
                
                if processed_data:
                    all_weather_data.extend(processed_data)
                    print(f"Added {len(processed_data)} records for {city['name']}")
            
            # Small delay to be respectful to the API
            time.sleep(1)
        
        # Convert to DataFrame and save
        if all_weather_data:
            df = pd.DataFrame(all_weather_data)
            df.to_csv(output_filename, index=False)
            print(f"\n✅ Data collection complete!")
            print(f"📊 Saved {len(all_weather_data)} records to {output_filename}")
            print(f"📅 Date range: {df['datetime'].min()} to {df['datetime'].max()}")
            print(f"🏛️ Cities: {df['city'].nunique()}")
            return df
        else:
            print("❌ No data collected!")
            return None

# Extended historical data collection
def collect_long_term_data():
    """
    Example: Collect weather data going back decades
    """
    collector = OpenMeteoHistoricalCollector()
    
    # You can go back to 1940! 
    # Start with a smaller range for testing, then expand
    start_date = "2015-01-01"  # Can go back to "1940-01-01"
    end_date = "2025-06-08"
    
    print("Open-Meteo Historical Weather Data Collection")
    print("=" * 50)
    print(f"📅 Collecting data from {start_date} to {end_date}")
    print("🔄 This may take several minutes for large date ranges...")
    
    df = collector.collect_europe_historical_data(
        start_date=start_date,
        end_date=end_date,
        output_filename=f'europe_weather_{start_date}_{end_date}.csv'
    )
    
    if df is not None:
        print("\n📈 Dataset Summary:")
        print(f"   Total records: {len(df):,}")
        print(f"   Cities: {df['city'].nunique()}")
        print(f"   Countries: {df['country'].nunique()}")
        print(f"   Time span: {df['datetime'].min()} to {df['datetime'].max()}")
        
        # Show sample of the data
        print("\n🔍 Sample data:")
        print(df.head()[['city', 'datetime', 'temperature_celsius', 'precipitation_mm', 'wind_speed_kmh']])

def collect_single_city_longterm():
    """
    Example: Get very long-term data for a single city
    """
    collector = OpenMeteoHistoricalCollector()
    
    # Example: 50+ years of data for London
    weather_data = collector.get_historical_weather(
        lat=51.5074,
        lon=-0.1278,
        start_date="1970-01-01",  # You can go back to 1940!
        end_date="2024-12-31"
    )
    
    if weather_data:
        processed_data = collector.process_weather_data(
            weather_data, 
            "London", 
            "UK"
        )
        
        df = pd.DataFrame(processed_data)
        df.to_csv('london_weather_1970_2024.csv', index=False)
        print(f"Collected {len(df)} records for London from 1970-2024")
        return df

if __name__ == "__main__":
    # Choose your collection method:
    
    # Option 1: Multiple cities, moderate time range
    collect_long_term_data()
    
    # Option 2: Single city, very long time range (uncomment to use)
    # collect_single_city_longterm()
