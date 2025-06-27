import pandas as pd

# Replace with the actual path to your weather data file
weather_data_path = 'C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data and Collection/europe_weather_2015-01-01_2025-06-08.csv'

# Import the European weather data
df_weather = pd.read_csv(weather_data_path)

# Make a copy of the dataframe for editing
df_weather_edit = df_weather.copy()

# Ensure the datetime column is in datetime format
df_weather_edit['datetime'] = pd.to_datetime(df_weather_edit['datetime'])

# Extract date from datetime for daily grouping
df_weather_edit['date'] = df_weather_edit['datetime'].dt.date

# Drop latitude and longitude columns
df_weather_edit = df_weather_edit.drop(['latitude', 'longitude'], axis=1)

# Get all numeric columns (excluding datetime, date, and city)
numeric_columns = df_weather_edit.select_dtypes(include=['number']).columns.tolist()

# Group by date and city, then take the mean of all numeric weather columns
df_weather_daily = df_weather_edit.groupby(['date', 'city'])[numeric_columns].mean().reset_index()

# Pivot to get cities as columns for each weather metric
df_weather_pivoted = df_weather_daily.pivot(index='date', columns='city', values=numeric_columns)

# Flatten the multi-level column names (weather_metric_city_name)
df_weather_pivoted.columns = ['_'.join([str(col[0]), str(col[1])]) for col in df_weather_pivoted.columns]

# Reset index to make date a regular column
df_weather_pivoted = df_weather_pivoted.reset_index()

# Save the daily dataframe to a new CSV file
df_weather_pivoted.to_csv('C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data Cleaning and Prep/european_weather_daily_2015_2025.csv', index=False)

print(f"Shape of original data: {df_weather.shape}")
print(f"Shape of daily aggregated data: {df_weather_pivoted.shape}")
print(f"Columns in final dataset: {list(df_weather_pivoted.columns)}")
print(f"Number of NaN values in final dataset: {df_weather_pivoted.isna().sum().sum()}")