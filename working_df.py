import pandas as pd

price_data_path = r'C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data Cleaning and Prep/european_prices_daily_2015_2025.csv'
weather_data_path = r'C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data Cleaning and Prep/european_weather_daily_2015_2025.csv'
# Load the price data
df_prices = pd.read_csv(price_data_path)   
# Load the weather data
df_weather = pd.read_csv(weather_data_path)
# Ensure the datetime column is in datetime format
df_prices['Date'] = pd.to_datetime(df_prices['Date'])   
df_weather['date'] = pd.to_datetime(df_weather['date'])
# Merge the two dataframes on the date column   
df_merged = pd.merge(df_prices, df_weather, left_on='Date', right_on='date', how='outer')
# Drop the date column from the weather data as it's already included in the price data 
df_merged = df_merged.drop(columns=['date'])
#Drop any rows with NaN values in the merged dataframe
df_merged = df_merged.dropna()
#Create day ahead prices for each country by shifting the price data
for country in df_prices.columns[1:]:  # Skip the 'Date' column
    df_merged[f'{country}_day_ahead'] = df_merged[country].shift(-1)
#Drop the last row of the dataframe as it will have NaN values for the day ahead prices
df_merged = df_merged[:-1]
# Save the merged dataframe to a new CSV file
df_merged.to_csv(r'C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data Cleaning and Prep/european_data_merged_2015_2025.csv', index=False)
print(f"Shape of price data: {df_merged.shape}")    
print(f"Nan values in merged data: {df_merged.isna().sum().sum()}   ")