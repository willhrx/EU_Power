import pandas as pd

price_data_path = 'C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data and Collection/european_wholesale_electricity_price_data_daily.csv'

df_prices = pd.read_csv(price_data_path)

# Make a copy of the dataframe for editing  
df_prices_edit = df_prices.copy()

# Ensure the datetime column is in datetime format
df_prices_edit['Date'] = pd.to_datetime(df_prices_edit['Date'])

# Drop the area code column (assuming it's called something like 'area_code' or 'code')
# Replace 'area_code' with the actual column name if different
df_prices_edit = df_prices_edit.drop(['ISO3 Code'], axis=1)

# Pivot the data so each country becomes a column
df_prices_pivoted = df_prices_edit.pivot(index='Date', columns='Country', values='Price (EUR/MWhe)')

# Reset index to make datetime a regular column
df_prices_pivoted = df_prices_pivoted.reset_index()

# Flatten column names (remove the 'country' level name)
df_prices_pivoted.columns.name = None

# Drop any rows with NaN values
df_prices_pivoted = df_prices_pivoted.dropna()

# Drop any duplicate rows
df_prices_pivoted = df_prices_pivoted.drop_duplicates()

# Drop luxembourg as it is not needed
df_prices_pivoted = df_prices_pivoted.drop(columns=['Luxembourg'])

# Ensure the 'Date' column is the first column
df_prices_pivoted = df_prices_pivoted[['Date'] + [col for col in df_prices_pivoted.columns if col != 'Date']]   

# Save the pivoted dataframe
df_prices_pivoted.to_csv('C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data Cleaning and Prep/european_prices_daily_2015_2025.csv', index=False)

print(f"Shape of original data: {df_prices.shape}")
print(f"Shape of pivoted data: {df_prices_pivoted.shape}")
print(f"Columns in final dataset: {list(df_prices_pivoted.columns)}")
# Print number of nan values in dataset
print(f"Number of NaN values in final dataset: {df_prices_pivoted.isna().sum().sum()}")