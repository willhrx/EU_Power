import matplotlib.pyplot as plt
import pandas as pd
#Import the price data
price_data_path = 'C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data Cleaning and Prep/european_prices_daily_2015_2025.csv'
df_prices = pd.read_csv(price_data_path)
# Ensure the 'Date' column is in datetime format        
df_prices['Date'] = pd.to_datetime(df_prices['Date'])
# Set the 'Date' column as the index
df_prices.set_index('Date', inplace=True)
#Import the prediction data
prediction_data_path = 'C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Modelling and Forcasting/xgboost_predictions.csv'
df_predictions = pd.read_csv(prediction_data_path)
# Ensure the 'Date' column is in datetime format
df_predictions['Date'] = pd.to_datetime(df_predictions['Date'])
# Set the 'Date' column as the index
df_predictions.set_index('Date', inplace=True)
# Plotting function for daily prices
def plot_daily_prices(df, country):
    plt.figure(figsize=(14, 7))
    plt.plot(df.index, df[country], label=f'{country} Daily Prices', color='blue')
    plt.plot(df_predictions.index, df_predictions[f'{country}_day_ahead'].shift(1), label=f'{country} Predictions', color='orange', linestyle='--')
    plt.title(f'Daily Electricity Prices in {country} (2015-2025)')
    plt.xlabel('Date')
    plt.ylabel('Price (EUR/MWhe)')
    plt.grid()
    plt.legend()
    plt.show()
#Plot the daily prices for Germany
plot_daily_prices(df_prices, 'France')