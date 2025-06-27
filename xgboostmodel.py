import pandas as pd
from xgboost import XGBRegressor
from sklearn.multioutput import MultiOutputRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error
import joblib
import numpy as np

# Load data
df = pd.read_csv(r'C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Data Cleaning and Prep/european_data_merged_2015_2025.csv')

# Ensure the 'Date' column is in datetime format   
df['Date'] = pd.to_datetime(df['Date'])
# Set the 'Date' column as the index
df.set_index('Date', inplace=True)

# Select features and target variables
prices = df.filter(like='_day_ahead')
features = df.drop(columns=prices.columns)

print(f"Original data shape: {df.shape}")
print(f"Features shape: {features.shape}")
print(f"Targets shape: {prices.shape}")
print(f"NaN in features: {features.isnull().sum().sum()}")
print(f"NaN in targets: {prices.isnull().sum().sum()}")

# Handle missing values
# Remove rows where ANY target has NaN
valid_rows = ~prices.isnull().any(axis=1)
features_clean = features[valid_rows]
prices_clean = prices[valid_rows]

print(f"After removing NaN rows:")
print(f"Clean features shape: {features_clean.shape}")
print(f"Clean targets shape: {prices_clean.shape}")

# Handle any remaining NaN in features
features_clean = features_clean.fillna(features_clean.mean())

# Time-based split (instead of random split for time series)
# Use last 20% of data as test set
split_index = int(len(features_clean) * 0.8)
X_train = features_clean.iloc[:split_index]
X_test = features_clean.iloc[split_index:]
y_train = prices_clean.iloc[:split_index]
y_test = prices_clean.iloc[split_index:]

print(f"Training set: {X_train.shape}, {y_train.shape}")
print(f"Test set: {X_test.shape}, {y_test.shape}")

# Initialize the XGBoost regressor
xgb_params = {
    'n_estimators': 500,
    'learning_rate': 0.1,
    'max_depth': 6,
    'subsample': 0.9,
    'colsample_bytree': 0.8,
    'reg_alpha': 0.1,
    'reg_lambda': 1.0,
    'random_state': 42,
    'n_jobs': -1
}

# Use MultiOutputRegressor for multiple target prediction
model = MultiOutputRegressor(
    XGBRegressor(**xgb_params),
    n_jobs=-1
)

print("Training model...")
# Fit the model
model.fit(X_train, y_train)

print("Making predictions...")
# Make predictions
predictions = model.predict(X_test)

# Convert predictions to a DataFrame
predictions_df = pd.DataFrame(predictions, columns=prices_clean.columns, index=X_test.index)

# Save the predictions to a CSV file
predictions_df.to_csv(r'C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Modelling and Forcasting/xgboost_predictions.csv', index=True)
print(f"Shape of predictions: {predictions_df.shape}")

# Print mean absolute error of predictions
mae = mean_absolute_error(y_test, predictions)
print(f"Mean Absolute Error: {mae:.2f}")

# Calculate MAE per city/target
for i, col in enumerate(prices_clean.columns):
    city_mae = mean_absolute_error(y_test.iloc[:, i], predictions[:, i])
    print(f"MAE for {col}: {city_mae:.2f}")

# Save the model using joblib (better for sklearn wrappers)
joblib.dump(model, r'C:/Users/willi/OneDrive/Documents/Coding/Quant Trading/EU Power Markets/Modelling and Forcasting/xgboost_model.pkl')
print("Model saved successfully!")