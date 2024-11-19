import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Load the data
current_dir=Path.cwd()
print(current_dir)
path = current_dir / 'market_prices'
print(path)

# Function to read a single CSV file
def read_csv(file):
    return pd.read_csv(file, usecols=[0,3,4,5])

# Read multiple CSV files from the directory using ThreadPoolExecutor
all_files = list(path.glob("*.csv"))
with ThreadPoolExecutor() as executor:
    data_frames = list(executor.map(read_csv, all_files))

combined_data = pd.concat(data_frames, ignore_index=True)

# Change column names
new_column_names = ['City', 'Date', 'CropName', 'Price']
combined_data.columns = new_column_names

# Remove certain string from a specific column
string_to_remove = "   View Graph"
combined_data['City'] = combined_data['City'].str.replace(string_to_remove, '', regex=False)
combined_data["Price"].fillna('N/A', inplace=True)
combined_data.sort_values(by=['City', 'Date'], inplace=True)
print(combined_data, combined_data.dtypes)


