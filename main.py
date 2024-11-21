import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pyodbc
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load the data
current_dir = Path.cwd()
path = current_dir / 'market_prices'

# Function to read a single CSV file
def read_csv(file):
    return pd.read_csv(file, usecols=[0, 3, 4, 5])

# Read multiple CSV files from the directory using os.listdir
all_files = [path / file for file in os.listdir(path) if file.endswith('.csv')]
print(f"Total number of CSV files found: {len(all_files)}")
print(all_files)

with ThreadPoolExecutor(max_workers=8) as executor:
    data_frames = list(executor.map(read_csv, all_files))

combined_data = pd.concat(data_frames, ignore_index=True)
combined_data.dropna(inplace=True)
# Change column names
new_column_names = ['City', 'Date', 'CropName', 'Price']
combined_data.columns = new_column_names

# Remove certain string from a specific column
string_to_remove = "   View Graph"
combined_data['City'] = combined_data['City'].str.replace(string_to_remove, '', regex=False)

# Sort the data by 'City'
combined_data.groupby(by='City').apply(lambda x: x.sort_values(by='Date'))

# Remove commas from 'Price' column
combined_data['Price'] = combined_data['Price'].str.replace(',', '').astype(float)

# Function to save data to MSSQL server
def save_to_mssql(df, table_name, server, database, username, password):
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    logging.info(f"Connection string: {conn_str}")
    try:
        conn = pyodbc.connect(conn_str)
        logging.info("Server connected", conn)
        cursor = conn.cursor()
        cursor
        for index, row in df.iterrows():
            logging.info(f"Inserting row {index}: {row.to_dict()}")
            try:
                cursor.execute(
                    f"INSERT INTO {table_name} (city, date, crop_name, price) VALUES (?, ?, ?, ?)",
                    row.City, row.Date, row.CropName, row.Price
                )
                logging.info(f"Successfully inserted row {index}")
            except Exception as e:
                logging.error(f"Error inserting row {index}: {e}")
        
        conn.commit()
        logging.info("Data committed")
        cursor.close()
        logging.info("Cursor closed")
        conn.close()
        logging.info("Connection closed")
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")

print(combined_data, combined_data.dtypes)

# Example usage
save_to_mssql(
    combined_data,  
    table_name='crop_prices', 
    server='<server_name>', 
    database='<database_name>', 
    username='<username>', 
    password='<password>'
)


