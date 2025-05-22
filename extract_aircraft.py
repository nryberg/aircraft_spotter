import json
import os
import sys
import pandas as pd
import re
from glob import glob
from datetime import datetime

def extract_aircraft_data_from_file(json_file_path):
    """
    Extract aircraft data from a single JSON file.
    
    Args:
        json_file_path (str): Path to JSON file containing aircraft data
    
    Returns:
        pandas.DataFrame: DataFrame containing the extracted aircraft data
    """
    try:
        # Read the JSON data
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
        
        # Get the timestamp from the data
        timestamp_utc = data.get('now', None)
        utc_datetime = None
        if timestamp_utc:
            utc_datetime = datetime.utcfromtimestamp(timestamp_utc)
        
        # Get the aircraft data
        aircraft_list = data.get('aircraft', [])
        
        # Extract only the requested fields
        # Use get() method to handle missing fields
        rows = []
        for aircraft in aircraft_list:
            row = {
                'hex': aircraft.get('hex', ''),
                'alt_baro': aircraft.get('alt_baro', None),
                'track': aircraft.get('track', None),
                'gs': aircraft.get('gs', None),
                'timestamp_utc': utc_datetime
            }
            rows.append(row)
        
        # Convert to pandas DataFrame
        df = pd.DataFrame(rows)
        
        # Convert numeric columns properly
        numeric_cols = ['alt_baro', 'track', 'gs']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # Convert timestamp column to datetime type
        if 'timestamp_utc' in df.columns:
            df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        
        return df, len(aircraft_list)
        
    except Exception as e:
        print(f"Error processing {json_file_path}: {e}")
        return pd.DataFrame(), 0

def extract_aircraft_data(json_file_path, parquet_file_path):
    """
    Extract aircraft data from JSON file(s) and save it to Parquet.
    
    Args:
        json_file_path (str): Path to JSON file or directory containing aircraft data
        parquet_file_path (str): Path to save the Parquet file
    """
    try:
        # Check if the path is a directory
        if os.path.isdir(json_file_path):
            # Process all JSON files in the directory
            json_files = glob(os.path.join(json_file_path, "*.json"))
            
            if not json_files:
                print(f"No JSON files found in {json_file_path}")
                sys.exit(1)
            
            all_dfs = []
            total_records = 0
            
            for json_file in json_files:
                df, num_records = extract_aircraft_data_from_file(json_file)
                if not df.empty:
                    all_dfs.append(df)
                    total_records += num_records
                    print(f"Processed {json_file} ({num_records} records)")
            
            if not all_dfs:
                print("No valid data extracted from any of the files")
                sys.exit(1)
            
            # Combine all DataFrames
            combined_df = pd.concat(all_dfs, ignore_index=True)
            
            # Save as Parquet
            combined_df.to_parquet(parquet_file_path, index=False)
            
            print(f"Successfully extracted data from {len(json_files)} files to {parquet_file_path}")
            print(f"Processed {total_records} total aircraft records")
            
        else:
            # Process a single JSON file
            df, num_records = extract_aircraft_data_from_file(json_file_path)
            
            if df.empty:
                print(f"No valid data extracted from {json_file_path}")
                sys.exit(1)
            
            # Save as Parquet
            df.to_parquet(parquet_file_path, index=False)
            
            print(f"Successfully extracted data from {json_file_path} to {parquet_file_path}")
            print(f"Processed {num_records} aircraft records")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Process command line arguments if provided
    if len(sys.argv) > 1:
        # User provided specific paths
        json_file_path = sys.argv[1]
        
        if len(sys.argv) > 2:
            parquet_file_path = sys.argv[2]
        else:
            # Extract folder name from the JSON file path
            folder_match = re.search(r'chunks/([^/]+)/', json_file_path)
            folder_name = folder_match.group(1) if folder_match else "output"
            parquet_file_path = f"{folder_name}.parquet"
        
        # Only create directory if the parquet path contains a directory
        if os.path.dirname(parquet_file_path):
            os.makedirs(os.path.dirname(parquet_file_path), exist_ok=True)
        
        # Extract the data
        extract_aircraft_data(json_file_path, parquet_file_path)
    else:
        # Process both "one" and "two" directories
        directories = ["chunks/one", "chunks/two"]
        
        for directory in directories:
            # Get the folder name (one or two)
            folder_name = os.path.basename(directory)
            parquet_file_path = f"{folder_name}.parquet"
            
            # Extract the data
            print(f"\nProcessing directory: {directory}")
            extract_aircraft_data(directory, parquet_file_path)