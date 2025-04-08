import os
import json
import sqlite3

# Directory containing the JSON files
directory = './aircraft'

# Fields to extract
fields = [
    "now", "hex", "flight", "alt_baro", "baro_rate", "gs",
    "track", "category", "nav_heading", "lat", "lon"
]

# Connect to SQLite
conn = sqlite3.connect("air_traffic_filtered.db")
c = conn.cursor()

# Create table with selected fields
columns_sql = ', '.join([f'"{f}" TEXT' for f in fields])
c.execute('DROP TABLE IF EXISTS aircraft')
c.execute(f'CREATE TABLE aircraft ({columns_sql})')

# Prepare insert statement
insert_sql = f'INSERT INTO aircraft ({", ".join(fields)}) VALUES ({", ".join(["?"] * len(fields))})'

# Process all JSON files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.json'):
        file_path = os.path.join(directory, filename)
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                # Insert data with top-level 'now' value included in every row
                for ac in data.get('aircraft', []):
                    row = [str(data.get('now', ''))]  # Start with the 'now' timestamp
                    row.extend([
                        str(ac.get(f, '') if ac.get(f) is not None else '') for f in fields[1:]
                    ])
                    c.execute(insert_sql, row)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in file {file_path}: {e}")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

# Commit changes and close the connection
conn.commit()
conn.close()

print("All aircraft data saved to air_traffic_filtered.db")
