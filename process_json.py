import os
import json
import csv

# Directory containing the JSON files
directory = './aircraft'
output_csv = 'aircraft_data.csv'

# Fields to collect
desired_fields = [
    'airframe_id', 'flight', 'altitude', 'speed', 
    'track', 'lat', 'lon', 'category', 'timestamp', 'rate'
]

def process_json_file(file_path):
    """
    Reads and parses a JSON file.
    Filters the data to include only the desired fields.
    Returns a list of dictionaries with the filtered data.
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            filtered_data = []

            if isinstance(data, list):
                for entry in data:
                    filtered_data.append({key: entry.get(key, None) for key in desired_fields})
            elif isinstance(data, dict):
                filtered_data.append({key: data.get(key, None) for key in desired_fields})

            return filtered_data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON in file {file_path}: {e}")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
    return []

def write_to_csv(data, output_file):
    """
    Writes a list of dictionaries to a CSV file.
    """
    if not data:
        print("No data to write to CSV.")
        return

    # Use the desired fields as headers
    headers = desired_fields

    try:
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        print(f"Data successfully written to {output_file}")
    except Exception as e:
        print(f"Error writing to CSV file {output_file}: {e}")

def process_all_json_files(directory, output_csv):
    """
    Processes all JSON files in the specified directory and writes their data to a CSV file.
    """
    all_data = []

    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist.")
        return

    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            data = process_json_file(file_path)
            all_data.extend(data)

    if all_data:
        write_to_csv(all_data, output_csv)
    else:
        print("No valid data found in JSON files.")

if __name__ == '__main__':
    process_all_json_files(directory, output_csv)