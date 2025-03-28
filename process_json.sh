#!/bin/bash
# This script processes JSON files in the ./aircraft directory and extracts specific fields.

echo "airframe_id,altitude,speed,track,lat,lon,timestamp" > aircraft_data.csv
for file in ./aircraft/*.json; do
  jq -r --argjson now "$(jq -r '.now' "$file")" '.aircraft[] | [.hex, .alt_baro, .gs, .track, .lat, .lon, $now] | @csv' "$file" >> aircraft_data.csv
done