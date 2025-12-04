import json
import glob

# Folder where your JSON files are stored
input_folder = "./sitps_by_county"
output_file = "stations_coordinates.json"

result = {}

# Loop through all JSON files in the folder
for file_path in glob.glob(f"{input_folder}/*.json"):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

        # Navigate to mainEntity → itemListElement
        try:
            items = data["mainEntity"]["itemListElement"]
        except KeyError:
            continue  # Skip files that don't match expected structure

        for list_item in items:
            item = list_item.get("item", {})
            station_code = None
            latitude = None
            longitude = None

            # Extract stationCode from additionalProperty
            for prop in item.get("additionalProperty", []):
                if prop.get("name") == "stationCode":
                    station_code = prop.get("value")
                    break

            # Extract coordinates
            geo = item.get("geo", {})
            latitude = geo.get("latitude")
            longitude = geo.get("longitude")

            if station_code and latitude is not None and longitude is not None:
                result[station_code] = {
                    "latitude": latitude,
                    "longitude": longitude
                }

# Save to JSON
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=4, ensure_ascii=False)

print(f"Created {output_file} with {len(result)} stations.")
