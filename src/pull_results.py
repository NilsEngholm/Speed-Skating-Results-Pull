import requests
import pandas as pd
import time

def load_csv_to_dataframe(file_path):
    """
    Load a CSV file into a pandas DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None


def fetch_skater_results(skater_id, distance):
    url = f"https://speedskatingresults.com/api/json/skater_results.php?skater={skater_id}&distance={distance}"
    time.sleep(0.5)
    print(f"Fetching data for skater {skater_id} (distance {distance})")
    try:
        response = requests.get(url)
        response.raise_for_status() 
        return response.json()  # parse json
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for skater {skater_id} (distance {distance}): {e}")
        return None

# iterate over df & write results
def process_skater_results_nested(df, distances):
    results_list = []
    for index, row in df.iterrows():
        skater_data = {
            "skater_id": row["id"],
            "family_name": row["family_name"],
            "given_name": row["given_name"],
            "sport": row["sport"],
            "distances": {}
        }
        for distance in distances:
            results = fetch_skater_results(row["id"], distance)
            if results:
                skater_data["distances"][distance] = results
        results_list.append(skater_data)
    return results_list

# main
df = load_csv_to_dataframe("data\\athletes_list_with_ids.csv")
distances_to_query = ['500m', '1000m', '1500m', '3000m', '5000m', '10000m']  # distances
nested_results = process_skater_results_nested(df, distances_to_query)

# save to json
import json
with open("athlete_results.json", "w") as f:
    json.dump(nested_results, f, indent=4)

print(json.dumps(nested_results, indent=4))