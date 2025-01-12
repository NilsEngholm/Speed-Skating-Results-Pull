import requests
import pandas as pd

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

def get_ids(df):
    """
    Add a new column "id" to the DataFrame with IDs retrieved from the API.

    Args:
        df (pd.DataFrame): The DataFrame containing "given_name" and "family_name".

    Returns:
        pd.DataFrame: The updated DataFrame with the "id" column.
    """
    base_url = "https://speedskatingresults.com/api/json/skater_lookup"

    # ensure index matches the row number
    df = df.reset_index(drop=True)

    # init id column
    df['id'] = None

    for index, row in df.iterrows():
        print(f"Getting ID for athlete {row['given_name']} {row['family_name']}")
        api_url = f"{base_url}?familyname={row['family_name']}&givenname={row['given_name']}"
        try:
            response = requests.get(api_url)
            if response.status_code == 200:
                data = response.json()
                id_number = data['skaters'][0]['id'] if data['skaters'] else None
                print(f"Retrieved ID: {id_number}")
                df.at[index, 'id'] = id_number
            else:
                print(f"API Error for {row['given_name']} {row['family_name']}: {response.status_code}")
        except Exception as e:
            print(f"Error fetching data for {row['given_name']} {row['family_name']}: {e}")
    return df

def save_dataframe_to_csv(df, output_path):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        output_path (str): The path for the output CSV file.
    """
    try:
        df.to_csv(output_path, index=False)
        print(f"DataFrame successfully saved to {output_path}")
    except Exception as e:
        print(f"Error saving CSV: {e}")

if __name__ == "__main__":
    # path to csv
    input_file_path = "data\\athletes_list.csv"

    # output path
    output_file_path = "data\\athletes_list_with_ids.csv"

    # load csv to df > use 'csv_template.csv' as an example of how the file should be formatted
    df = load_csv_to_dataframe(input_file_path)

    if df is not None:
        print("Original DataFrame:")
        print(df)

        # get ids for skaters
        updated_df = get_ids(df)

        print("Updated DataFrame with IDs:")
        print(updated_df)

        # make a new csv
        save_dataframe_to_csv(updated_df, output_file_path)
