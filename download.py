from pybaseball import (
    batting_stats,
    pitching_stats,
    fielding_stats,
    team_batting,
    team_fielding,
    team_pitching,
)
import pandas as pd
from google.oauth2 import service_account
import datetime

# Set the current year
current_year = datetime.datetime.now().year

# Path to your service account key file
service_account_path = 'swingandmiss.json'
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# Google Cloud Project ID
project_id = 'swingandmiss'

# Google BigQuery Dataset ID
dataset_id = 'raw_data'

def download_chunk_data(start_year, end_year):
    common_params = {
        "league": "ALL",
        "split_seasons": True,
        "qual": 0,
        "max_results": 100000000,
    }
    return (
        batting_stats(start_season=start_year, end_season=end_year, **common_params),
        pitching_stats(start_season=start_year, end_season=end_year, **common_params),
        fielding_stats(start_season=start_year, end_season=end_year, **common_params),
        team_batting(start_season=start_year, end_season=end_year, **common_params),
        team_fielding(start_season=start_year, end_season=end_year, **common_params),
        team_pitching(start_season=start_year, end_season=end_year, **common_params)
    )

def replace_special_chars(df):
    df.columns = [col.replace(" ", "_").replace("-", "_").replace("%", "_").replace("(", "_").replace(")", "_").replace("/", "_").replace("+", "_").replace("#", "_") for col in df.columns]
    return df

def remove_duplicate_columns(df):
    df = df.loc[:,~df.columns.duplicated()]
    return df

def upload_to_bigquery(df, table_id):
    destination_table = f"{dataset_id}.{table_id}"
    print("DataFrame Schema Before Upload:")
    print(df.dtypes)
    try:
        df.to_gbq(destination_table, project_id=project_id, credentials=credentials, if_exists='replace')
        print(f"Uploaded {table_id} successfully.")
    except Exception as e:
        print(f"Failed to upload {table_id}: {e}")

def download_baseball_data(start_year, end_year):
    stats_functions = [batting_stats, pitching_stats, fielding_stats, team_batting, team_fielding, team_pitching]
    stat_types = ['batting_stats', 'pitching_stats', 'fielding_stats', 'team_batting_stats', 'team_fielding_stats', 'team_pitching_stats']

    for year in range(start_year, end_year + 1, 3):
        end_chunk_year = min(year + 2, end_year)
        print(f"Downloading data for {year} to {end_chunk_year}")
        chunks = download_chunk_data(year, end_chunk_year)

        for func, stat_type, chunk in zip(stats_functions, stat_types, chunks):
            chunk = replace_special_chars(chunk)
            chunk = remove_duplicate_columns(chunk)  # Remove any duplicate columns
            upload_to_bigquery(chunk, stat_type)

if __name__ == "__main__":
    download_baseball_data(current_year, current_year)
