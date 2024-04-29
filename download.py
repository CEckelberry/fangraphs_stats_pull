from pybaseball import (
    batting_stats,
    pitching_stats,
    fielding_stats,
    team_batting,
    team_fielding,
    team_pitching,
)
import pandas as pd
from google.cloud import bigquery
import datetime
import json
import os
import glob

current_year = datetime.datetime.now().year

# Setup Google Cloud client
# client = bigquery.Client()


def download_chunk_data(start_year, end_year):
    common_params = {
        "league": "ALL",
        "split_seasons": True,
        "qual": 0,
        "max_results": 100000000,
    }

    # Download data for the specified chunk
    batting_chunk = batting_stats(
        start_season=start_year, end_season=end_year, **common_params
    )
    pitching_chunk = pitching_stats(
        start_season=start_year, end_season=end_year, **common_params
    )
    fielding_chunk = fielding_stats(
        start_season=start_year, end_season=end_year, **common_params
    )
    team_batting_chunk = team_batting(
        start_season=start_year, end_season=end_year, **common_params
    )
    team_fielding_chunk = team_fielding(
        start_season=start_year, end_season=end_year, **common_params
    )
    team_pitching_chunk = team_pitching(
        start_season=start_year, end_season=end_year, **common_params
    )

    return (
        batting_chunk,
        pitching_chunk,
        fielding_chunk,
        team_batting_chunk,
        team_fielding_chunk,
        team_pitching_chunk,
    )


def replace_special_chars(df):
    df.columns = [col.replace(" ", "_").replace("-", "_").replace("%", "_").replace("(", "_").replace(")", "_").replace("/", "_").replace("+", "_").replace("#", "_")for col in df.columns]
    return df

def convert_data_types(df):
    for column in df.columns:
        initial_dtype = df[column].dtype  # Capture the initial data type for debugging
        if initial_dtype == 'object':
            try:
                # Handle NaN and None values specifically for object columns
                df[column] = df[column].fillna('NULL')
                df[column] = df[column].astype("string")
            except Exception as e:
                print(f"Error converting column '{column}' from {initial_dtype} to string: {e}")

        # Log after conversion attempt
        print(f"Column '{column}': converted from {initial_dtype} to {df[column].dtype}")

    return df

def download_baseball_data(start_year, end_year):
    # Initialize empty DataFrames to accumulate data
    (   batting_master,
        pitching_master,
        fielding_master,
        team_batting_master,
        team_fielding_master,
        team_pitching_master,
    ) = [pd.DataFrame() for _ in range(6)]

    # Loop through years in chunks
    for year in range(start_year, end_year + 1, 3):
        end_chunk_year = min(year + 2, end_year)  # Ensure we don't go beyond end_year
        print(f"Downloading data for {year} to {end_chunk_year}")

        (
            batting_chunk,
            pitching_chunk,
            fielding_chunk,
            team_batting_chunk,
            team_fielding_chunk,
            team_pitching_chunk,
        ) = download_chunk_data(year, end_chunk_year)

        # Append the chunk data to the master DataFrames
        batting_master = pd.concat([batting_master, batting_chunk])
        pitching_master = pd.concat([pitching_master, pitching_chunk])
        fielding_master = pd.concat([fielding_master, fielding_chunk])
        team_batting_master = pd.concat([team_batting_master, team_batting_chunk])
        team_fielding_master = pd.concat([team_fielding_master, team_fielding_chunk])
        team_pitching_master = pd.concat([team_pitching_master, team_pitching_chunk])

    # Save the master DataFrames to CSV
    batting_master.to_csv("batting_stats_2024_2024.csv")
    pitching_master.to_csv("pitching_stats_2024_2024.csv")
    fielding_master.to_csv("fielding_stats_2024_2024.csv")
    team_batting_master.to_csv("team_batting_2024_2024.csv")
    team_fielding_master.to_csv("team_fielding_2024_2024.csv")
    team_pitching_master.to_csv("team_pitching_2024_2024.csv")


    # Define patterns for the CSV files
    file_patterns = [
        "batting_stats_*_*.csv",
        "pitching_stats_*_*.csv",
        "fielding_stats_*_*.csv",
        "team_batting_*_*.csv",
        "team_fielding_*_*.csv",
        "team_pitching_*_*.csv"
    ]

    # Gather all matching CSV files based on defined patterns
    csv_files = []
    for pattern in file_patterns:
        csv_files.extend(glob.glob(pattern))

    # Process each CSV file and replace special chars
    for csv_file in csv_files:
        # Load the CSV into a DataFrame
        df = pd.read_csv(csv_file)
        
        # Modify the DataFrame column headers
        df = replace_special_chars(df)
        
        # Save the DataFrame back to the same CSV, replacing the original header
        df.to_csv(csv_file, index=False)

        print(f"Processed {csv_file} with updated headers.")
    
    # Process each CSV file and convert datatypes
    for csv_file in csv_files:
        # Load the CSV into a DataFrame
        df = pd.read_csv(csv_file)
        
        print("before datatypes")
        print("df.head for ${csv_file} before..")
        print(df.head())
        print("df.dtypes for ${csv_file} before..")
        print(df.dtypes)

        # Modify the DataFrame column headers
        df = convert_data_types(df)
        
        print("df.head for ${csv_file} after..")
        print(df.head())
        print("df.dtypes for ${csv_file} after..")
        print(df.dtypes)

        # Save the DataFrame back to the same CSV, replacing the original header
        df.to_csv(csv_file, index=False)

        print(f"Processed {csv_file} with updated datatypes.")



# Specify the full year range
download_baseball_data(current_year, current_year)



