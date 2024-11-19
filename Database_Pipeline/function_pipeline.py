import pyodbc
import json
import pandas as pd 
import warnings
import sqlalchemy
from sqlalchemy import create_engine
import urllib.parse


def connect_database_sqlalchemy(database, server='localhost', username='sa', password='rainscales@2024', port=1433):
    encoded_password = urllib.parse.quote_plus(password)
    # Correct connection string
    connection_string = f"mssql+pyodbc://{username}:{encoded_password}@{server}:{port}/{database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

    # Create an SQLAlchemy engine
    engine = create_engine(connection_string)
    print(f"Connected to database: {database}")
    return engine



def connect_database_pyodbc(database, server='localhost,1433', driver='{ODBC Driver 18 for SQL Server}', username='sa', password='rainscales@2024'):
    # establish connecttion
    conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes')
    print(f"Connection to '{database}' established")
    
    # create cursor
    cursor = conn.cursor()
    return conn, cursor


def drop_database_if_exist(database, conn, cursor):
    # drop database
    conn.autocommit = True
        # Drop the database if it exists
    cursor.execute(f"IF EXISTS (SELECT * FROM sys.databases WHERE name = '{database}') DROP DATABASE {database}")
    print(F"Database '{database}' dropped successfully if it existed.")
    
    
def create_new_database(database, conn, cursor):
    conn.autocommit = True
    cursor.execute(f"CREATE DATABASE {database}")
    print(f"Database '{database}' created successfully.")
    conn.autocommit = False
    
    
def load_csv_files(config):
    """
    Load CSV files based on the configuration.

    Args:
        config (dict): Configuration dictionary with file paths and schema.

    Returns:
        dict: Dictionary of DataFrames mapped to table names.
    """
    dataframes = {}
    for file_key, file_info in config['files'].items():
        print(f"Loading {file_key} from {file_info['path']}...")
        df = pd.read_csv(file_info['path'])
        dataframes[file_info['table']] = df
    print('Data extracted successfully from CSV files')
    for table, dataframe in dataframes.items():
        print(f'Table: {table} - Length: {len(dataframe)}')
    return dataframes



def detect_conflicted_records(df, table_name, primary_key, engine):
    # Query the database for existing primary keys
    query = f"SELECT {primary_key} FROM {table_name}"
    try:
        existing_ids = pd.read_sql(query, con=engine)[primary_key].tolist()
    except Exception as e:
        print(f"Error querying {table_name}: {e}")
        return []

    # Detect conflicts
    conflicted_ids = df[df[primary_key].isin(existing_ids)][primary_key].tolist()
    # Log conflicts
    if conflicted_ids:
        print(f"Conflicting records found in {table_name} with primary keys: {conflicted_ids}")
    return conflicted_ids

def remove_conflicted_records(df, conflicted_ids, primary_key):
    df_cleaned = df[~df[primary_key].isin(conflicted_ids)]
    print(f"Removed {len(conflicted_ids)} conflicted records. Remaining records: {len(df_cleaned)}")
    return df_cleaned

def load_cleaned_data(df, table_name, engine):
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Successfully loaded {len(df)} records into {table_name}.")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

