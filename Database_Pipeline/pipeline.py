import pyodbc
import json
import pandas as pd 
import warnings
from sqlalchemy import create_engine

import function_pipeline as fp

# ---------------------------------- EXTRACT DATA 
# load config json file for tables
with open('tables_config.json', 'r') as config_file:
    tables_config = json.load(config_file)

# Load data from csv files
dict_dataframes = fp.load_csv_files(tables_config)


# ---------------------------------- LOAD DATA 
# Connect database: HospitalOperation
engine_hospitaloperation = fp.connect_database_sqlalchemy(database='HospitalOperation')

# check conflicts and load
for table, dataframe in dict_dataframes.items():
    # get primary key from json file of tables
    json_name = table.lower()
    key = tables_config['files'][json_name]['key']
    
    # find conflicts records
    list_conflicted_records = fp.detect_conflicted_records(df=dataframe, table_name=table, primary_key=key,engine=engine_hospitaloperation)
    
    # clean data 
    df_cleaned = fp.remove_conflicted_records(df=dataframe, conflicted_ids=list_conflicted_records, primary_key=key)
    
    # load data
    fp.load_cleaned_data(df=df_cleaned, table_name=table, engine=engine_hospitaloperation)