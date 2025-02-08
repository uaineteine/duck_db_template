import duckdb
import pandas as pd
from modules import fileio

#read in master list
def read_table_list(path):
    df = pd.read_csv(path)
    return df

def init_these_tables(new_table_list):
    df = read_table_list(new_table_list)

    distinct_tables = df[["DBNAME","TABLENAME"]].drop_duplicates()
    print(distinct_tables.head())
