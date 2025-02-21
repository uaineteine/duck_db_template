import pandas as pd
from uainepydat import duckfunc

#read in master list
def read_table_list(path):
    df = pd.read_csv(path)
    return df

def init_these_tables(con, new_table_list):
    df = read_table_list(new_table_list)

    distinct_tables = df[["DBNAME","TABLENAME"]].drop_duplicates()

    for i, row in distinct_tables.iterrows():
        DBNAME = row["DBNAME"]
        TABLENAME = row["TABLENAME"]
        #filter for this result
        new_table_frame = df[df['DBNAME'] == DBNAME].drop(columns=["DBNAME"])
        new_table_frame = new_table_frame[new_table_frame['TABLENAME'] == TABLENAME].drop(columns=["TABLENAME"])

        duckfunc.init_table(con, new_table_frame, DBNAME, TABLENAME)
