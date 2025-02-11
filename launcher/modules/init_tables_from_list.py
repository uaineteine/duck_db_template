import pandas as pd
from uainepydat import duckdata

#read in master list
def read_table_list(path):
    df = pd.read_csv(path)
    return df

def init_table(con, frame, db, tablename):
    #takes in a frame of string columns VARNAME and TYPE
    #those formats should be duckDB compatible
    exist = duckdata.does_table_exist(con, db, tablename)
    if (exist == False):
        print("Creating table " + db + "." + tablename)

        tbl_ref = db + "." + tablename
        exstring = "CREATE TABLE IF NOT EXISTS " + tbl_ref + "("
        # Create a comma-delimited list with variable names and types
        exstring = exstring + ', '.join([f"{row['VARNAME']} {row['TYPE']}" for _, row in frame.iterrows()])
        exstring = exstring + ")"
        con.sql(exstring)

def init_these_tables(con, new_table_list):
    df = read_table_list(new_table_list)

    distinct_tables = df[["DBNAME","TABLENAME"]].drop_duplicates()

    for i, row in distinct_tables.iterrows():
        DBNAME = row["DBNAME"]
        TABLENAME = row["TABLENAME"]
        #filter for this result
        new_table_frame = df[df['DBNAME'] == DBNAME].drop(columns=["DBNAME"])
        new_table_frame = new_table_frame[new_table_frame['TABLENAME'] == TABLENAME].drop(columns=["TABLENAME"])

        init_table(con, new_table_frame, DBNAME, TABLENAME)
