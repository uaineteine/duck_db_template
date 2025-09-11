print("[Uaine DB starter template]")
import os
import duckdb
import sys
import pandas as pd
from uainepydat import fileio
from uainepydat import dataio
from uainepydat import duckfunc
#modules
import dbmet
import parse_db_list
import db_hash
import views

DB_VER = "1.5.1"
print(DB_VER)

def attach_db(con, path, name, readonly=False):
    fileio.create_filepath_dirs(path)
    ex_string = f"ATTACH DATABASE '{path}' AS {name}"
    if (readonly):
        ex_string += " (READ_ONLY)"
    con.execute(ex_string)

def create_and_attach_dbs(def_tables_path):
    # Read the CSV file using pandas
    dblist = os.path.join(def_tables_path, "db_list.csv")
    driver_name, all_names, primary_dbs, secondary_dbs = parse_db_list.parselist(dblist)

    # Connect to the driver database
    fileio.create_filepath_dirs(driver_name)
    con = duckdb.connect(driver_name)

    # Attach primary databases
    for i, row in primary_dbs.iterrows():
        attach_db(con, row['PATH'], row['DB_NAME'])

    # Attach secondary databases as read only
    for i, row in secondary_dbs.iterrows():
        attach_db(con, row['PATH'], row['DB_NAME'], readonly=True)

    attached = duckfunc.get_attached_dbs(con)
    print("Attached the following databases")
    attached.show()

    all_assigned = attached["DB_NAME"] not in all_names
    if all_assigned:
        print("All databases attached successfully.")
    else:
        print(f"Missing databases")

    return con

def check_db_version(con):
    if (dbmet.db_version_match(con, DB_VER) == False):
        print("WARNING: Database version mismatch")
        print("Database version is: " + dbmet.get_db_version(con))
        print("Expecting version " + DB_VER)

def init_tables_from_list(con, new_table_list):
    df = dataio.read_flat_df(new_table_list)

    distinct_tables = df[["DBNAME","TABLENAME"]].drop_duplicates()

    for i, row in distinct_tables.iterrows():
        DBNAME = row["DBNAME"]
        TABLENAME = row["TABLENAME"]
        # filter for this result
        new_table_frame = df[df['DBNAME'] == DBNAME].drop(columns=["DBNAME"])
        new_table_frame = new_table_frame[new_table_frame['TABLENAME'] == TABLENAME].drop(columns=["TABLENAME"])

        # Always add ID column as INT64 PRIMARY KEY if not present
        if not (new_table_frame["VARNAME"] == "ID").any():
            import pandas as pd
            id_row = pd.DataFrame({
                "VARNAME": ["ID"],
                "TYPE": ["INT64 PRIMARY KEY"]
            })
            new_table_frame = pd.concat([id_row, new_table_frame], ignore_index=True)

        duckfunc.init_table(con, new_table_frame, DBNAME, TABLENAME)

def salt_checking(con) -> bool:
    """
    Checks if the SALT_CHECK value in the META table matches the current hashed salt.
    
    Args:
        con: DuckDB connection object.
    
    Returns:
        bool: True if the SALT_CHECK value matches the current hashed salt, False otherwise.
    """
    # Get the SALT_CHECK value from the META table
    df = con.execute("SELECT SALT_CHECK FROM main.META").fetchdf()
    if df.empty:
        return False  # No SALT_CHECK value found
    
    stored_salt_check = df["SALT_CHECK"].iloc[0]
    
    # Generate the current SALT_CHECK value
    current_salt_check = db_hash.generate_salt_check()
    
    # Compare the stored and current values
    return stored_salt_check == current_salt_check

def start_db(def_tables_path="init_tables"):
    con = create_and_attach_dbs(def_tables_path)
    #attempt to make new tables
    init_tables_from_list(con, os.path.join(def_tables_path, "def_tables.csv"))

    #read out the system time and update it necessary
    now = duckfunc.getCurrentTimeForDuck(timezone_included=True)
    df = dbmet.get_meta_table(con)
    n = len(df)
    if n == 0: #empty table, set this up
        newtime = {
            "ID": 1,
            "START_TIME": now, 
            "PREV_START_TIME" : "", 
            "DB_VERSION" : str(DB_VER),
            "PYTHON_VERSION": sys.version.split()[0],
            "DUCKDB_VERSION": duckdb.__version__,
            "SALT_CHECK": db_hash.generate_salt_check()  # Generate new SALT_CHECK for empty table
        }
        df.loc[n] = newtime
    elif n==1:
        oldtime = dbmet.get_last_launch_time(con)
        # Extract the existing SALT_CHECK value
        existing_salt_check = df["SALT_CHECK"][0]
        # Update other fields
        df["START_TIME"][0] = now
        df["PREV_START_TIME"][0] = oldtime
        df["PYTHON_VERSION"][0] = sys.version.split()[0]
        df["DUCKDB_VERSION"][0] = duckdb.__version__
        # Preserve the existing SALT_CHECK value
        df["SALT_CHECK"][0] = existing_salt_check
    else:
        raise ValueError("main.META is broken, too many results")

    # Write the DataFrame back to DuckDB, overwriting the existing table
    con.execute("CREATE OR REPLACE TABLE main.META AS SELECT * FROM df")

    #check db_versions
    check_db_version(con)

    # Check the SALT_CHECK value
    if not salt_checking(con):
        raise ValueError("SALT_CHECK value mismatch. Potential integrity issue detected.")
    
    views.setupviews(con, def_tables_path)

    return con
