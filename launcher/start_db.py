print("[Uaine DB starter template]")
import os
import duckdb
import pandas as pd
from uainepydat import duckfunc
from uainepydat import fileio
from modules import dbmet
from modules import parse_db_list
from modules import init_tables_from_list

DB_VER = "1.2.7"
print(DB_VER)

def attach_db(con, path, name, readonly=False):
    fileio.create_filepath_dirs(path)
    ex_string = f"ATTACH DATABASE '{path}' AS {name}"
    if (readonly):
        ex_string += " (READ_ONLY)"
    con.execute(ex_string)

def create_and_attach_dbs(launcher_loc="."):
    # Read the CSV file using pandas
    driver_name, all_names, primary_dbs, secondary_dbs = parse_db_list.parselist(os.path.join(launcher_loc,"init_tables/db_list.csv"))

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

def start_db(launcher_loc="."):
    con = create_and_attach_dbs(launcher_loc=launcher_loc)
    #attempt to make new tables
    init_tables_from_list.init_these_tables(con, os.path.join(launcher_loc,"init_tables/def_tables.csv"))

    #read out the system time and update it necessary
    now = duckfunc.getCurrentTimeForDuck(timezone_included=True)
    df = dbmet.get_meta_table(con)
    n = len(df)
    if n == 0: #empty table, set this up
        newtime = {"START_TIME": now, "PREV_START_TIME" : "", "DB_VERSION" : str(DB_VER)}
        df.loc[n] = newtime
    elif n==1:
        oldtime = dbmet.get_last_launch_time(con)
        df["START_TIME"][0] = now
        df["PREV_START_TIME"][0] = oldtime
    else:
        raise ValueError("main.META is broken, too many results")

    # Write the DataFrame back to DuckDB, overwriting the existing table
    con.execute("CREATE OR REPLACE TABLE main.META AS SELECT * FROM df")

    #check db_versions
    check_db_version(con)
    
    return con
