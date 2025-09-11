"""
Main database startup module for DuckDB template system.

This module handles the complete database initialization process including
database attachment, table creation, metadata management, and view setup.
It serves as the primary entry point for starting the database system.
"""
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

DB_VER = "1.0"
UAINEDB_VER = "1.5.1"
print(f"DB_VER: {DB_VER}, UAINEDB_VER: {UAINEDB_VER}")

def attach_db(con, path, name, readonly=False):
    """
    Attach a database file to the DuckDB connection.
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.
    path : str
        Path to the database file to attach.
    name : str
        Alias name for the attached database.
    readonly : bool, optional
        Whether to attach the database in read-only mode, by default False.
    """
    fileio.create_filepath_dirs(path)
    ex_string = f"ATTACH DATABASE '{path}' AS {name}"
    if (readonly):
        ex_string += " (READ_ONLY)"
    con.execute(ex_string)

def create_and_attach_dbs(def_tables_path):
    """
    Create and attach all databases defined in the database list.
    
    Parameters
    ----------
    def_tables_path : str
        Path to the directory containing the db_list.csv file.
    
    Returns
    -------
    duckdb.DuckDBPyConnection
        DuckDB connection object with all databases attached.
    """
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
    """
    Check if the database version matches the expected version and warn if not.
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.
    """
    if (dbmet.db_version_match(con, DB_VER) == False):
        print("WARNING: Database version mismatch")
        print("Database version is: " + dbmet.get_db_version(con))
        print("Expecting version " + DB_VER)

def init_tables_from_list(con, new_table_list):
    """
    Initialize tables in the database from a table definition list.
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.
    new_table_list : str
        Path to the CSV file containing table definitions.
    """
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
    """
    Initialize and start the database system with all configurations.
    
    Parameters
    ----------
    def_tables_path : str, optional
        Path to the directory containing database and table definitions,
        by default "init_tables".
    
    Returns
    -------
    duckdb.DuckDBPyConnection
        Configured DuckDB connection object with all databases attached,
        tables initialized, and views set up.
    
    Raises
    ------
    ValueError
        If the META table has invalid data or salt check fails.
    """
    con = create_and_attach_dbs(def_tables_path)
    #attempt to make new tables
    init_tables_from_list(con, os.path.join(def_tables_path, "def_tables.csv"))

    # Ensure META_HISTORY table exists
    con.execute('''
        CREATE TABLE IF NOT EXISTS main.META_HISTORY AS 
        SELECT *, CURRENT_TIMESTAMP AS CREATE_DATE FROM main.META WHERE 1=0
    ''')

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
            "UAINEDB_VERSION": str(UAINEDB_VER),
            "PYTHON_VERSION": sys.version.split()[0],
            "DUCKDB_VERSION": duckdb.__version__,
            "SALT_CHECK": db_hash.generate_salt_check()  # Generate new SALT_CHECK for empty table
        }
        df.loc[n] = newtime
    elif n==1:
        oldtime = dbmet.get_last_launch_time(con)
        # Extract the existing SALT_CHECK value
        existing_salt_check = df.loc[0, "SALT_CHECK"]
        # Check for DB_VERSION or UAINEDB_VERSION change
        old_db_version = str(df.loc[0, "DB_VERSION"]) if "DB_VERSION" in df.columns else None
        old_uaine_version = str(df.loc[0, "UAINEDB_VERSION"]) if "UAINEDB_VERSION" in df.columns else None
        version_changed = (str(DB_VER) != old_db_version) or (str(UAINEDB_VER) != old_uaine_version)
        if version_changed:
            # Insert previous row into META_HISTORY with timestamp
            meta_hist_row = df.copy()
            meta_hist_row["CREATE_DATE"] = now
            # Write to META_HISTORY
            cols = ','.join(meta_hist_row.columns)
            for _, row in meta_hist_row.iterrows():
                values = tuple(row)
                placeholders = ','.join(['?']*len(values))
                con.execute(f"INSERT INTO main.META_HISTORY ({cols}) VALUES ({placeholders})", values)
        # Update other fields
        df.loc[0, "START_TIME"] = now
        df.loc[0, "PREV_START_TIME"] = oldtime
        df.loc[0, "PYTHON_VERSION"] = sys.version.split()[0]
        df.loc[0, "DUCKDB_VERSION"] = duckdb.__version__
        # Preserve the existing SALT_CHECK value
        df.loc[0, "SALT_CHECK"] = existing_salt_check
        df.loc[0, "DB_VERSION"] = str(DB_VER)
        df.loc[0, "UAINEDB_VERSION"] = str(UAINEDB_VER)
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
