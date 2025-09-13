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
UAINEDB_VER = "1.6.1"
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
    
    # Collect foreign key relationships for proper table ordering
    table_dependencies = {}
    table_info = {}
    
    # First pass: collect table information and dependencies
    for i, row in distinct_tables.iterrows():
        DBNAME = row["DBNAME"]
        TABLENAME = row["TABLENAME"]
        table_key = f"{DBNAME}.{TABLENAME}"
        table_dependencies[table_key] = []
        
        # filter for this result
        new_table_frame = df[df['DBNAME'] == DBNAME].drop(columns=["DBNAME"])
        new_table_frame = new_table_frame[new_table_frame['TABLENAME'] == TABLENAME].drop(columns=["TABLENAME"])

        # Process LINKS_TO column if it exists
        foreign_key_specs = []
        if 'LINKS_TO' in new_table_frame.columns:
            # Get links for this table by finding the first non-empty LINKS_TO value
            links_series = new_table_frame['LINKS_TO'].dropna()
            links_str = ""
            if not links_series.empty:
                # Find the first non-empty LINKS_TO value for this table
                for link_val in links_series:
                    if link_val and str(link_val).strip():
                        links_str = str(link_val).strip()
                        break
            
            # Add foreign key columns for linked tables
            if links_str:
                import pandas as pd
                linked_tables = [table.strip() for table in links_str.split(',') if table.strip()]
                for linked_table in linked_tables:
                    # Extract table name from DBNAME.TABLENAME format if present
                    if '.' in linked_table:
                        ref_db = linked_table.split('.')[0]
                        ref_table = linked_table.split('.')[1]
                        ref_table_key = f"{ref_db}.{ref_table}"
                    else:
                        ref_db = DBNAME  # Fallback for backward compatibility
                        ref_table = linked_table
                        ref_table_key = f"{ref_db}.{ref_table}"
                    
                    table_dependencies[table_key].append(ref_table_key)
                    
                    fk_column_name = f"{ref_table}_ID"
                    # Check if foreign key column already exists
                    if not (new_table_frame["VARNAME"] == fk_column_name).any():
                        # Use simple table name for references within same database
                        if ref_db == DBNAME:
                            fk_type = f"INT64 REFERENCES {ref_table}(ID)"
                        else:
                            # Cross-database references not supported, create without constraint
                            fk_type = "INT64"
                            print(f"Warning: Cross-database foreign key not supported for {DBNAME}.{TABLENAME}.{fk_column_name} -> {ref_db}.{ref_table}")
                        
                        fk_row = pd.DataFrame({
                            "VARNAME": [fk_column_name],
                            "TYPE": [fk_type],
                            "LINKS_TO": [""]
                        })
                        new_table_frame = pd.concat([new_table_frame, fk_row], ignore_index=True)
                        foreign_key_specs.append((fk_column_name, ref_db, ref_table))
            
            # Drop LINKS_TO column before creating table
            new_table_frame = new_table_frame.drop(columns=["LINKS_TO"])

        # Always add ID column as INT64 PRIMARY KEY if not present
        if not (new_table_frame["VARNAME"] == "ID").any():
            import pandas as pd
            id_row = pd.DataFrame({
                "VARNAME": ["ID"],
                "TYPE": ["INT64 PRIMARY KEY"]
            })
            new_table_frame = pd.concat([id_row, new_table_frame], ignore_index=True)

        table_info[table_key] = {
            'frame': new_table_frame,
            'dbname': DBNAME,
            'tablename': TABLENAME,
            'foreign_keys': foreign_key_specs
        }
    
    # Topological sort to determine creation order
    def topological_sort(dependencies):
        visited = set()
        temp_visited = set()
        result = []
        
        def visit(table):
            if table in temp_visited:
                # Circular dependency detected, create without foreign keys first
                return []
            if table in visited:
                return []
                
            temp_visited.add(table)
            deps = dependencies.get(table, [])
            for dep in deps:
                if dep in dependencies:  # Only process if dependency is in our table list
                    visit(dep)
            temp_visited.remove(table)
            visited.add(table)
            result.append(table)
            
        for table in dependencies:
            if table not in visited:
                visit(table)
                
        return result
    
    creation_order = topological_sort(table_dependencies)
    
    # Create tables in dependency order
    for table_key in creation_order:
        if table_key in table_info:
            info = table_info[table_key]
            try:
                # Set database context if creating tables with foreign keys in attached databases
                if info['foreign_keys'] and info['dbname'] != 'main_db':
                    # Use the database context for foreign key references
                    con.execute(f"USE {info['dbname']}")
                
                duckfunc.init_table(con, info['frame'], info['dbname'], info['tablename'])
                
                # Log foreign key relationships that were created
                for fk_col, ref_db, ref_table in info['foreign_keys']:
                    print(f"Created foreign key: {info['dbname']}.{info['tablename']}.{fk_col} -> {ref_db}.{ref_table}.ID")
                    
            except Exception as e:
                # If foreign key creation fails, try creating without foreign keys
                print(f"Warning: Failed to create table {table_key} with foreign keys: {e}")
                print(f"Retrying without foreign key constraints...")
                
                # Create a version without foreign key constraints
                frame_no_fk = info['frame'].copy()
                for fk_col, _, _ in info['foreign_keys']:
                    # Change foreign key column type to just INT64
                    mask = frame_no_fk['VARNAME'] == fk_col
                    frame_no_fk.loc[mask, 'TYPE'] = 'INT64'
                
                # Reset database context and try again
                con.execute("USE main_db")
                duckfunc.init_table(con, frame_no_fk, info['dbname'], info['tablename'])
            finally:
                # Always reset to main database context
                try:
                    con.execute("USE main_db")
                except:
                    pass

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
