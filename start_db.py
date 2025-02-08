print("[Uaine DB starter v1.2]")
import duckdb
from modules import metadata
from modules import parse_db_list
from modules import fileio
from modules import init_tables_from_list

def attach_db(con, path, name, readonly=False):
    fileio.create_filepath_dirs(path)
    ex_string = f"ATTACH DATABASE '{path}' AS {name}"
    if (readonly):
        ex_string += " (READ_ONLY)"
    con.execute(ex_string)

def create_and_attach_dbs():
    # Read the CSV file using pandas
    driver_name, all_names, primary_dbs, secondary_dbs = parse_db_list.parselist("db_list.csv")

    # Connect to the driver database
    fileio.create_filepath_dirs(driver_name)
    con = duckdb.connect(driver_name)

    # Attach primary databases
    for i, row in primary_dbs.iterrows():
        attach_db(con, row['PATH'], row['DB_NAME'])

    # Attach secondary databases as read only
    for i, row in secondary_dbs.iterrows():
        attach_db(con, row['PATH'], row['DB_NAME'], readonly=True)

    attached = metadata.get_attached_dbs(con)
    print("Attached the following databases")
    attached.show()

    all_assigned = attached["DB_NAME"] not in all_names
    if all_assigned:
        print("All databases attached successfully.")
    else:
        print(f"Missing databases")

    return con

def start_db():
    con = create_and_attach_dbs()
    #attempt to make new tables
    init_tables_from_list.init_these_tables(con, "init_tables/def_tables.csv")
    return con
