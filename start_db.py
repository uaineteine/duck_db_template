import duckdb
import os
from modules import metadata
from modules import parse_db_list
from modules import fileio

def create_db_dir(db_path):
    if fileio.check_folder_in_path(db_path):
        db_path = os.path.dirname(db_path)
        os.makedirs(db_path, exist_ok=True)

def create_and_attach_dbs():
    # Read the CSV file using pandas
    driver_name, all_names, primary_dbs, secondary_dbs = parse_db_list.parselist("db_list.csv")

    # Connect to the driver database
    con = duckdb.connect(driver_name)

    # Attach primary databases
    for i, row in primary_dbs.iterrows():
        create_db_dir(row['PATH'])
        con.execute(f"ATTACH DATABASE '{row['PATH']}' AS {row['DB_NAME']}")

    # Attach secondary databases as read only
    for i, row in secondary_dbs.iterrows():
        create_db_dir(row['PATH'])
        con.execute(f"ATTACH DATABASE '{row['PATH']}' AS {row['DB_NAME']} (READ_ONLY)")

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
    return con
