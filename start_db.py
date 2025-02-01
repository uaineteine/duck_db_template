import duckdb
import parse_db_list

def get_attached_dbs(db_con):
    return db_con.sql("SELECT database_name as DB_NAME, path as PATH, type FROM duckdb_databases")

def get_inventory(db_con):
    return db_con.sql("SELECT * from duckdb_tables")

def create_and_attach_dbs():
    # Read the CSV file using pandas
    driver_name, all_names, primary_dbs = parse_db_list.parselist()

    # Connect to the driver database
    con = duckdb.connect(driver_name)

    # Attach primary databases
    for i, row in primary_dbs.iterrows():
        con.execute(f"ATTACH DATABASE '{row['PATH']}' AS {row['DB_NAME']}")

    attached = get_attached_dbs(con)
    print("Attached the following databases")
    attached.show()

    all_assigned = attached["DB_NAME"] not in all_names
    if all_assigned:
        print("Primary databases attached successfully.")
    else:
        print(f"Missing databases")

    return con

def start_db():
    con = create_and_attach_dbs()
    return con

con = start_db()

get_inventory(con).show()
