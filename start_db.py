import duckdb
import parse_db_list
import metadata

def create_and_attach_dbs():
    # Read the CSV file using pandas
    driver_name, all_names, primary_dbs = parse_db_list.parselist()

    # Connect to the driver database
    con = duckdb.connect(driver_name)

    # Attach primary databases
    for i, row in primary_dbs.iterrows():
        con.execute(f"ATTACH DATABASE '{row['PATH']}' AS {row['DB_NAME']}")

    attached = metadata.get_attached_dbs(con)
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

con.sql("CREATE TABLE users.test (i INTEGER)")
con.sql("CREATE TABLE models.test (k INTEGER)")
con.sql("CREATE TABLE a1.test (f INTEGER)")

#return the table list
metadata.get_inventory(con).show()
