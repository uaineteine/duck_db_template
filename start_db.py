import duckdb
import pandas as pd

# Read the CSV file using pandas
df = pd.read_csv("db_list.csv", delimiter='|')

driver_name = df[df['PURPOSE'].str.strip() == 'main']['PATH'][0]
# Connect to the driver database
con = duckdb.connect(driver_name)

# Attach primary databases
primary_dbs = df[df['PURPOSE'].str.strip() == 'primary'] # Filter for primary databases
# Attach primary databases
for index, row in primary_dbs.iterrows():
    con.execute(f"ATTACH DATABASE '{row['PATH'].strip()}' AS {row['DB_NAME'].strip()}")

print("Primary databases attached successfully.")
