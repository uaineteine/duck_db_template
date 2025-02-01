import duckdb
import pandas as pd

# Connect to the driver database
driver_db = 'haupt.db'
con = duckdb.connect(driver_db)

# Path to the CSV file
csv_file = 'db_list.csv'

# Read the CSV file using pandas
df = pd.read_csv(csv_file, delimiter='|')

# Attach primary databases
primary_dbs = df[df['PURPOSE'].str.strip() == 'primary'] # Filter for primary databases
# Attach primary databases
for index, row in primary_dbs.iterrows():
    con.execute(f"ATTACH DATABASE '{row['PATH'].strip()}' AS {row['DB_NAME'].strip()}")

print("Primary databases attached successfully."
