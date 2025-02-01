import duckdb
import pandas as pd

# Connect to the driver database
driver_db = 'haupt.db'
conn = duckdb.connect(driver_db)

# Path to the CSV file
csv_file = 'db_list.csv'

# Read the CSV file using pandas
df = pd.read_csv(csv_file, delimiter='|')

# Attach primary databases
for index, row in df.iterrows():
    if row['PURPOSE'].strip() == 'primary':
        conn.sql(f"ATTACH DATABASE '{row['PATH'].strip()}' AS {row['DB_NAME'].strip()}")

print("Databases attached successfully.")
