import os
import pandas as pd
import conn
from uainepydat import duckfunc
import csv

def dump_database_to_parquet(dump_name):
    # Create the root directory for the dump
    os.makedirs(dump_name, exist_ok=True)
    
    # Get database connection
    con = conn.get_connection()
    
    try:
        # Get list of all attached databases
        attached_dbs = duckfunc.get_attached_dbs(con)
        
        # Convert to pandas DataFrame if it's not already
        if not isinstance(attached_dbs, pd.DataFrame):
            attached_dbs = attached_dbs.df()
        
        # List to store dumped tables information
        dumped_tables = []
        
        # Ask for output format once
        output_format = input("Enter output format for all tables (csv/parquet): ").strip().lower()
        if output_format not in ['csv', 'parquet']:
            print("Invalid format, exiting.")
            return
        
        # Iterate through each database
        for _, db_row in attached_dbs.iterrows():
            db_name = db_row['DB_NAME']
            print("dumping for", db_name)
            db_dir = os.path.join(dump_name, db_name)
            
            # Create directory for this database
            os.makedirs(db_dir, exist_ok=True)
            
            # Get inventory of all tables
            inventory = duckfunc.get_inventory(con)
            if not isinstance(inventory, pd.DataFrame):
                inventory = inventory.df()
            
            # Filter inventory for current database
            db_tables = inventory[inventory['database_name'] == db_name]
            
            # Dump each table to the chosen format
            for _, table_row in db_tables.iterrows():
                table_name = table_row['table_name']
                output_path = os.path.join(db_dir, f"{table_name}.{output_format}")
                
                # Use duckfunc.save_from_db to query and save the table
                success = duckfunc.save_from_db(con, db_name, table_name, output_path)
                
                if success:
                    print(f"Dumped {db_name}.{table_name} to {output_path}")
                    # Append database and table name to the list
                    dumped_tables.append({'database_name': db_name, 'table_name': table_name})
                else:
                    print(f"Failed to dump {db_name}.{table_name} (table might not exist). Skipping.")
        
        # Write the dumped tables information to a CSV file
        csv_path = os.path.join(dump_name, 'dumped_tables.csv')
        with open(csv_path, 'w', newline='') as csvfile:
            fieldnames = ['database_name', 'table_name']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(dumped_tables)

        print(f"\nDumped tables information written to {csv_path}")
        
        print(f"\nDatabase dump completed successfully to {dump_name}")
        
    finally:
        conn.close_connection(con)

if __name__ == "__main__":
    dump_name = input("Enter the name for the dump folder: ")
    dump_database_to_parquet(dump_name) 