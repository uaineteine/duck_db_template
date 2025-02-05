import duckdb

def get_attached_dbs(db_con):
    return db_con.sql("SELECT database_name as DB_NAME, path as PATH, type FROM duckdb_databases")

def get_inventory(db_con):
    return db_con.sql("SELECT * from duckdb_tables")
