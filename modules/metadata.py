import duckdb

def get_attached_dbs(db_con):
    return db_con.sql("SELECT database_name as DB_NAME, path as PATH, type FROM duckdb_databases")

def get_inventory(db_con):
    return db_con.sql("SELECT * from duckdb_tables")

def does_table_exist(db_con, dbname, tablename):
    ex_string = "SELECT COUNT(*) AS c from duckdb_tables WHERE table_name = '" + tablename + "'"
    ex_string = ex_string + " AND database_name = '" + dbname + "'"
    count = db_con.sql(ex_string).df()["c"][0]
    if (count == 1):
        return True
    #else
    return False
