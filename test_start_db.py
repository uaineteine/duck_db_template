from modules import metadata
from modules import init_tables_from_list
import start_db as sdb

con = sdb.start_db()

#con.sql("CREATE TABLE IF NOT EXISTS users.test (i INTEGER)")
#con.sql("CREATE TABLE IF NOT EXISTS models.test (k INTEGER)")
#con.sql("CREATE TABLE IF NOT EXISTS a1.test (f INTEGER)")

#attempt to make new tables
init_tables_from_list.init_these_tables(con, "init_tables/new_tables.csv")

#return the table list
metadata.get_inventory(con).show()

con.close()
