from modules import *
import start_db as sdb

con = sdb.start_db()

con.sql("CREATE TABLE users.test (i INTEGER)")
con.sql("CREATE TABLE models.test (k INTEGER)")
con.sql("CREATE TABLE a1.test (f INTEGER)")

#return the table list
metadata.get_inventory(con).show()

con.close()
