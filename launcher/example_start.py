from uainepydat import duckdata
import start_db as sdb

con = sdb.start_db()

#return the table list
duckdata.get_inventory(con).show()

print(con.sql("SELECT * from main.META"))

con.close()
