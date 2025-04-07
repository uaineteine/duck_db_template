from uainepydat import duckfunc

# Append the directory path to openserver
import sys
sys.path.append("launcher")
from launcher import start_db as sdb

def get_connection():
    con = sdb.start_db()
    
    # Print the table list
    duckfunc.get_inventory(con).show()
    
    # Print the contents of the META table
    print(con.sql("SELECT * from meta.META"))
    
    return con

def close_connection(con):
    con.close()
