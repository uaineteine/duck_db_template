import duckdb
from datetime import datetime

def get_last_launch_times(con):
    return con.sql("SELECT * from main.META").df()
