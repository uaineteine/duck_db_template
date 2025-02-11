from datetime import datetime

def get_meta_table(con):
    return con.sql("SELECT * from main.META").df()

def get_last_launch_time(con):
    df = get_meta_table(con)
    return df["START_TIME"][0]

def get_db_version(con):
    df = get_meta_table(con)
    return df["DB_VERSION"][0]

def db_version_match(con, expecting_version):
    ver = get_db_version()
    if (ver == expecting_version):
        return True
    #else
    return False