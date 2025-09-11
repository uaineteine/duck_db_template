"""
Database metadata utilities for retrieving and checking META table information.

This module provides functions to access the META table and verify
database version compatibility.
"""
from datetime import datetime

def get_meta_table(con):
    """
    Retrieve the META table from the main database.
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.
    
    Returns
    -------
    pandas.DataFrame
        DataFrame containing all rows and columns from the main.META table.
    """
    return con.sql("SELECT * from main.META").df()

def get_last_launch_time(con):
    """
    Get the last launch time from the META table.
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.
    
    Returns
    -------
    str or datetime
        The START_TIME value from the first row of the META table.
    """
    df = get_meta_table(con)
    return df["START_TIME"][0]

def get_db_version(con):
    """
    Get the database version from the META table.
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.
    
    Returns
    -------
    str
        The DB_VERSION value from the first row of the META table.
    """
    df = get_meta_table(con)
    return df["DB_VERSION"][0]

def db_version_match(con, expecting_version):
    """
    Check if the database version matches the expected version.
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.
    expecting_version : str
        The expected database version to compare against.
    
    Returns
    -------
    bool
        True if the database version matches the expected version, False otherwise.
    """
    ver = get_db_version(con)
    if (ver == expecting_version):
        return True
    #else
    return False