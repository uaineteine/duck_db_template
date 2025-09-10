import pandas as pd
from uainepydat import dataio

def get_db_views(con) -> pd.DataFrame:
    """
    Retrieve all views from the connected DuckDB database.

    Parameters:
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.

    Returns:
    -------
    pandas.DataFrame
        A DataFrame containing the names and SQL definitions of all views
        in the current DuckDB session.
    """
    sql_query = "SELECT view_name AS VIEW_NAME, sql AS SQL FROM duckdb_views();"
    df = con.sql(sql_query).df()

    return df

def read_db_csv(tbl_views_path:str) -> pd.DataFrame:
    df = dataio.read_flat_psv(tbl_views_path)

    return df
