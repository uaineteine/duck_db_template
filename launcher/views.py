import os
import pandas as pd
from uainepydat import dataio

def get_db_views(con) -> pd.DataFrame:
    """
    Retrieve all views from the connected DuckDB database.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.

    Returns
    -------
    pandas.DataFrame
        DataFrame with two columns:
        - ``VIEW_NAME``: The name of each view.
        - ``SQL``: The SQL definition of each view.
    """
    sql_query = "SELECT view_name AS VIEW_NAME, sql AS SQL FROM duckdb_views();"
    df = con.sql(sql_query).df()

    return df


def read_db_csv(tbl_views_path: str) -> pd.DataFrame:
    """
    Load view definitions from a pipe-separated values (PSV) file.

    Parameters
    ----------
    tbl_views_path : str
        Path to the ``views.csv`` file containing view definitions
        (expected to be PSV formatted).

    Returns
    -------
    pandas.DataFrame
        DataFrame containing view metadata read from the CSV file.
    """
    df = dataio.read_flat_psv(tbl_views_path)

    return df


def setupviews(con, def_tables_path: str) -> None:
    """
    Synchronize views in DuckDB with definitions stored in a CSV file.

    This function compares the views defined in the database with those
    listed in ``views.csv``. Any views present in the CSV but missing from
    the database are created.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.
    def_tables_path : str
        Path to the directory containing ``views.csv`` with view definitions.

    Returns
    -------
    None
        Executes view creation in the database, no direct return value.
    """
    # Get the views
    db_views = get_db_views(con)
    csv_views = read_db_csv(os.path.join(def_tables_path, "views.csv"))
    remaining_views = pd.concat([csv_views, db_views, db_views]).drop_duplicates(keep=False)

    # Assuming remaining_views is a DataFrame with columns: VIEW_NAME and SQL
    for _, row in remaining_views.iterrows():
        view_name = row['VIEW_NAME']
        sql_query = row['SQL']
        
        # Construct the CREATE VIEW statement
        create_view_stmt = f"CREATE VIEW {view_name} AS {sql_query};"
        
        # Execute the statement in DuckDB
        con.execute(create_view_stmt)
