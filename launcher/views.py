import os
import pandas as pd
from uainepydat import dataio

def get_db_views(con):
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
    df = con.sql(sql_query)

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
    the database are created, and any existing views are updated to match
    the CSV definition.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        An active DuckDB connection object.
    def_tables_path : str
        Path to the directory containing ``views.csv`` with view definitions.

    Returns
    -------
    None
        Executes view creation or replacement in the database, no direct
        return value.
    """

    # Get the views
    db_views = get_db_views(con).df()
    csv_views = read_db_csv(os.path.join(def_tables_path, "views.csv"))

    # Only create views from csv_views that do not already exist in db_views
    existing_view_names = set(db_views["VIEW_NAME"].astype(str))
    views_to_create = csv_views[~csv_views["VIEW_NAME"].astype(str).isin(existing_view_names)]

    for _, row in views_to_create.iterrows():
        view_name = row['VIEW_NAME']
        sql_query = row['SQL']
        create_view_stmt = f"CREATE VIEW {view_name} AS {sql_query};"
        con.execute(create_view_stmt)
        print(f"Created view: {view_name}\nSQL: {sql_query}\n")

    # After all views are created, display the list of all views in the DB
    print("\nAll views in the database:")
    all_views = get_db_views(con).show()

    #d = con.execute("SELECT * FROM db_num_tables_view;").df()
    #print(d)
