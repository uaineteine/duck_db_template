"""
Database list parsing utilities for processing database configuration files.

This module provides functions to parse, validate, and clean database
list CSV files, ensuring proper database configuration and preventing
duplicate entries.
"""
from uainepydat import dataio
from uainepydat import dataclean

def clean_db_list(df):
    """
    Clean whitespace from the database list DataFrame.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing database list information.
    
    Returns
    -------
    pandas.DataFrame
        DataFrame with whitespace cleaned from all string columns.
    """
    print("Cleaning database list file...")
    df_cleaned = dataclean.clean_whitespace_in_df(df)
    if (df_cleaned.equals(df) == False):
        print("Frame was cleaned to remove whitespace, please fix this in your list")
    return df_cleaned

def verify_if_any_duplicates(df):
    """
    Verify that there are no duplicate database names in the list.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing database list with a 'DB_NAME' column.
    
    Returns
    -------
    pandas.Series
        Series containing all database names.
    
    Raises
    ------
    ValueError
        If duplicate database names are found.
    """
    all_names = df["DB_NAME"]
    if all_names.duplicated().any():
        raise ValueError("DB_LIST has duplicated database names")
    
    return all_names

def verify_if_1_metadb(df):
    """
    Verify that exactly one meta database exists and return its path.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing database list with 'PURPOSE' and 'PATH' columns.
    
    Returns
    -------
    str
        Path to the single meta database.
    
    Raises
    ------
    ValueError
        If more than one meta database is found.
    """
    driver_names = df[df['PURPOSE'] == 'main']['PATH']
    n_names = driver_names.shape[0]
    if n_names > 1:
        raise ValueError("DB_LIST can only contain 1 meta database")

    return driver_names[0]

def parselist(csvpath):
    """
    Parse the database list CSV file and extract database information.
    
    Parameters
    ----------
    csvpath : str
        Path to the CSV file containing database list.
    
    Returns
    -------
    tuple
        A tuple containing:
        - driver_name (str): Path to the main/meta database
        - all_names (pandas.Series): All database names
        - primary_dbs (pandas.DataFrame): DataFrame of primary databases
        - secondary_dbs (pandas.DataFrame): DataFrame of secondary databases
    
    Raises
    ------
    ValueError
        If duplicate database names are found or more than one meta database exists.
    """
    print("Parsing database list file...")
    df = clean_db_list(dataio.read_flat_psv(csvpath))

    #check for duplicated names
    all_names = verify_if_any_duplicates(df)

    #get the driver name
    driver_name = verify_if_1_metadb(df)

    primary_dbs = df[df['PURPOSE'] == 'primary'] # Filter for primary databases
    secondary_dbs = df[df['PURPOSE'] == 'secondary'] # Filter for secondary databases

    return driver_name, all_names, primary_dbs, secondary_dbs
