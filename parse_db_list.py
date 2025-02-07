import pandas as pd

def readlist():
    # Read the CSV file using pandas
    df = pd.read_csv("db_list.csv", delimiter='|')
    return df

def clean_db_list(df):
    print("Cleaning database list file...")
    df_cleaned = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    if (df_cleaned.equals(df) == False):
        "Frame was cleaned to remove whitespace, please fix this in your list"
    return df_cleaned

def verify_if_any_duplicates(df):
    all_names = df["DB_NAME"]
    if all_names.duplicated().any():
        raise ValueError("DB_LIST has duplicated database names")
    
    return all_names

def verify_if_1_maindb(df):
    driver_names = df[df['PURPOSE'] == 'main']['PATH']
    n_names = driver_names.shape[0]
    if n_names > 1:
        raise ValueError("DB_LIST can only contain 1 main database")

    return driver_names[0]

def parselist():
    print("Parsing database list file...")
    df = clean_db_list(readlist())

    #check for duplicated names
    all_names = verify_if_any_duplicates(df)

    #get the driver name
    driver_name = verify_if_1_maindb(df)

    primary_dbs = df[df['PURPOSE'] == 'primary'] # Filter for primary databases
    primary_dbs = df[df['PURPOSE'] == 'secondary'] # Filter for secondary databases

    return driver_name, all_names, primary_dbs, secondary_dbs
