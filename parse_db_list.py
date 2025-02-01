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

def parselist():
    print("Parsing database list file...")
    df = clean_db_list(readlist())

    #get the driver name
    driver_name = df[df['PURPOSE'] == 'main']['PATH'][0]

    #check the primary_names
    primary_dbs = df[df['PURPOSE'] == 'primary'] # Filter for primary databases

    return driver_name, df["DB_NAME"], primary_dbs
