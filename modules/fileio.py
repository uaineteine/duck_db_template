import os
import pandas as pd

def read_psv(path):
    return pd.read_csv(path, delimiter='|')

def check_folder_in_filepath(path):
    # Get the directory name of the path
    dir_name = os.path.dirname(path)
    
    if dir_name == "":
        #print("No directory specified in the path.")
        return False
    else:
        # Check if the path has a file extension
        _, file_extension = os.path.splitext(dir_name)
        if file_extension:
            #print(f"The path '{path}' appears to be a file.")
            return False
        else:
            #print(f"The path '{path}' does not appear to have a file extension.")
            return True
        
def create_filepath_dirs(path):
    if check_folder_in_filepath(path):
        path = os.path.dirname(path)
        os.makedirs(path, exist_ok=True)