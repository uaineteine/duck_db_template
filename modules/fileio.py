import os

def check_folder_in_path(path):
    # Get the directory name of the path
    dir_name = os.path.dirname(path)
    
    if dir_name == "":
        print("No directory specified in the path.")
        return False
    else:
        # Check if the directory exists
        if os.path.isdir(dir_name):
            print(f"Folder '{dir_name}' exists.")
        else:
            print(f"Folder '{dir_name}' does not exist.")
        
        return os.path.isdir(dir_name)
