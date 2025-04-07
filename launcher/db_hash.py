from uainepydat import datahash

# Module-level variable to store the salt file location
salt_location = 'db_salt.txt'

def load_salt() -> str:
    # Read the salt from the file
    with open(salt_location, 'r') as file:
        salt = file.read().strip()
    return salt

def hash_with_db_salt(data: str) -> str:
    # Load the salt using the new function
    salt = load_salt()
    
    # Combine the data with the salt
    return datahash.hash256(data, salt)

def generate_salt_check() -> str:
    """
    Generates the SALT_CHECK value by loading the salt and hashing it.
    """
    salt = load_salt()
    return hash_with_db_salt(salt)
