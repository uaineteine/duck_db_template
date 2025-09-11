"""
Database hash utilities for salt-based hashing operations.

This module provides functions for loading salt values and creating
hashed data using the database salt for security purposes.
"""
from uainepydat import datahash

# Module-level variable to store the salt file location
salt_location = 'db_salt.txt'

def load_salt() -> str:
    """
    Load salt value from the salt file.
    
    Returns
    -------
    str
        The salt value read from the file, with whitespace stripped.
    
    Raises
    ------
    FileNotFoundError
        If the salt file cannot be found.
    IOError
        If there's an error reading the salt file.
    """
    # Read the salt from the file
    with open(salt_location, 'r') as file:
        salt = file.read().strip()
    return salt

def hash_with_db_salt(data: str) -> str:
    """
    Hash data using the database salt value.
    
    Parameters
    ----------
    data : str
        The data string to be hashed.
    
    Returns
    -------
    str
        The hashed data combined with the salt using SHA256 algorithm.
    """
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
