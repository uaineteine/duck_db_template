"""
Database hash utilities for salt-based hashing operations.

This module provides functions for loading salt values and creating
hashed data using the database salt for security purposes.
Now loads from db_salt.json, supporting hash method, key, and truncation length.
"""
import json
from uainepydat import datahash

# Module-level variable to store the salt file location
salt_location = 'db_salt.json'

def load_salt_config() -> dict:
    """
    Load salt configuration from the JSON file.
    Returns
    -------
    dict
        Dictionary with keys: hash_method, key, truncation_length
    Raises
    ------
    FileNotFoundError
        If the salt file cannot be found.
    IOError
        If there's an error reading the salt file.
    ValueError
        If the JSON is invalid or missing required fields.
    """
    with open(salt_location, 'r') as file:
        config = json.load(file)
    # Validate required fields
    if not all(k in config for k in ("hash_method", "key", "truncation_length")):
        raise ValueError("db_salt.json missing required fields")
    return config

def load_salt() -> str:
    """
    Load salt value (the key) from the salt config file.
    Returns
    -------
    str
        The salt value (key) from the JSON file.
    """
    config = load_salt_config()
    return config["key"]

def hash_with_db_salt(data: str) -> str:
    """
    Hash data using the database salt value and method from config.
    Parameters
    ----------
    data : str
        The data string to be hashed.
    Returns
    -------
    str
        The hashed data combined with the salt using the configured algorithm and truncation.
    """
    config = load_salt_config()
    salt = config["key"]
    method = config["hash_method"].upper()
    trunc_len = int(config.get("truncation_length", 0))

    # Select hash function
    if method == "SHA256":
        hashed = datahash.hash256(data, salt)
    elif method == "MD5":
        hashed = datahash.hashmd5(data, salt)
    else:
        raise ValueError(f"Unsupported hash method: {method}")

    # Truncate if truncation_length is set and less than hash length
    if trunc_len > 0 and trunc_len < len(hashed):
        return hashed[:trunc_len]
    return hashed

def generate_salt_check() -> str:
    """
    Generates the SALT_CHECK value by loading the salt and hashing it.
    """
    salt = load_salt()
    return hash_with_db_salt(salt)
