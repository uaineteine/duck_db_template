# DUCK DB TEMPLATE

Creates multiple partitioned databases based on input list(s) (csv) supplied by the user. This allows manipulations to be handled and routed through the main driver and possible re-allocation and migration of different components.

![py version](https://img.shields.io/badge/python-3.9+-blue) ![Version 1.6.1](https://img.shields.io/badge/version-1.6.1-brightgreen)

#### STATUS 

[![Lint Check](https://github.com/uaineteine/duck_db_template/actions/workflows/lint_check.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/lint_check.yaml) [![Pycheck](https://github.com/uaineteine/duck_db_template/actions/workflows/pycheck.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/pycheck.yaml) [![Test Launch](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server.yaml) [![Dup Checks](https://github.com/uaineteine/duck_db_template/actions/workflows/duplication_check.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/duplication_check.yaml)

DEVELOPMENT:

[![Lint Check - dev](https://github.com/uaineteine/duck_db_template/actions/workflows/lint_check_dev.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/lint_check_dev.yaml) [![Pycheck - dev](https://github.com/uaineteine/duck_db_template/actions/workflows/pycheck.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/lint_check_dev.yaml) [![Test Launch - dev](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server_dev.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server_dev.yaml) [![Dup Checks - dev](https://github.com/uaineteine/duck_db_template/actions/workflows/duplication_check_dev.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/duplication_check_dev.yaml)

### GET STARTED

* Python 3.9 or above is required

Install package dependencies found in `requirements.txt`. This can be installed via

```
pip install -r requirements.txt
```

### USAGE

Modify the `db_list.csv` to outline your proposed multi-database structure and then run the start_db script. This will launch the server and return a connection for you to use. any uncreated directories will be produced automatically.

```bash
python example_start.py
```

**Primary Key Column:**
All tables will now automatically include an `ID` column as `INT64 PRIMARY KEY` (unless already present in your schema). You do not need to add this manually to your `def_tables.csv`—it will be prepended to every table definition on creation.

**Foreign Key Relationships (LINKS_TO):**
You can now specify table relationships using the `LINKS_TO` column in your `def_tables.csv`. When a table links to another table, foreign key columns and **constraints** will be automatically created:
- Format: `LINKS_TO` column can contain a single table name or comma-separated list
- Example: If `PROFILES` links to `USERS`, a `USERS_ID` column (INT64) will be automatically added to the `PROFILES` table **with a foreign key constraint**
- Multiple links: `ORDER_ITEMS` linking to `"ORDERS,USERS"` will get both `ORDERS_ID` and `USERS_ID` columns with constraints
- **Referential Integrity**: The foreign key constraints enforce data integrity by preventing invalid references and maintaining consistency across related tables
- **Cross-Database**: Foreign key constraints only work within the same database; cross-database references will create columns without constraints

Extend and only EXTEND the `def_tables.csv` list with your proposed schema and empty tables will be generated accordingly on DB start.

There are three types of dbs you can use in this list:

* The main (only one) will be the driver DB for your host's session. If you should like to sotre information on this DB, that is up to the user.
* The primary dbs have write and read access to a new pathed location.
* The secondary dbs are a read-only connection to a path, this intended for additional instances or hosts of the DB.

I would recommend keeping this template as a dedicated sub-folder and importing the start_db script from another module.

### STREAMLIT UI DESIGNER

The system now includes a user-friendly Streamlit web interface for designing and configuring your DuckDB template:

```bash
streamlit run design_app.py
```

The Streamlit app provides 3 intuitive tabs:
- **📊 Databases**: Configure database connections and purposes
- **🗂️ Tables**: Design table schemas and columns  
- **👁️ Views**: Create and edit SQL views

Access the app at `http://localhost:8501` after running. See `STREAMLIT_USAGE.md` for detailed instructions.

### UI FEATURE

The system includes a basic UI interface that can be launched using the `launch_ui.bat` script. This script will:

1. Read the database configuration from `db_list.csv`
2. Generate SQL statements to attach all databases
3. Launch DuckDB with the UI interface and execute the generated SQL statements

To use the UI feature, simply run the `launch_ui.bat` script from your command line. Currently this requires an installation of duckdbs CLI program to launch the UI component.


### SECURITY FEATURES

The system includes a salt check mechanism to verify database integrity, now using a flexible JSON configuration:

* The salt configuration is stored in `db_salt.json` (replacing the old `db_salt.txt` format)
* The JSON file must contain the following fields:

	- `hash_method`: The hash algorithm to use (e.g., `SHA256` or `MD5`)
	- `key`: The salt value (a random string, e.g., `A7DF3866B66E1BEE8E564DFFC4D30994`)
	- `truncation_length`: (integer) The number of characters to keep from the hash (e.g., `64` for full SHA256, or a shorter value for truncated hashes)

Example `db_salt.json`:

```json
{
	"hash_method": "SHA256",
	"key": "A7DF3866B66E1BEE8E564DFFC4D30994",
	"truncation_length": 64
}
```

* On first database launch, a hash of the salt is stored in the META table
* On subsequent launches, the system verifies that the stored hash matches the current calculation using the configured method and truncation
* If a mismatch is detected, the system will raise an error to prevent potential integrity issues

This feature helps ensure that your database configuration hasn't been tampered with between sessions, and allows you to easily change the hash method or salt policy by editing `db_salt.json`.

### VIEWS FROM TEMPLATE
* Setup of views can be made from the `views.csv` list. Update this to create new views in the DB on launch.
* This template comes with a view of the number of rows per table in the whole DB. 

```sql
SELECT * FROM db_num_tables_view;
```

### WHAT IT WILL NOT DO

Function without the default tables made in the `def_tables.csv`. Please only append to this or use a 2nd csv.

#### What we want it to do

~~Create foreign keys between tables automatically.~~ ✅ **COMPLETED**: Foreign key columns and **constraints** are now automatically created based on the `LINKS_TO` specification in `def_tables.csv`. Referential integrity is enforced at the database level.

### Dumping Feature

The dump_db module when executed will ask the user for an output directory and a CSV or parquet format for saving the entire loaded DB into data files of each table.

```bash
python dump_db.py
```

### Database Structure Visualization

You can generate an interactive HTML diagram of your current database structure (databases, tables, and user-created views) using the `db_network_viz.py` script. This will output a file named `db_network.html` in your project directory.

To view the diagram, open `db_network.html` in your browser:

```bash
python db_network_viz.py
```

![Database Diagram](db_network.html)

