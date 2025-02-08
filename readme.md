# DUCK DB TEMPLATE v1.2.1

Creates multiple partitioned databases based on input list(s) (csv) supplied by the user. This allows manipulations to be handled and routed through the main driver and possible re-allocation and migration of different components.

#### STATUS

[![Test Launch](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server.yaml)
[![Test Launch - dev](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server_dev.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server_dev.yaml)

### GET STARTED

Modify the `db_list.csv` to outline your proposed multi-database structure and then run the start_db script. This will launch the server and return a connection for you to use. any uncreated directories will be produced automatically.

Extend and only EXTEND the `def_tables.csv` list with your porposed schema and empty tables will be generated accordingly on DB start.

There are three types of dbs you can use in this list:

* The main (only one) will be the driver DB for your host's session. If you should like to sotre information on this DB, that is up to the user.
* The primary dbs have write and read access to a new pathed location.
* The secondary dbs are a read-only connection to a path, this intended for additional instances or hosts of the DB.

I would recommend keeping this template as a dedicated sub-folder and importing the start_db script from another module.

### WHAT IT WILL NOT DO

Function without the default tables made in the `def_tables.csv`. Please only append to this or use a 2nd csv.

#### What we want it to do

Automaticlaly produce an svg chart of data items
