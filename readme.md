# DUCK DB TEMPLATE v1.2

Creates multiple partitioned databases based on a csv list supplied by the user. This allows manipulations to be handled and routed through the main driver and possible re-allocation and migration of different components.

#### STATUS

[![Test Launch](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server.yaml)
[![Test Launch - dev](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server_dev.yaml)

### GET STARTED

Modify the `db_list.csv` to outline your proposed multi-database structure and then run the start_db script. This will launch the server and return a connection for you to use. any uncreated directories will be produced automatically.

There are three types of dbs you can use in this list:

* The main (only one) will be the driver DB for your host's session. If you should like to sotre information on this DB, that is up to the user.
* The primary dbs have write and read access to a new pathed location.
* The secondary dbs are a read-only connection to a path, this intended for additional instances or hosts of the DB.

I would recommend keeping this template as a dedicated sub-folder and importing the start_db script from another module.

### WHAT IT WILL NOT DO

It will not create your initial data or tables. It is planned it will generate your tables and variable types from a csv.

