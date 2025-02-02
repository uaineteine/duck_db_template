# DUCK DB TEMPLATE

Creates multiple partitioned databases based on a csv list supplied by the user. This allows manipulations to be handled and routed through the main driver and possible re-allocation and migration of different components.

#### STATUS

[![Run DuckDB Script](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server.yaml/badge.svg?branch=develop)](https://github.com/uaineteine/duck_db_template/actions/workflows/start_server.yaml)

### GET STARTED

Modify the `db_list.csv` to outline your proposed multi-database structure and then run the start_db script. This will launch the server and return a connection for you to use.

### WHAT IT WILL NOT DO

It will not make folders and directories needed, which is a shame as adding new databases would be very easy. Hence this is a planned feature.

