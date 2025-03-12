import os
import duckdb
import graphviz
from uainepydat import fileio
from uainepydat import duckfunc
import parse_db_list


def generate_svg(def_tables_path):
    # Read the CSV file using pandas
    dblist = os.path.join(def_tables_path, "db_list.csv")
    driver_name, all_names, primary_dbs, secondary_dbs = parse_db_list.parselist(dblist)

    # Connect to the driver database
    fileio.create_filepath_dirs(driver_name)
    con = duckdb.connect(driver_name)

    # Attach primary databases
    for i, row in primary_dbs.iterrows():
        attach_db(con, row['PATH'], row['DB_NAME'])

    # Attach secondary databases as read only
    for i, row in secondary_dbs.iterrows():
        attach_db(con, row['PATH'], row['DB_NAME'], readonly=True)

    # Create a graph
    dot = graphviz.Digraph(comment='Database Tables')

    # Retrieve tables from each attached database
    attached_dbs = duckfunc.get_attached_dbs(con)
    for db in attached_dbs:
        con.execute(f"USE {db['name']}")
        tables = con.execute("SHOW TABLES").fetchall()
        for table in tables:
            dot.node(table[0], f"{table[0]}\n({db['name']})")

    # Save the graph as an SVG file
    dot.render('database_tables', format='svg')


def attach_db(con, path, name, readonly=False):
    fileio.create_filepath_dirs(path)
    ex_string = f"ATTACH DATABASE '{path}' AS {name}"
    if readonly:
        ex_string += " (READ_ONLY)"
    con.execute(ex_string)


if __name__ == "__main__":
    generate_svg("init_tables") 