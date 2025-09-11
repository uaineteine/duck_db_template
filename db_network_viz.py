"""
Script to visualize the live DuckDB network (databases and views) using pyvis.
"""
import conn
from uainepydat import duckfunc
from pyvis.network import Network
from launcher import parse_db_list

# Connect to the database system
con = conn.get_connection()

# Get all attached databases
attached_dbs = duckfunc.get_attached_dbs(con).df()

# Create a pyvis network
net = Network(height='600px', width='100%', bgcolor='#222222', font_color='white', directed=True)

# Add nodes for each attached database
for idx, row in attached_dbs.iterrows():
    db_name = row['DB_NAME']
    label = f"{db_name}\n(DB)"
    color = '#ffcc00' if db_name == 'main' else '#00ccff'
    net.add_node(db_name, label=label, color=color, shape='database')

# Add views as nodes and connect to their database
try:
    for idx, row in attached_dbs.iterrows():
        db_name = row['DB_NAME']
        # Switch to the database and get its views
        # For DuckDB, views are global, but you can prefix with db_name if needed
        views_df = con.sql(f"SELECT view_name AS VIEW_NAME FROM {db_name}.duckdb_views()" ).df()
        for _, vrow in views_df.iterrows():
            view_name = vrow['VIEW_NAME']
            vlabel = f"{view_name}\n(View)"
            net.add_node(view_name, label=vlabel, color='#66ff66', shape='ellipse')
            net.add_edge(db_name, view_name, label='has view')
except Exception as e:
    print(f"Error reading views: {e}")

# Optionally, parse view SQL to find dependencies (not implemented here)

# Save and show the network
net.show('db_network.html', notebook=False)
print('Database network visualization saved as db_network.html')

# Close the connection
conn.close_connection(con)
