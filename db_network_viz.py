"""
Script to visualize the live DuckDB network (databases and views) using pyvis.
"""
import conn
from uainepydat import duckfunc
from pyvis.network import Network

# Connect to the database system
con = conn.get_connection()

# Get all attached databases
attached_dbs = duckfunc.get_attached_dbs(con).df()

# Create a pyvis network with white background and black font/edges
net = Network(height='600px', width='100%', bgcolor='#ffffff', font_color='black', directed=True)

# Increase node separation for better visibility
net.barnes_hut(gravity=-20000, central_gravity=0.1, spring_length=300, spring_strength=0.01, damping=0.09, overlap=0.5)
net.set_options("""
{
    "layout": { "improvedLayout": true },
    "physics": { "barnesHut": { "springLength": 300, "avoidOverlap": 1 } }
}
""")

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
        # Get all tables in this database using the new tableP_df structure
        tables_df = duckfunc.get_inventory(con).df()
        db_tables = tables_df[tables_df['database_name'] == db_name]
        for _, trow in db_tables.iterrows():
            table_name = trow['table_name']
            tlabel = f"{table_name}\n(Table)"
            net.add_node(f"{db_name}.{table_name}", label=tlabel, color='#3399ff', shape='box')
            net.add_edge(db_name, f"{db_name}.{table_name}", label='has table')

        # Get only user-created views (schema_name = 'main')
        views_df = con.sql("SELECT view_name AS VIEW_NAME FROM main.duckdb_views() WHERE schema_name = 'main'" ).df()
        for _, vrow in views_df.iterrows():
            view_name = vrow['VIEW_NAME']
            vlabel = f"{view_name}\n(View)"
            net.add_node(f"{db_name}.{view_name}", label=vlabel, color='#66ff66', shape='ellipse')
            net.add_edge(db_name, f"{db_name}.{view_name}", label='has view')
except Exception as e:
    print(f"Error reading views: {e}")

# Optionally, parse view SQL to find dependencies (not implemented here)


# Save as a pure HTML file (do not open, just write the HTML string)
html_str = net.generate_html()
with open('db_network.html', 'w', encoding='utf-8') as f:
    f.write(html_str)
print('Database network visualization saved as db_network.html')

# Close the connection
conn.close_connection(con)
