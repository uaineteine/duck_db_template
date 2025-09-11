"""
Script to visualize the database list as a network using pyvis.
"""
import os
import pandas as pd
from pyvis.network import Network
from launcher import parse_db_list

# Path to the db_list.csv file
DB_LIST_PATH = os.path.join(os.path.dirname(__file__), 'init_tables', 'db_list.csv')

# Read the database list
# The CSV uses '|' as a separator

df = pd.read_csv(DB_LIST_PATH, sep='|')
df = parse_db_list.clean_db_list(df)

# Create a pyvis network
net = Network(height='600px', width='100%', bgcolor='#222222', font_color='white', directed=True)

# Add nodes for each database
for idx, row in df.iterrows():
    db_name = row['DB_NAME']
    purpose = row['PURPOSE']
    label = f"{db_name}\n({purpose})"
    color = '#ffcc00' if purpose == 'main' else '#00ccff'
    net.add_node(db_name, label=label, color=color)

# Add edges: meta (main) points to all others
meta_db = df[df['PURPOSE'] == 'main']['DB_NAME'].values
if len(meta_db) == 1:
    meta = meta_db[0]
    for db_name in df['DB_NAME']:
        if db_name != meta:
            net.add_edge(meta, db_name)

# Save and show the network
net.show('db_network.html', notebook=False)
print('Database network visualization saved as db_network.html')
