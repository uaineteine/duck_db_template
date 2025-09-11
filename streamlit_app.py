"""
Streamlit App for DuckDB Template Configuration

This app provides a user-friendly interface to design and edit the 3 starting tables:
1. Database configuration (db_list.csv)
2. Table schema definitions (def_tables.csv) 
3. View definitions (views.csv)
"""

import streamlit as st
import pandas as pd
import os
from io import StringIO

# Set page configuration
st.set_page_config(
    page_title="DuckDB Template Designer",
    page_icon="🦆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Main title
st.title("🦆 DuckDB Template Designer")
st.markdown("Design and configure your DuckDB databases, tables, and views")

# Define paths
INIT_TABLES_DIR = "init_tables"
DB_LIST_PATH = os.path.join(INIT_TABLES_DIR, "db_list.csv")
DEF_TABLES_PATH = os.path.join(INIT_TABLES_DIR, "def_tables.csv")
VIEWS_PATH = os.path.join(INIT_TABLES_DIR, "views.csv")

def load_csv_file(file_path):
    """Load CSV file with error handling"""
    try:
        if os.path.exists(file_path):
            # Views CSV uses pipe delimiter, others use comma
            if "views.csv" in file_path:
                return pd.read_csv(file_path, delimiter='|')
            else:
                return pd.read_csv(file_path)
        else:
            st.error(f"File not found: {file_path}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading {file_path}: {str(e)}")
        return pd.DataFrame()

def save_csv_file(df, file_path):
    """Save DataFrame to CSV file"""
    try:
        # Views CSV uses pipe delimiter, others use comma
        if "views.csv" in file_path:
            df.to_csv(file_path, index=False, sep='|')
        else:
            df.to_csv(file_path, index=False)
        st.success(f"✅ Saved {os.path.basename(file_path)} successfully!")
        return True
    except Exception as e:
        st.error(f"Error saving {file_path}: {str(e)}")
        return False

def create_tabs():
    """Create the main tab interface"""
    tab1, tab2, tab3 = st.tabs(["📊 Databases", "🗂️ Tables", "👁️ Views"])
    
    with tab1:
        database_tab()
    
    with tab2:
        tables_tab()
    
    with tab3:
        views_tab()

def database_tab():
    """Database configuration tab"""
    st.header("Database Configuration")
    st.markdown("Configure the databases that will be created and attached.")
    
    # Load current database list
    df_db = load_csv_file(DB_LIST_PATH)
    
    if not df_db.empty:
        st.subheader("Current Database Configuration")
        
        # Display current configuration
        st.dataframe(df_db, use_container_width=True)
        
        # Edit existing configuration
        st.subheader("Edit Configuration")
        edited_df = st.data_editor(
            df_db,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "PATH": st.column_config.TextColumn(
                    "Database Path",
                    help="Path where the database file will be stored"
                ),
                "DB_NAME": st.column_config.TextColumn(
                    "Database Name",
                    help="Alias name for the database connection"
                ),
                "PURPOSE": st.column_config.SelectboxColumn(
                    "Purpose",
                    help="Database purpose: main (driver), primary (read/write), secondary (read-only)",
                    options=["main", "primary", "secondary"]
                )
            }
        )
        
        # Save button
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("💾 Save Database Config", type="primary"):
                if save_csv_file(edited_df, DB_LIST_PATH):
                    st.rerun()
    else:
        st.warning("No database configuration found. Creating a default template.")
        # Create default template
        default_db = pd.DataFrame({
            "PATH": ["DBDAT/main.duckdb", "DBDAT/users.duckdb"],
            "DB_NAME": ["meta", "users"],
            "PURPOSE": ["main", "primary"]
        })
        st.dataframe(default_db, use_container_width=True)
        
        if st.button("Create Default Database Config"):
            save_csv_file(default_db, DB_LIST_PATH)
            st.rerun()
    
    # Information section
    with st.expander("ℹ️ Database Configuration Help"):
        st.markdown("""
        **Database Purpose Types:**
        - **main**: The primary driver database (only one allowed)
        - **primary**: Read/write databases for your application data  
        - **secondary**: Read-only databases (useful for shared/reference data)
        
        **Path Examples:**
        - `DBDAT/main.duckdb` - Main driver database
        - `DBDAT/users.duckdb` - User data database
        - `DBDAT/analytics.duckdb` - Analytics database
        """)

def tables_tab():
    """Table schema definition tab"""
    st.header("Table Schema Designer")
    st.markdown("Define the structure of tables that will be created in your databases.")
    
    # Load current table definitions
    df_tables = load_csv_file(DEF_TABLES_PATH)
    
    if not df_tables.empty:
        st.subheader("Current Table Definitions")
        
        # Display current tables grouped by database
        databases = df_tables['DBNAME'].unique()
        for db in databases:
            with st.expander(f"🗃️ Database: {db}"):
                db_tables = df_tables[df_tables['DBNAME'] == db]
                tables = db_tables['TABLENAME'].unique()
                
                for table in tables:
                    st.write(f"**Table: {table}**")
                    table_cols = db_tables[db_tables['TABLENAME'] == table][['VARNAME', 'TYPE']]
                    st.dataframe(table_cols, use_container_width=True, hide_index=True)
        
        # Edit existing configuration
        st.subheader("Edit Table Definitions")
        edited_df = st.data_editor(
            df_tables,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "DBNAME": st.column_config.TextColumn(
                    "Database Name",
                    help="Name of the database (must match a DB_NAME from database config)"
                ),
                "TABLENAME": st.column_config.TextColumn(
                    "Table Name",
                    help="Name of the table to create"
                ),
                "VARNAME": st.column_config.TextColumn(
                    "Column Name",
                    help="Name of the column"
                ),
                "TYPE": st.column_config.TextColumn(
                    "Data Type",
                    help="DuckDB data type (e.g., VARCHAR, INT64, BOOLEAN, TIMESTAMP)"
                )
            }
        )
        
        # Save button
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("💾 Save Table Definitions", type="primary"):
                if save_csv_file(edited_df, DEF_TABLES_PATH):
                    st.rerun()
    else:
        st.warning("No table definitions found.")
        # Create default template
        default_tables = pd.DataFrame({
            "DBNAME": ["main", "main"],
            "TABLENAME": ["META", "META"],
            "VARNAME": ["START_TIME", "DB_VERSION"],
            "TYPE": ["VARCHAR", "VARCHAR"]
        })
        st.dataframe(default_tables, use_container_width=True)
        
        if st.button("Create Default Table Definitions"):
            save_csv_file(default_tables, DEF_TABLES_PATH)
            st.rerun()
    
    # Quick add section
    st.subheader("➕ Quick Add Table")
    with st.form("add_table_form"):
        col1, col2 = st.columns(2)
        with col1:
            new_db = st.text_input("Database Name")
            new_table = st.text_input("Table Name")
        with col2:
            new_column = st.text_input("Column Name")
            new_type = st.selectbox("Data Type", 
                ["VARCHAR", "INT64", "BOOLEAN", "TIMESTAMP", "DOUBLE", "DATE", "BLOB"])
        
        if st.form_submit_button("Add Column"):
            if new_db and new_table and new_column and new_type:
                new_row = pd.DataFrame({
                    "DBNAME": [new_db],
                    "TABLENAME": [new_table], 
                    "VARNAME": [new_column],
                    "TYPE": [new_type]
                })
                
                current_df = load_csv_file(DEF_TABLES_PATH)
                if current_df.empty:
                    updated_df = new_row
                else:
                    updated_df = pd.concat([current_df, new_row], ignore_index=True)
                
                if save_csv_file(updated_df, DEF_TABLES_PATH):
                    st.rerun()
            else:
                st.error("Please fill in all fields")
    
    # Information section
    with st.expander("ℹ️ Table Definition Help"):
        st.markdown("""
        **Important Notes:**
        - All tables automatically get an `ID` column as `INT64 PRIMARY KEY`
        - You don't need to manually add ID columns
        - Database names must match those defined in the Database tab
        
        **Common DuckDB Data Types:**
        - `VARCHAR` - Variable length text
        - `INT64` - 64-bit integer
        - `DOUBLE` - Double precision float
        - `BOOLEAN` - True/false values
        - `TIMESTAMP` - Date and time
        - `DATE` - Date only
        - `BLOB` - Binary data
        """)

def views_tab():
    """Views definition tab"""
    st.header("Views Designer")
    st.markdown("Create custom SQL views for your database.")
    
    # Load current views
    df_views = load_csv_file(VIEWS_PATH)
    
    if not df_views.empty:
        st.subheader("Current Views")
        
        # Display views with their SQL
        for idx, row in df_views.iterrows():
            with st.expander(f"👁️ View: {row['VIEW_NAME']}"):
                st.code(row['SQL'], language='sql')
        
        # Edit existing views
        st.subheader("Edit Views")
        edited_df = st.data_editor(
            df_views,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "VIEW_NAME": st.column_config.TextColumn(
                    "View Name",
                    help="Name of the view to create"
                ),
                "SQL": st.column_config.TextColumn(
                    "SQL Statement",
                    help="SQL query that defines the view"
                )
            }
        )
        
        # Save button
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("💾 Save Views", type="primary"):
                if save_csv_file(edited_df, VIEWS_PATH):
                    st.rerun()
    else:
        st.warning("No views found.")
        # Create default template
        default_views = pd.DataFrame({
            "VIEW_NAME": ["db_num_tables_view"],
            "SQL": ["SELECT table_name, COUNT(*) OVER () AS row_count FROM information_schema.tables;"]
        })
        st.dataframe(default_views, use_container_width=True)
        
        if st.button("Create Default Views"):
            save_csv_file(default_views, VIEWS_PATH)
            st.rerun()
    
    # Quick add view section
    st.subheader("➕ Add New View")
    with st.form("add_view_form"):
        new_view_name = st.text_input("View Name")
        new_sql = st.text_area("SQL Query", height=100, 
                               placeholder="SELECT * FROM your_table WHERE condition = 'value';")
        
        if st.form_submit_button("Add View"):
            if new_view_name and new_sql:
                new_row = pd.DataFrame({
                    "VIEW_NAME": [new_view_name],
                    "SQL": [new_sql]
                })
                
                current_df = load_csv_file(VIEWS_PATH)
                if current_df.empty:
                    updated_df = new_row
                else:
                    updated_df = pd.concat([current_df, new_row], ignore_index=True)
                
                if save_csv_file(updated_df, VIEWS_PATH):
                    st.rerun()
            else:
                st.error("Please provide both view name and SQL query")
    
    # Information section
    with st.expander("ℹ️ Views Help"):
        st.markdown("""
        **About Views:**
        - Views are virtual tables based on SQL queries
        - They are created automatically when the database starts
        - Use the pipe symbol `|` as delimiter in the CSV file
        
        **Example SQL Queries:**
        ```sql
        -- Count rows in all tables
        SELECT table_name, COUNT(*) OVER () AS row_count 
        FROM information_schema.tables;
        
        -- Summary view
        SELECT database_name, COUNT(*) as table_count 
        FROM information_schema.tables 
        GROUP BY database_name;
        ```
        """)

def main():
    """Main application function"""
    # Sidebar
    with st.sidebar:
        st.header("🦆 DuckDB Template")
        st.markdown("Use this app to configure your DuckDB template system.")
        
        # Status indicators
        st.subheader("Configuration Status")
        
        # Check file existence
        files_status = {
            "Databases": os.path.exists(DB_LIST_PATH),
            "Tables": os.path.exists(DEF_TABLES_PATH),
            "Views": os.path.exists(VIEWS_PATH)
        }
        
        for name, exists in files_status.items():
            if exists:
                st.success(f"✅ {name} configured")
            else:
                st.warning(f"⚠️ {name} not configured")
        
        st.markdown("---")
        
        # Quick actions
        st.subheader("Quick Actions")
        if st.button("🔄 Refresh All Data"):
            st.rerun()
        
        if st.button("📁 Create Missing Directories"):
            os.makedirs(INIT_TABLES_DIR, exist_ok=True)
            st.success("Directories created!")
    
    # Create the main tab interface
    create_tabs()
    
    # Footer
    st.markdown("---")
    st.markdown("**DuckDB Template Designer** - Configure your databases, tables, and views with ease! 🦆")

if __name__ == "__main__":
    main()