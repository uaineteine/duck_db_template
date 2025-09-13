# Running the Streamlit DuckDB Template Designer

## Quick Start

To launch the Streamlit app for designing your DuckDB configuration:

```bash
streamlit run design_app.py
```

The app will be available at `http://localhost:8501`

## Features

The Streamlit app provides 3 tabs for designing your DuckDB template:

### 📊 Databases Tab
- Configure database connections from `init_tables/db_list.csv`
- Set database paths, names, and purposes (main/primary/secondary)
- Add/remove databases with the data editor

### 🗂️ Tables Tab  
- Design table schemas from `init_tables/def_tables.csv`
- View existing tables organized by database
- Quick-add functionality for new tables and columns
- Note: ID columns are automatically added as primary keys

### 👁️ Views Tab
- Create and edit SQL views from `init_tables/views.csv`
- View SQL code for existing views
- Quick-add functionality for new views
- Note: Views CSV uses pipe (|) delimiter

## Screenshots

- `databases-tab.png` - Database configuration interface
- `tables-tab.png` - Table schema designer
- `views-tab.png` - Views designer
- `views-tab-with-new-view.png` - Example of adding a new view

## Notes

- All changes are saved directly to the CSV files in `init_tables/`
- The existing DuckDB system continues to work normally
- The sidebar shows configuration status for all components
- Use the help sections in each tab for detailed guidance