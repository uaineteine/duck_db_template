@echo off

REM Path to the db_list.csv file
set DB_LIST=init_tables\db_list.csv

REM Output SQL file in the 'temp' folder
set SQL_OUTPUT=temp\attach_databases.sql

REM Create the 'temp' folder if it doesn't exist
if not exist "temp" mkdir temp

REM Clear the output file (overwrite if it exists)
echo. > %SQL_OUTPUT%

REM Generate SQL statements and append to the output file
for /f "skip=1 tokens=1,2 delims=|" %%a in (%DB_LIST%) do (
    echo ATTACH '%%~a' AS %%b (READ_ONLY^); >> %SQL_OUTPUT%
)

REM Start DuckDB and execute the generated SQL file, then keep the CLI open
duckdb -ui -init %SQL_OUTPUT%