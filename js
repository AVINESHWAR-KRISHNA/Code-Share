from sqlalchemy import create_engine, event
import pandas as pd

# Replace 'your_database_uri' with the actual URI for your database
database_uri = 'your_database_uri'
engine = create_engine(database_uri)

# Path to your CSV file
csv_file_path = 'path_to_your_csv_file.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Table name where you want to insert the data
tbl = 'your_table_name'

# Event listener function for fast_executemany
@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True

# Bulk insert operation
df.to_sql(tbl, engine, index=False, if_exists="append", schema="dbo")
