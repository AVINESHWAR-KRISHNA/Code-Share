
from sqlalchemy import create_engine, event
import pandas as pd


SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech'
DATABASE ='Srdial'
DRIVER = 'SQL+Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-Test.csv'


engine = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver=ODBC Driver 17 for SQL Server',fast_executemany=True)

# Path to your CSV file
csv_file_path = FTP

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path,sep=',',low_memory=True)
df = df.astype(str)

# Table name where you want to insert the data
tbl = TABLE_NAME

# Event listener function for fast_executemany
@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True

# Bulk insert operation
df.to_sql(tbl, engine, index=False, if_exists="append", schema="dbo")
