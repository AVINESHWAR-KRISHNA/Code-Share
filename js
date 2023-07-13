import dask.dataframe as dd
import numpy as np
import pandas as pd
import os
from sqlalchemy import create_engine, text, bindparam
from dask.distributed import Client
from dask.diagnostics import ProgressBar

# Update the database connection details
SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech'
DATABASE ='Srdial'
DRIVER = 'SQL+Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = r'C:\Users\IN10011418\OneDrive - R1\Desktop\MFS-TestData.csv'
CHUNK_SIZE = 10000

# Create a Dask client
client = Client()

# Update the database connection details
conn_str = f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}'
os.environ['DATABASE_URL'] = conn_str

# Create the SQLAlchemy engine
ENGINE = create_engine(os.environ['DATABASE_URL'], fast_executemany=True)

# Define the insert_records function
def insert_records(chunk):
    try:
        global rows_inserted, insertion_err, insert_records_failure_flag_counter

        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk.fillna('NULL', inplace=True)

        float_columns = chunk.select_dtypes(include='float').columns
        chunk[float_columns] = chunk[float_columns].replace([np.inf, -np.inf], np.nan)
        chunk[float_columns] = chunk[float_columns].astype(pd.Int64Dtype())

        insert_query = f"INSERT INTO {TABLE_NAME} ({', '.join(chunk.columns)}) VALUES ({', '.join([':' + col for col in chunk.columns])})"

        # Use SQLAlchemy to execute the insert query
        with ENGINE.connect() as cnx:
            stmt = text(insert_query)
            stmt = stmt.bindparams(*[bindparam(col) for col in chunk.columns])
            cnx.execute(stmt, chunk.to_dict(orient='records'))

        rows_inserted += len(chunk)

    except Exception as e:
        print(e)
        insertion_err += str(e)
        insert_records_failure_flag_counter += 1

        print(f"Unable to insert data in table :: {TABLE_NAME}. err_msg :: {insertion_err}")


# Replace create_chunk with the existing code, but updating the for loop
def create_chunk(df):
    global insertion_err

    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    futures = []
    for chunk in chunks:
        future = client.submit(insert_records, chunk)
        futures.append(future)

    # No need to wait for futures

    print(f"Data inserted successfully into table :: {TABLE_NAME}.")
    print(f"Total number of rows inserted :: {rows_inserted}.")


if __name__ == '__main__':
    matching_file = FTP

    if matching_file:
        df = dd.read_csv(matching_file, sep=',')
        dd_progress = dd.compute(df)
        dd_progress.visualize()

        df = dd_progress[0].compute()

        rows_inserted = 0
        insertion_err = ""
        insert_records_failure_flag_counter = 0

        with ProgressBar():
            create_chunk(df)
    else:
        print("No file found. Sys exit.")
