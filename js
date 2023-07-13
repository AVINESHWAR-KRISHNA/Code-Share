
import dask.dataframe as dd
import dask.distributed
from sqlalchemy import create_engine, text, bindparam
import numpy as np
import pandas as pd
import os

# Update the database connection details
SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech'
DATABASE ='Srdial'
DRIVER = 'SQL+Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = r'C:\Users\IN10011418\OneDrive - R1\Desktop\MFS-TestData.csv'
CHUNK_SIZE = 10000
MAX_THREADS = 25

# Create a Dask cluster
cluster = dask.distributed.LocalCluster()
client = dask.distributed.Client(cluster)

# Update the database connection details
conn_str = f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}'
os.environ['DATABASE_URL'] = conn_str

# Create the SQLAlchemy engine
ENGINE = create_engine(os.environ['DATABASE_URL'], fast_executemany=True)

# Define the insert_records function
def insert_records(chunk):
    try:
        global rows_inserted, insert_records_failure_flag, insertion_err, insert_records_failure_flag_counter

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


# Replace create_chunk with the existing code, but updating the ThreadPoolExecutor with dask.distributed
def create_chunk(df):
    global insertion_err

    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    futures = []

    for chunk in chunks:
        future = client.submit(insert_records, chunk)
        futures.append(future)

    dask.distributed.wait(futures)

    print(f"Data inserted successfully into table :: {TABLE_NAME}.")
    print(f"Total number of rows inserted :: {rows_inserted}.")


if __name__ == '__main__':

    matching_file = FTP

    if matching_file:
        df = dd.read_csv(matching_file, sep=',')
        dd_progress = dd.compute(df)
        dd_progress.visualize()

        df = dd_progress[0].compute()

        create_chunk(df)
    else:
        print("No file found. Sys exit.")

RuntimeError:
        An attempt has been made to start a new process before the
        current process has finished its bootstrapping phase.

        This probably means that you are not using fork to start your
        child processes and you have forgotten to use the proper idiom
        in the main module:

            if __name__ == '__main__':
                freeze_support()
                ...

        The "freeze_support()" line can be omitted if the program
        is not going to be frozen to produce an executable.
