import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from sqlalchemy import create_engine, text, bindparam
import numpy as np
import pandas as pd
import os

# Update the database connection details
SERVER_NAME = 'DEVCONTWCOR01.r1rcm.tech'
DATABASE = 'Srdial'
DRIVER = 'SQL+Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = r'C:\Users\IN10011418\OneDrive - R1\Desktop\MFS-TestData.csv'
CHUNK_SIZE = 10000
MAX_THREADS = 25

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


if __name__ == '__main__':
    # Create a Dask cluster
    cluster = LocalCluster()
    client = Client(cluster)

    # Update the database connection details
    conn_str = f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}'
    os.environ['DATABASE_URL'] = conn_str

    # Create the SQLAlchemy engine
    ENGINE = create_engine(os.environ['DATABASE_URL'], fast_executemany=True)

    # Load the CSV file as a Dask DataFrame
    df = dd.read_csv(FTP)

    # Convert Dask DataFrame to Pandas DataFrame
    df = df.compute()

    # Split the DataFrame into chunks
    chunks = [df[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    # Submit each chunk for parallel processing
    futures = client.map(insert_records, chunks)
    client.gather(futures)

    print(f"Data inserted successfully into table :: {TABLE_NAME}.")
    print(f"Total number of rows inserted :: {rows_inserted}.")
