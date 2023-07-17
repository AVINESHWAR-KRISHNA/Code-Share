from sqlalchemy import create_engine, insert
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np

SERVER_NAME = 'DEVCONTWCOR01.r1rcm.tech'
DATABASE = 'Srdial'
DRIVER = 'SQL+Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-Test.csv'
MAX_THREADS = 25
CHUNK_SIZE = 100000

insert_records_failure_flag_counter = 0
rows_inserted = 0
insertion_err = ''

try:
    ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}', fast_executemany=True)
except Exception as e:
    print(f"Unable to connect to server :: {SERVER_NAME} err_msg :: {e}.")


def insert_records(chunk):
    global rows_inserted, insert_records_failure_flag, insertion_err, insert_records_failure_flag_counter

    try:
        cnx = ENGINE.connect()

        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk.fillna('NULL', inplace=True)

        float_columns = chunk.select_dtypes(include='float').columns
        chunk[float_columns] = chunk[float_columns].replace([np.inf, -np.inf], np.nan)
        chunk[float_columns] = chunk[float_columns].astype(pd.Int64Dtype())

        metadata = {
            'type': f'{TABLE_NAME}',
        }
        stmt = insert(metadata)
        cnx.execute(stmt, chunk.to_dict(orient='records'))
        cnx.close()

        rows_inserted += len(chunk)
    except Exception as e:
        insertion_err += str(e)
        insert_records_failure_flag_counter += 1
        print(f"Unable to insert data in table :: {TABLE_NAME}. err_msg :: {insertion_err}")


def create_chunk(df):
    global insertion_err

    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = []

        print(f"Inserting data into table :: {TABLE_NAME}.")

        for chunk in chunks:
            future = executor.submit(insert_records, chunk)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

    print(f"Total number of rows inserted :: {rows_inserted}.")


if __name__ == '__main__':
    matching_file = FTP

    if matching_file:
        df = pd.read_csv(matching_file, sep=',', low_memory=False)
        create_chunk(df)
    else:
        print("No file found. Sys exit.")
        sys.exit(1)
