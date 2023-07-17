from sqlalchemy import create_engine, insert, MetaData, Table, Column, Integer, String, Float
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np

SERVER_NAME = 'DEVCONTWCOR01.r1rcm.tech'
DATABASE = 'Srdial'
DRIVER = 'SQL+Server'
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

def create_dynamic_table(df, table_name):
    metadata = MetaData()
    columns = []
    
    # Analyze the data types of the columns
    for column_name, column_data in df.dtypes.items():
        if column_data == np.int64:
            col = Column(column_name, Integer)
        elif column_data == np.float64:
            col = Column(column_name, Float)
        else:
            col = Column(column_name, String)
        columns.append(col)

    # Create the dynamic table
    table = Table(table_name, metadata, *columns)
    metadata.create_all(ENGINE)
    
    return table

def insert_records(chunk, table):
    global rows_inserted, insert_records_failure_flag, insertion_err, insert_records_failure_flag_counter

    try:
        cnx = ENGINE.connect()

        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk.fillna('NULL', inplace=True)

        float_columns = chunk.select_dtypes(include='float').columns
        chunk[float_columns] = chunk[float_columns].replace([np.inf, -np.inf], np.nan)
        chunk[float_columns] = chunk[float_columns].astype(pd.Int64Dtype())

        ins = insert(table)
        cnx.execute(ins, chunk.to_dict(orient='records'))
        cnx.close()

        rows_inserted += len(chunk)
    except Exception as e:
        insertion_err += str(e)
        insert_records_failure_flag_counter += 1
        print(f"Unable to insert data in table :: {table.name}. err_msg :: {insertion_err}")

def create_chunk(df, table):
    global insertion_err

    chunks = [df[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = []

        print(f"Inserting data into table :: {table.name}.")

        for chunk in chunks:
            future = executor.submit(insert_records, chunk, table)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

    print(f"Total number of rows inserted :: {rows_inserted}.")


if __name__ == '__main__':
    matching_file = FTP

    if matching_file:
        df = pd.read_csv(matching_file, sep=',', low_memory=False)

        # Ask the user for the table name or use a default name
        table_name = input("Enter the table name: ") or 'DynamicTable'

        # Create a dynamic table based on the CSV data
        dynamic_table = create_dynamic_table(df, table_name)

        # Insert the data into the dynamic table
        create_chunk(df, dynamic_table)
    else:
        print("No file found. Sys exit.")
        sys.exit(1)
