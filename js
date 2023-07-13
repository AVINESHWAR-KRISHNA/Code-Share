import sys
import pyodbc
import pandas as pd
import numpy as np
import concurrent.futures
import gc
gc.enable()

SERVER_NAME = 'DEVCONTWCOR01.r1rcm.tech'
DATABASE = 'Srdial'
DRIVER = '{SQL Server}'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = r'C:\Users\IN10011418\OneDrive - R1\Desktop\MFS-TestData.csv'
MAX_THREADS = 25
CHUNK_SIZE = 1000

insert_records_failure_flag_counter = 0
rows_inserted = 0
insertion_err = ''
insert_records_failure_flag = True

try:
    cnxn = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER_NAME};DATABASE={DATABASE};Trusted_Connection=yes')
    cursor = cnxn.cursor()

except Exception as e:
    print(f"Unable to connect to server :: {SERVER_NAME}. Error message: {e}.")
    sys.exit(1)

def insert_records(chunk):
    try:
        global rows_inserted, insert_records_failure_flag, insertion_err, insert_records_failure_flag_counter

        # Disable constraints and triggers
        cursor.execute('ALTER TABLE {TABLE_NAME} DISABLE TRIGGER ALL')
        cursor.execute('ALTER TABLE {TABLE_NAME} NOCHECK CONSTRAINT ALL')

        # Prepare the data for bulk insert
        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk.fillna('NULL', inplace=True)

        float_columns = chunk.select_dtypes(include='float').columns
        chunk[float_columns] = chunk[float_columns].replace([np.inf, -np.inf], np.nan)
        chunk[float_columns] = chunk[float_columns].astype(pd.Int64Dtype())

        # Create a temporary table for bulk insert
        temp_table_name = f'Temp_{TABLE_NAME}'
        cursor.execute(f'CREATE TABLE {temp_table_name} ({", ".join(chunk.columns)})')

        # Insert data into the temporary table
        chunk_values = ", ".join(["?" for _ in range(len(chunk.columns))])
        insert_query = f'BULK INSERT {temp_table_name} FROM \'{FTP}\' WITH (FIELDTERMINATOR=\',\', ROWTERMINATOR=\'\\n\', BATCHSIZE={CHUNK_SIZE})'
        cursor.execute(insert_query)

        # Move data from temporary table to final table
        cursor.execute(f'INSERT INTO {TABLE_NAME} SELECT * FROM {temp_table_name}')

        # Delete the temporary table
        cursor.execute(f'DROP TABLE {temp_table_name}')

        # Enable constraints and triggers
        cursor.execute('ALTER TABLE {TABLE_NAME} CHECK CONSTRAINT ALL')
        cursor.execute('ALTER TABLE {TABLE_NAME} ENABLE TRIGGER ALL')

        rows_inserted += len(chunk)

    except Exception as e:
        print(e)
        insertion_err += str(e)
        insert_records_failure_flag_counter += 1
        print(f"Unable to insert data into table {TABLE_NAME}. Error message: {insertion_err}")

def create_chunk(df):
    global insertion_err

    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = []
        for chunk in chunks:
            future = executor.submit(insert_records, chunk)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

    print(f"Data inserted successfully into table {TABLE_NAME}.")
    print(f"Total number of rows inserted: {rows_inserted}.")

if __name__ == '__main__':
    matching_file = FTP

    if matching_file:
        df = pd.read_csv(matching_file, sep=',')
        create_chunk(df)
    else:
        print("No file found. Exiting the program.")
        sys.exit(1)

# Close the connection
cursor.close()
cnxn.close()
