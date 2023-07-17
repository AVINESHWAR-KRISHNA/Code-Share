
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine,text, bindparam,MetaData,Table,insert
import concurrent.futures
import gc
gc.enable()


SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech'
DATABASE ='Srdial'
DRIVER = 'SQL+Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-Test.csv'
MAX_THREADS = 25
CHUNK_SIZE = 100000


insert_records_failure_flag_counter = 0
rows_inserted = 0
insertion_err = ''
insert_records_failure_flag = True

try:
    ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)

except Exception as e:

    print(f"Unable to connect to server :: {SERVER_NAME} err_msg :: {e}.")


def insert_records(chunk):

    try:
        global rows_inserted, insert_records_failure_flag,insertion_err,insert_records_failure_flag_counter

        cnx = ENGINE.connect()

        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk.fillna('NULL', inplace=True)

        float_columns = chunk.select_dtypes(include='float').columns
        chunk[float_columns] = chunk[float_columns].replace([np.inf, -np.inf], np.nan)
        chunk[float_columns] = chunk[float_columns].astype(pd.Int64Dtype())

        insert_query = f"INSERT INTO {TABLE_NAME} ({', '.join(chunk.columns)}) VALUES ({', '.join([':' + col for col in chunk.columns])})"

        with ENGINE.connect() as connection:
            metadata = MetaData()
            table = Table(TABLE_NAME, metadata, autoload_with=ENGINE)

            values_list = chunk.to_dict(orient='records')
            ins = insert(table).values(values_list)
            connection.execute(ins)
        
        cnx.close()
        rows_inserted += len(chunk)

    except Exception as e:

        insertion_err += str(e)

        insert_records_failure_flag_counter += 1

        print(f"Unable to insert data in table :: {TABLE_NAME}. err_msg :: {insertion_err}")


def create_chunk(df):

    global insertion_err

    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

        futures = []

        print(f"Inserting data into table :: {TABLE_NAME}.")

        for chunk in chunks:
            future = executor.submit(insert_records,chunk)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

    print(f"Total number of rows inserted :: {rows_inserted}.")


if __name__ == '__main__':

    matching_file = FTP

    if matching_file:
        df = pd.read_csv(matching_file,sep=',',low_memory=False)
        
        create_chunk(df)

    else:
        
        print("No file found. Sys exit.")
        sys.exit(1) 
        
'''
['inin-outbound-id', 'cqRecordId', 'cqCampName', 'cqCampId', 'cqCampOrder', 'cqSourceDB', 'cqAppName', 'cqAppRecordId', 'cqFacility', 'cqAccountNum', 'cqFirstName', 'cqLastName', 'cqStateCode', 'cqZipCode', 'cqPhoneHome', 'cqPhoneWork', 'cqPhoneMobile', 'cqTimeZoneCode', 'cqDayLightFlag', 'cqFlag', 'cqNotes', 'cqDestination', 'WeightScore', 'ContactCallable', 'ContactableByVoice', 'ContactableBySms', 'ContactableByEmail', 'ZipCodeAutomaticTimeZone', 'CallRecordLastAttempt-cqPhoneHome', 'CallRecordLastResult-cqPhoneHome', 'CallRecordLastAgentWrapup-cqPhoneHome', 'SmsLastAttempt-cqPhoneHome', 'SmsLastResult-cqPhoneHome', 'Callable-cqPhoneHome', 'ContactableByVoice-cqPhoneHome', 'ContactableBySms-cqPhoneHome', 'AutomaticTimeZone-cqPhoneHome', 'CallRecordLastAttempt-cqPhoneWork', 'CallRecordLastResult-cqPhoneWork', 'CallRecordLastAgentWrapup-cqPhoneWork', 'SmsLastAttempt-cqPhoneWork', 'SmsLastResult-cqPhoneWork', 'Callable-cqPhoneWork', 'ContactableByVoice-cqPhoneWork', 'ContactableBySms-cqPhoneWork', 'AutomaticTimeZone-cqPhoneWork', 'CallRecordLastAttempt-cqPhoneMobile', 'CallRecordLastResult-cqPhoneMobile', 'CallRecordLastAgentWrapup-cqPhoneMobile', 'SmsLastAttempt-cqPhoneMobile', 'SmsLastResult-cqPhoneMobile', 'Callable-cqPhoneMobile', 'ContactableByVoice-cqPhoneMobile', 'ContactableBySms-cqPhoneMobile', 'AutomaticTimeZone-cqPhoneMobile']
Inserting data into table :: MFS_Export_GenesysRaw.
'''

err_msg :: (pyodbc.Error) ('HY104', '[HY104] [Microsoft][ODBC SQL Server Driver]Invalid precision value (0) (SQLBindParameter)')
[SQL: SELECT [INFORMATION_SCHEMA].[COLUMNS].[COLUMN_NAME], [INFORMATION_SCHEMA].[COLUMNS].[DATA_TYPE], [INFORMATION_SCHEMA].[COLUMNS].[IS_NULLABLE], [INFORMATION_SCHEMA].[COLUMNS].[CHARACTER_MAXIMUM_LENGTH], [INFORMATION_SCHEMA].[COLUMNS].[NUMERIC_PRECISION], [INFORMATION_SCHEMA].[COLUMNS].[NUMERIC_SCALE], [INFORMATION_SCHEMA].[COLUMNS].[COLUMN_DEFAULT], [INFORMATION_SCHEMA].[COLUMNS].[COLLATION_NAME], sys.computed_columns.definition, sys.computed_columns.is_persisted, 
sys.identity_columns.is_identity, CAST(sys.identity_columns.seed_value AS NUMERIC) AS seed_value, CAST(sys.identity_columns.increment_value AS NUMERIC) AS increment_value, CAST(sys.extended_properties.value AS NVARCHAR(max)) AS comment 
FROM [INFORMATION_SCHEMA].[COLUMNS] LEFT OUTER JOIN sys.computed_columns ON sys.computed_columns.object_id = object_id([INFORMATION_SCHEMA].[COLUMNS].[TABLE_SCHEMA] + CAST(? AS NVARCHAR(max)) + [INFORMATION_SCHEMA].[COLUMNS].[TABLE_NAME]) AND sys.computed_columns.name = ([INFORMATION_SCHEMA].[COLUMNS].[COLUMN_NAME] COLLATE DATABASE_DEFAULT) LEFT OUTER JOIN sys.identity_columns ON sys.identity_columns.object_id = object_id([INFORMATION_SCHEMA].[COLUMNS].[TABLE_SCHEMA] + 
CAST(? AS NVARCHAR(max)) + [INFORMATION_SCHEMA].[COLUMNS].[TABLE_NAME]) AND sys.identity_columns.name = ([INFORMATION_SCHEMA].[COLUMNS].[COLUMN_NAME] COLLATE 
DATABASE_DEFAULT) LEFT OUTER JOIN sys.extended_properties ON sys.extended_properties.class = ? AND sys.extended_properties.major_id = object_id([INFORMATION_SCHEMA].[COLUMNS].[TABLE_SCHEMA] + CAST(? AS NVARCHAR(max)) + [INFORMATION_SCHEMA].[COLUMNS].[TABLE_NAME]) AND sys.extended_properties.minor_id = [INFORMATION_SCHEMA].[COLUMNS].[ORDINAL_POSITION] AND sys.extended_properties.name = CAST(? AS NVARCHAR(max)) 
WHERE [INFORMATION_SCHEMA].[COLUMNS].[TABLE_NAME] = CAST(? AS NVARCHAR(max)) AND [INFORMATION_SCHEMA].[COLUMNS].[TABLE_SCHEMA] = CAST(? AS NVARCHAR(max)) ORDER BY [INFORMATION_SCHEMA].[COLUMNS].[ORDINAL_POSITION]]
[parameters: ('.', '.', 1, '.', 'MS_Description', 'MFS_Export_GenesysRaw', 'dbo')]
(Background on this error at: https://sqlalche.me/e/20/dbapi)
<Future at 0x123b315e130 state=finished returned NoneType>
