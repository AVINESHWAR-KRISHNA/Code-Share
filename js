

import pandas as pd
from sqlalchemy import create_engine 
from sqlalchemy import Column,String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from concurrent.futures import ThreadPoolExecutor

SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech'
DATABASE ='Srdial'
DRIVER = 'SQL+Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = r'C:\Users\IN10011418\OneDrive - R1\Desktop\MFS-TestData.csv'
MAX_THREADS = 25
CHUNK_SIZE = 10000

df = pd.read_csv(FTP, sep=',')
df = df.rename(columns=lambda x: x.replace('-', ''))
df.fillna('NULL', inplace=True)

Base = declarative_base()

columns = [column_name for column_name in df.columns]

YourTable = type('YourTable', (Base,),
                  {'__tablename__': TABLE_NAME, 
                    '__table_args__': {'extend_existing': True},
                    **{column_name: Column(String) for column_name in columns}})

ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
Session = sessionmaker(bind=ENGINE)
SESSION = Session()

def Process_chuck(chunk):
    try:
        SESSION.bulk_save_objects(chunk)
        SESSION.commit()

    except Exception as e:
        print("Error Occured :: ",str(e))
        SESSION.rollback()

try:
    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(Process_chuck, chunks)

except Exception as e:
    print("Error Occured :: ",str(e))

finally:
    SESSION.close()
