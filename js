
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from concurrent.futures import ThreadPoolExecutor

SERVER_NAME = 'DEVCONTWCOR01.r1rcm.tech'
DATABASE = 'Srdial'
DRIVER = 'SQL+Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = r'C:\Users\IN10011418\OneDrive - R1\Desktop\MFS-TestData.csv'
MAX_THREADS = 25
CHUNK_SIZE = 10000

df = pd.read_csv(FTP, sep=',', low_memory=False)
df = df.rename(columns=lambda x: x.replace('-', ''))
df.fillna('NULL', inplace=True)

Base = declarative_base()

ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
Session = sessionmaker(bind=ENGINE)
SESSION = Session()

dtype_to_sqlalchemy = {
    'object': String,
    'int64': Integer,
    'float64': Float,
    'datetime64': DateTime
}

columns = []


for column_name, dtype in df.dtypes.items():
    column_type = dtype_to_sqlalchemy.get(str(dtype))
    if column_type:
        columns.append(Column(column_name, column_type, primary_key=False))
    else:
        print(f"Skipping column '{column_name}' due to unsupported dtype: {dtype}")

YourTable = type('YourTable', (Base,),
                {'__tablename__': TABLE_NAME,
                 '__table_args__': {'extend_existing': True},
                 'id': Column(Integer, primary_key=True), 
                 **{column_name: column for column_name, column in zip(df.columns, columns)}})

def Process_chuck(chunk):
    try:
        objects = [YourTable(**row) for row in chunk.to_dict(orient='records')]
        SESSION.bulk_save_objects(objects)
        SESSION.commit()
    except Exception as e:
        print("Error Occurred :: ", str(e))
        SESSION.rollback()

try:
    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(Process_chuck, chunks)

except Exception as e:
    print("Error Occured :: ",str(e))

finally:
    SESSION.close()
