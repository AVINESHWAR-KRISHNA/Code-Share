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

df = pd.read_csv(FTP, sep=',')
df = df.rename(columns=lambda x: x.replace('-', ''))
df.fillna('NULL', inplace=True)

Base = declarative_base()

# Dictionary mapping pandas dtypes to SQLAlchemy column types
dtype_to_sqlalchemy = {
    'object': String,
    'int64': Integer,
    'float64': Float,
    'datetime64': DateTime,
    # Add more mappings as needed for other dtypes
}

columns = []

# Iterate over DataFrame columns and their dtypes
for column_name, dtype in df.dtypes.items():
    column_type = dtype_to_sqlalchemy.get(str(dtype))
    if column_type:
        columns.append(Column(column_name, column_type, primary_key=False))
    else:
        print(f"Skipping column '{column_name}' due to unsupported dtype: {dtype}")

YourTable = type('YourTable', (Base,),
                {'__tablename__': TABLE_NAME,
                 '__table_args__': {'extend_existing': True},
                 'id': Column(Integer, primary_key=True),  # Example primary key column
                 **{column_name: column for column_name, column in zip(df.columns, columns)}})

ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}', fast_executemany=True)
Session = sessionmaker(bind=ENGINE)
SESSION = Session()

# Rest of the code remains the same...
