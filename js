
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor

def create_sqlalchemy_engine(SERVER_NAME, DATABASE, DRIVER):
    connection_string = f"mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True"
    engine = create_engine(connection_string)
    return engine

def create_table(engine, TABLE_NAME, csv_columns):
    Base = declarative_base()
    metadata = MetaData(engine)

    class CustomTable(Base):
        __table__ = Table(
            TABLE_NAME,
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            *(Column(column, String(length=max)) for column in csv_columns)
        )

    Base.metadata.create_all(engine)
    return CustomTable.__table__

def dump_csv_to_table(engine, table, data):
    Session = sessionmaker(engine)
    session = Session()

    session.bulk_insert_mappings(table, data)
    session.commit()
    session.close()

def process_chunk(chunk, engine, table):
    with engine.connect() as conn:
        with conn.begin():
            dump_csv_to_table(engine, table, chunk)

def dump_csv_to_sql_table_parallel(SERVER_NAME, DATABASE, DRIVER,TABLE_NAME, FTP, CHUNK_SIZE=100000, MAX_THREADS=4):
    engine = create_sqlalchemy_engine(SERVER_NAME, DATABASE, DRIVER)
    with open(FTP, 'r', newline='', encoding='utf-8') as file:
        df = pd.read_csv(file,sep=',',low_memory=False)
        csv_columns = df.columns.tolist()

    table = create_table(engine, TABLE_NAME, csv_columns)

    with open(FTP, 'r', newline='', encoding='utf-8') as file:
        csv_data = pd.read_csv(file, chunksize=CHUNK_SIZE)

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = []
            for chunk in csv_data:
                data = chunk.astype(str).to_dict(orient='records')
                futures.append(executor.submit(process_chunk, data, engine, table))

            for future in futures:
                future.result()

if __name__ == "__main__":
    dump_csv_to_sql_table_parallel(

        SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech',
        DATABASE ='Srdial',
        DRIVER = 'SQL+Server',
        TABLE_NAME = 'MFS_Export_GenesysRaw',
        FTP = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-Test.csv',
        MAX_THREADS = 25,
        CHUNK_SIZE = 100000

    )


 MovedIn20Warning: The ``declarative_base()`` function is now available as sqlalchemy.orm.declarative_base(). (deprecated since: 2.0) (Background on SQLAlchemy 2.0 at: https://sqlalche.me/e/b8d9)
  Base = declarative_base()
Traceback (most recent call last):
  File "c:\Users\IN10011418\OneDrive - R1\Scripts\PYTHON\Sample.py", line 515, in <module>
    dump_csv_to_sql_table_parallel(
  File "c:\Users\IN10011418\OneDrive - R1\Scripts\PYTHON\Sample.py", line 500, in dump_csv_to_sql_table_parallel
    table = create_table(engine, TABLE_NAME, csv_columns)
  File "c:\Users\IN10011418\OneDrive - R1\Scripts\PYTHON\Sample.py", line 468, in create_table
    metadata = MetaData(engine)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\sql\schema.py", line 5440, in __init__
    raise exc.ArgumentError(
sqlalchemy.exc.ArgumentError: expected schema argument to be a string, got <class 'sqlalchemy.engine.base.Engine'>.
