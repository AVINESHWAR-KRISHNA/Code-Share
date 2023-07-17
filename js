import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor

def create_sqlalchemy_engine(username, password, host, database, driver):
    connection_string = f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver={driver}"
    engine = create_engine(connection_string)
    return engine

def create_table(engine, table_name, csv_columns):
    Base = declarative_base()
    metadata = MetaData(bind=engine)

    class CustomTable(Base):
        __table__ = Table(
            table_name,
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            *(Column(column, String(length=255)) for column in csv_columns)
        )

    Base.metadata.create_all(engine)
    return CustomTable.__table__

def dump_csv_to_table(engine, table, data):
    Session = sessionmaker(bind=engine)
    session = Session()

    session.bulk_insert_mappings(table, data)
    session.commit()
    session.close()

def process_chunk(chunk, engine, table):
    with engine.connect() as conn:
        with conn.begin():
            dump_csv_to_table(engine, table, chunk)

def dump_csv_to_sql_table_parallel(username, password, host, database, driver, table_name, csv_file_path, chunk_size=1000, num_workers=4):
    engine = create_sqlalchemy_engine(username, password, host, database, driver)
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as file:
        df = pd.read_csv(file)
        csv_columns = df.columns.tolist()

    table = create_table(engine, table_name, csv_columns)

    with open(csv_file_path, 'r', newline='', encoding='utf-8') as file:
        csv_data = pd.read_csv(file, chunksize=chunk_size)

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            for chunk in csv_data:
                data = chunk.astype(str).to_dict(orient='records')
                futures.append(executor.submit(process_chunk, data, engine, table))

            for future in futures:
                future.result()

if __name__ == "__main__":
    dump_csv_to_sql_table_parallel(
        username='your_username',
        password='your_password',
        host='your_host',
        database='your_database',
        driver='your_driver',
        table_name='your_table_name',
        csv_file_path='path/to/your/csv/file.csv',
        chunk_size=1000,  # Adjust this value based on the appropriate chunk size
        num_workers=4  # Adjust the number of workers based on your system's capabilities
    )
