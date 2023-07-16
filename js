import vaex
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor

# Define the CSV file path and SQL Server connection details
csv_file_path = 'path/to/large_data.csv'
sql_server_connection = 'mssql+pyodbc://username:password@server_name/database_name?driver=ODBC+Driver+17+for+SQL+Server'

# Create a SQLAlchemy engine and session
Base = declarative_base()
engine = create_engine(sql_server_connection)
Session = sessionmaker(bind=engine)

# Define the SQL Server table schema using SQLAlchemy ORM
class LargeData(Base):
    __tablename__ = 'large_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    col1 = Column(String)
    col2 = Column(Float)
    # Add more columns as needed

# Function to load a chunk of data into the SQL Server table
def load_chunk(chunk):
    session = Session()
    session.bulk_save_objects(chunk)
    session.commit()
    session.close()

# Function to load the entire CSV file into the SQL Server table using Vaex and parallel processing
def load_large_csv_to_sql(file_path, chunk_size=100000, num_workers=4):
    # Load the CSV file using Vaex
    df = vaex.from_csv(file_path, convert=True)

    # Define the chunk size and calculate the number of chunks
    num_rows = len(df)
    num_chunks = (num_rows + chunk_size - 1) // chunk_size

    # Split the Vaex DataFrame into chunks
    chunks = [df[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks)]

    # Create the SQL Server table if it doesn't exist
    Base.metadata.create_all(engine)

    # Load the chunks into the SQL Server table using concurrent processing
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        executor.map(load_chunk, chunks)

if __name__ == "__main__":
    load_large_csv_to_sql(csv_file_path)
