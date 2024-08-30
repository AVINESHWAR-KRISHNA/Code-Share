import asyncio
from sqlalchemy import create_engine, MetaData, Table, select, text, inspect, insert
from sqlalchemy.dialects.mssql import insert as mssql_insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from concurrent.futures import ThreadPoolExecutor

# Configuration Parameters
source_conn_str = "mssql+pyodbc://<username>:<password>@<source_server>/<source_db>?driver=ODBC+Driver+17+for+SQL+Server"
dest_conn_str = "mssql+pyodbc://<username>:<password>@<dest_server>/<dest_db>?driver=ODBC+Driver+17+for+SQL+Server"
source_table_name = '<source_table>'
dest_table_name = '<dest_table>'
columns_to_read = ['id', 'column1', 'column2']  # List of columns to read from source
filter_clause = 'WHERE column1 > 1000'   # Example filter
partition_column = 'id'  # Use 'id' or 'timestamp' for partitioning

# Async engine and session setup
source_engine = create_async_engine(source_conn_str, echo=True, pool_size=10, max_overflow=20)
dest_engine = create_async_engine(dest_conn_str, echo=True, pool_size=10, max_overflow=20)
AsyncSessionLocal = sessionmaker(bind=dest_engine, class_=AsyncSession, expire_on_commit=False)

# Fetch column data types dynamically
async def fetch_column_types(engine, table_name, columns):
    async with engine.connect() as conn:
        inspector = inspect(engine)
        table_columns = inspector.get_columns(table_name)
        return {col['name']: col['type'] for col in table_columns if col['name'] in columns}

# Asynchronous bulk upsert operation
async def bulk_upsert(session, dest_table, data):
    try:
        # Perform upsert operation
        stmt = mssql_insert(dest_table).values(data)
        # Define the update on conflict condition
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=['id'],  # Assuming 'id' is the unique constraint
            set_={c.key: c for c in stmt.excluded if c.key != 'id'}
        )
        await session.execute(upsert_stmt)
        await session.commit()
    except Exception as e:
        await session.rollback()
        print(f"Error in upsert operation: {e}")

# Parallel data read using partitioning
async def partition_data_read(engine, table, columns, partition_column, partition_value):
    async with engine.connect() as conn:
        query = select([table]).where(text(f"{partition_column} = :value")).params(value=partition_value)
        result = await conn.execute(query)
        return [dict(row) for row in result.fetchall()]

# Asynchronous processing function
async def process_data_partitions(partitions):
    async with AsyncSessionLocal() as session:
        tasks = [bulk_upsert(session, dest_table, partition) for partition in partitions]
        await asyncio.gather(*tasks)

# Main Execution
async def main():
    metadata = MetaData()

    # Define source and destination tables
    source_table = Table(source_table_name, metadata, autoload_with=source_engine)
    dest_table = Table(dest_table_name, metadata, autoload_with=dest_engine)
    
    # Fetch column types dynamically
    column_types = await fetch_column_types(source_engine, source_table_name, columns_to_read)
    
    # Partition data read using asyncio and ThreadPoolExecutor
    partition_values = range(1, 11)  # Example partition range
    partitions = await asyncio.gather(*[partition_data_read(source_engine, source_table, columns_to_read, partition_column, value) for value in partition_values])

    # Process partitions asynchronously
    await process_data_partitions(partitions)

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())