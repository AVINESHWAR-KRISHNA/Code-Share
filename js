from tortoise import fields, Model, Tortoise
import csv
from multiprocessing import Pool, cpu_count

# Database connection details
database_url = 'your_database_url'
database_name = 'your_database_name'
table_name = 'your_table_name'

# CSV file details
csv_file_path = 'path_to_your_csv_file.csv'

# Batch size for insertion
batch_size = 10000

# Define the Tortoise ORM model dynamically based on CSV headers
def create_model_class(headers):
    fields_dict = {}
    for header in headers:
        fields_dict[header] = fields.CharField(max_length=255)
    return type('YourModel', (Model,), fields_dict)

# Function to load a CSV chunk
def load_csv_chunk(chunk):
    YourModel, rows = chunk
    YourModel.bulk_create(rows)

# Define a function to load the CSV file
def load_csv():
    # Read the CSV file and extract headers
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        headers = next(csv_reader)

        # Generate the model class dynamically based on CSV headers
        YourModel = create_model_class(headers)

        # Initialize multiprocessing pool
        pool = Pool(processes=cpu_count())

        # Load CSV in chunks and process them concurrently
        chunk = []
        for row in csv_reader:
            row_dict = dict(zip(headers, row))
            chunk.append(row_dict)

            if len(chunk) >= batch_size:
                pool.apply_async(load_csv_chunk, args=((YourModel, chunk.copy()),))
                chunk = []

        if chunk:
            pool.apply_async(load_csv_chunk, args=((YourModel, chunk),))

        # Close the pool and wait for all processes to finish
        pool.close()
        pool.join()

# Connect to the database and load the CSV file
async def main():
    await Tortoise.init(db_url=database_url, modules={'models': ['__main__']})
    await Tortoise.generate_schemas()

    await load_csv()

    await Tortoise.close_connections()

# Run the main function
Tortoise.run(main)




import vaex
from tortoise import fields, Model, Tortoise
import csv
from multiprocessing import Pool, cpu_count

# Database connection details
database_url = 'your_database_url'
database_name = 'your_database_name'
table_name = 'your_table_name'

# CSV file details
csv_file_path = 'path_to_your_csv_file.csv'

# Batch size for insertion
batch_size = 10000

# Define the Tortoise ORM model dynamically based on CSV headers
def create_model_class(headers):
    fields_dict = {}
    for header in headers:
        fields_dict[header] = fields.CharField(max_length=255)
    return type('YourModel', (Model,), fields_dict)

# Function to load a CSV chunk
def load_csv_chunk(chunk):
    YourModel, rows = chunk
    YourModel.bulk_create(rows)

# Define a function to load the CSV file
def load_csv():
    # Read the CSV file and extract headers
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        headers = next(csv_reader)

        # Generate the model class dynamically based on CSV headers
        YourModel = create_model_class(headers)

        # Create vaex DataFrame from the CSV file
        df = vaex.from_csv(csv_file_path, copy_index=False, names=headers)

        # Get the total number of rows
        total_rows = len(df)

        # Initialize multiprocessing pool
        pool = Pool(processes=cpu_count())

        # Load CSV in chunks and process them concurrently
        chunk = []
        for i, chunk_start in enumerate(range(0, total_rows, batch_size)):
            chunk_end = min(chunk_start + batch_size, total_rows)
            chunk_df = df[chunk_start:chunk_end]
            chunk_rows = chunk_df.to_pandas_df().to_dict('records')
            chunk.extend(chunk_rows)

            if len(chunk) >= batch_size:
                pool.apply_async(load_csv_chunk, args=((YourModel, chunk.copy()),))
                chunk = []

        if chunk:
            pool.apply_async(load_csv_chunk, args=((YourModel, chunk),))

        # Close the pool and wait for all processes to finish
        pool.close()
        pool.join()

# Connect to the database and load the CSV file
async def main():
    await Tortoise.init(db_url=database_url, modules={'models': ['__main__']})
    await Tortoise.generate_schemas()

    await load_csv()

    await Tortoise.close_connections()

# Run the main function
Tortoise.run(main)
