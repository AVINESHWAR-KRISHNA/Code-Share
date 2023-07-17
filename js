import pandas as pd
import pyodbc

# Connection details
server_name = 'your_server_name'
database_name = 'your_database_name'
username = 'your_username'
password = 'your_password'
data_file_path = 'path_to_your_data_file.csv'  # Replace this with the actual file path
table_name = 'target_table_name'  # Replace this with the target table name

# Establish a connection
connection_string = f"Driver={{SQL Server}};Server={server_name};Database={database_name};UID={username};PWD={password};"
connection = pyodbc.connect(connection_string)

try:
    # Create a cursor
    cursor = connection.cursor()

    # Step 1: Read the CSV file into a DataFrame
    df = pd.read_csv(data_file_path)

    # Step 2: Remove hyphens from column names
    df.columns = df.columns.str.replace('-', '')

    # Step 3: Save the modified DataFrame to a temporary CSV file
    temp_csv_file_path = 'temp_data_file.csv'
    df.to_csv(temp_csv_file_path, index=False)  # Add index=False to remove the index column

    # Step 4: BULK INSERT query
    bulk_insert_query = f"BULK INSERT {table_name} FROM '{temp_csv_file_path}' WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n', FIRSTROW = 2);"

    # Step 5: Execute the BULK INSERT query
    cursor.execute(bulk_insert_query)

    # Step 6: Commit the changes
    connection.commit()

    print("Bulk insert completed successfully.")

except pyodbc.Error as e:
    print("Error occurred:", e)

finally:
    # Step 7: Close the cursor and connection
    cursor.close()
    connection.close()

    # Step 8: Remove the temporary CSV file
    import os
    if os.path.exists(temp_csv_file_path):
        os.remove(temp_csv_file_path)

