import pandas as pd

# Step 1: Read the CSV file into a pandas DataFrame
csv_file_path = "path_to_your_csv_file.csv"
df = pd.read_csv(csv_file_path)

# Step 2: Remove hyphens from column names
df.columns = df.columns.str.replace('-', '')

# Step 3: Save the modified DataFrame to a new CSV file
modified_csv_file_path = "path_to_modified_csv_file.csv"
df.to_csv(modified_csv_file_path, index=False)

# Now you have a new CSV file with column names without hyphens that you can use with the SQL Server script.

# Step 4: Execute the SQL Server script with the modified CSV file using your preferred method.
# You can use a SQL Server client or a Python library like pyodbc to execute the SQL script with the modified CSV file as follows:

import pyodbc

# Replace the connection details with your actual SQL Server credentials and server information
server = 'your_server_name'
database = 'your_database_name'
username = 'your_username'
password = 'your_password'

# Establish a connection to the SQL Server database
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};'
conn = pyodbc.connect(conn_str)

# Define the SQL Server script with the BULK INSERT logic
sql_script = f'''
USE [{database}]
BULK INSERT [table_name]
FROM '{modified_csv_file_path}'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDQUOTE = '"',
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '0x0a'
);
'''

# Execute the SQL script
with conn.cursor() as cursor:
    cursor.execute(sql_script)
    conn.commit()

# Close the connection
conn.close()
