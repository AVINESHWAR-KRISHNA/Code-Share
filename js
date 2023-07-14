YourTable = type('YourTable', (Base,),
                 {'__tablename__': TABLE_NAME,
                  '__table_args__': {'extend_existing': True},
                  'id': Column(String, primary_key=True),  # Add a primary key column
                  **{column_name: Column(String) for column_name in columns}})
