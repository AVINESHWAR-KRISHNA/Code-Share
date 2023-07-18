# ...

def create_insert_query(table_name, columns):
    placeholders = ', '.join(['%s'] * len(columns))
    return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

def insert_records(chunk):
    try:
        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk = chunk.where(pd.notna(chunk), None)  # Replace NaN with None for NULL values

        columns = chunk.columns
        insert_query = create_insert_query(TABLE_NAME, columns)

        values = [tuple(row) for row in chunk.values]

        # Combine the INSERT INTO query and the VALUES clause
        full_query = f"{insert_query} {str(values)[1:-1]}"

        return full_query

    except Exception as e:
        print(e)
        sys.exit(1)
