def insert_records(chunk, columns):
    try:
        with ENGINE.connect() as cnx:
            chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
            chunk = chunk.where(pd.notna(chunk), None)  # Replace NaN with None for NULL values
            placeholders = ', '.join(['%s'] * len(chunk.columns))
            values = [tuple(row) for row in chunk.values]

            stmt = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
            with cnx.begin() as transaction:
                cnx.execute(text(stmt), values)
                transaction.commit()

    except Exception as e:
        print(e)
        sys.exit(1)
def create_chunk(df):
    columns = ', '.join(df.columns)  # Get column names here

    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(insert_records, chunk, columns) for chunk in chunks]

        for future in concurrent.futures.as_completed(futures):
            print(future.result())
