        with ENGINE.connect() as connection:
            metadata = sqlalchemy.MetaData()
            table = sqlalchemy.Table(TABLE_NAME, metadata, autoload=True, autoload_with=ENGINE)

            values_list = chunk.to_dict(orient='records')
            ins = insert(table).values(values_list)
            connection.execute(ins)
