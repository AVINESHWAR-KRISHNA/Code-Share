from sqlalchemy import event
@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(
       conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True

df.to_sql(tbl, engine, index=False, if_exists="append", schema="dbo")
