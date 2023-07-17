

Traceback (most recent call last):
  File "c:\Users\IN10011418\OneDrive - R1\Scripts\PYTHON\Sample.py", line 512, in <module>
    dump_csv_to_sql_table_parallel(
  File "c:\Users\IN10011418\OneDrive - R1\Scripts\PYTHON\Sample.py", line 509, in dump_csv_to_sql_table_parallel
    future.result()
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\concurrent\futures\_base.py", line 439, in result
    return self.__get_result()
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\concurrent\futures\_base.py", line 391, in __get_result
    raise self._exception
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\concurrent\futures\thread.py", line 58, in run
    result = self.fn(*self.args, **self.kwargs)
  File "c:\Users\IN10011418\OneDrive - R1\Scripts\PYTHON\Sample.py", line 489, in process_chunk
    dump_csv_to_table(engine, table, chunk)
  File "c:\Users\IN10011418\OneDrive - R1\Scripts\PYTHON\Sample.py", line 482, in dump_csv_to_table
    session.bulk_insert_mappings(table, data)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\orm\session.py", line 4479, in bulk_insert_mappings
    self._bulk_save_mappings(
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\orm\session.py", line 4541, in _bulk_save_mappings
    mapper = _class_to_mapper(mapper)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\orm\base.py", line 445, in _class_to_mapper
    return insp.mapper  # type: ignore
AttributeError: 'Table' object has no attribute 'mapper'
