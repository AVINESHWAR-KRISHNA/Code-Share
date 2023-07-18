import os
import sys
import time
import pandas as pd
from sqlalchemy import create_engine, event,text
import concurrent.futures
import gc
gc.enable()

startT = time.time()

SERVER_NAME = 'DEVCONTWCOR01.r1rcm.tech'
DATABASE = 'Srdial'
DRIVER = 'ODBC Driver 17 for SQL Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
CSV_FILE = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-TestData.csv'
MAX_THREADS = 15
CHUNK_SIZE = 10000

try:
    ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}', fast_executemany=True)
    # Set up the event listener for fast_executemany outside the create_chunk function
    @event.listens_for(ENGINE, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
except Exception as e:
    print(e)
    sys.exit(1)

def insert_records(chunk):
    try:
        with ENGINE.connect() as cnx:
            chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
            chunk = chunk.where(pd.notna(chunk), None)  # Replace NaN with None for NULL values
            columns = ', '.join(chunk.columns)
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
    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(insert_records, chunk) for chunk in chunks]

        for future in concurrent.futures.as_completed(futures):
            print(future.result())

if __name__ == '__main__':
    df = pd.read_csv(CSV_FILE, sep=',', low_memory=False)
    create_chunk(df)

error

'<' not supported between instances of 'int' and 'str'
'<' not supported between instances of 'int' and 'str'
'<' not supported between instances of 'int' and 'str'
---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
Cell In[4], line 42, in insert_records(chunk)
     41 with cnx.begin() as transaction:
---> 42     cnx.execute(text(stmt), values)
     43     transaction.commit()

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\engine\base.py:1412, in Connection.execute(self, statement, parameters, execution_options)
   1411 else:
-> 1412     return meth(
   1413         self,
   1414         distilled_parameters,
   1415         execution_options or NO_OPTIONS,
   1416     )

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\sql\elements.py:483, in ClauseElement._execute_on_connection(self, connection, distilled_params, execution_options)
    482         assert isinstance(self, Executable)
--> 483     return connection._execute_clauseelement(
    484         self, distilled_params, execution_options
    485     )
    486 else:

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\engine\base.py:1611, in Connection._execute_clauseelement(self, elem, distilled_parameters, execution_options)
   1608 if distilled_parameters:
   1609     # ensure we don't retain a link to the view object for keys()
   1610     # which links to the values, which we don't want to cache
-> 1611     keys = sorted(distilled_parameters[0])
   1612     for_executemany = len(distilled_parameters) > 1

TypeError: '<' not supported between instances of 'int' and 'str'

During handling of the above exception, another exception occurred:

SystemExit                                Traceback (most recent call last)
    [... skipping hidden 1 frame]

Cell In[4], line 60
     59 df = pd.read_csv(CSV_FILE, sep=',', low_memory=False)
---> 60 create_chunk(df)

Cell In[4], line 56, in create_chunk(df)
     55 for future in concurrent.futures.as_completed(futures):
---> 56     print(future.result())

File ~\AppData\Local\Programs\Python\Python39\lib\concurrent\futures\_base.py:439, in Future.result(self, timeout)
    438 elif self._state == FINISHED:
--> 439     return self.__get_result()
    441 self._condition.wait(timeout)

File ~\AppData\Local\Programs\Python\Python39\lib\concurrent\futures\_base.py:391, in Future.__get_result(self)
    390 try:
--> 391     raise self._exception
    392 finally:
    393     # Break a reference cycle with the exception in self._exception

File ~\AppData\Local\Programs\Python\Python39\lib\concurrent\futures\thread.py:58, in _WorkItem.run(self)
     57 try:
---> 58     result = self.fn(*self.args, **self.kwargs)
     59 except BaseException as exc:

Cell In[4], line 47, in insert_records(chunk)
     46 print(e)
---> 47 sys.exit(1)

SystemExit: 1

During handling of the above exception, another exception occurred:

AttributeError                            Traceback (most recent call last)
    [... skipping hidden 1 frame]

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\IPython\core\interactiveshell.py:2095, in InteractiveShell.showtraceback(self, exc_tuple, filename, tb_offset, exception_only, running_compiled_code)
   2092 if exception_only:
   2093     stb = ['An exception has occurred, use %tb to see '
   2094            'the full traceback.\n']
-> 2095     stb.extend(self.InteractiveTB.get_exception_only(etype,
   2096                                                      value))
   2097 else:
   2098     try:
   2099         # Exception classes can customise their traceback - we
   2100         # use this in IPython.parallel for exceptions occurring
   2101         # in the engines. This should return a list of strings.

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\IPython\core\ultratb.py:710, in ListTB.get_exception_only(self, etype, value)
    702 def get_exception_only(self, etype, value):
    703     """Only print the exception type and message, without a traceback.
    704 
    705     Parameters
   (...)
    708     value : exception value
    709     """
--> 710     return ListTB.structured_traceback(self, etype, value)

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\IPython\core\ultratb.py:568, in ListTB.structured_traceback(self, etype, evalue, etb, tb_offset, context)
    565     chained_exc_ids.add(id(exception[1]))
    566     chained_exceptions_tb_offset = 0
    567     out_list = (
--> 568         self.structured_traceback(
    569             etype,
    570             evalue,
    571             (etb, chained_exc_ids),  # type: ignore
    572             chained_exceptions_tb_offset,
    573             context,
    574         )
    575         + chained_exception_message
    576         + out_list)
    578 return out_list

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\IPython\core\ultratb.py:1428, in AutoFormattedTB.structured_traceback(self, etype, evalue, etb, tb_offset, number_of_lines_of_context)
   1426 else:
   1427     self.tb = etb
-> 1428 return FormattedTB.structured_traceback(
   1429     self, etype, evalue, etb, tb_offset, number_of_lines_of_context
   1430 )

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\IPython\core\ultratb.py:1319, in FormattedTB.structured_traceback(self, etype, value, tb, tb_offset, number_of_lines_of_context)
   1316 mode = self.mode
   1317 if mode in self.verbose_modes:
   1318     # Verbose modes need a full traceback
-> 1319     return VerboseTB.structured_traceback(
   1320         self, etype, value, tb, tb_offset, number_of_lines_of_context
   1321     )
   1322 elif mode == 'Minimal':
   1323     return ListTB.get_exception_only(self, etype, value)

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\IPython\core\ultratb.py:1172, in VerboseTB.structured_traceback(self, etype, evalue, etb, tb_offset, number_of_lines_of_context)
   1163 def structured_traceback(
   1164     self,
   1165     etype: type,
   (...)
   1169     number_of_lines_of_context: int = 5,
   1170 ):
   1171     """Return a nice text document describing the traceback."""
-> 1172     formatted_exception = self.format_exception_as_a_whole(etype, evalue, etb, number_of_lines_of_context,
   1173                                                            tb_offset)
   1175     colors = self.Colors  # just a shorthand + quicker name lookup
   1176     colorsnormal = colors.Normal  # used a lot

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\IPython\core\ultratb.py:1062, in VerboseTB.format_exception_as_a_whole(self, etype, evalue, etb, number_of_lines_of_context, tb_offset)
   1059 assert isinstance(tb_offset, int)
   1060 head = self.prepare_header(str(etype), self.long_header)
   1061 records = (
-> 1062     self.get_records(etb, number_of_lines_of_context, tb_offset) if etb else []
   1063 )
   1065 frames = []
   1066 skipped = 0

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\IPython\core\ultratb.py:1130, in VerboseTB.get_records(self, etb, number_of_lines_of_context, tb_offset)
   1128 while cf is not None:
   1129     try:
-> 1130         mod = inspect.getmodule(cf.tb_frame)
   1131         if mod is not None:
   1132             mod_name = mod.__name__

AttributeError: 'tuple' object has no attribute 'tb_frame'
 
