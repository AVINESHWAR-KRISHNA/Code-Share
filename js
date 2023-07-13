
2023-07-13 14:56:47,179 - distributed.protocol.pickle - ERROR - Failed to serialize <ToPickle: HighLevelGraph with 1 layers.
<dask.highlevelgraph.HighLevelGraph object at 0x1c680010820>
 0. 1954192685376
>.
Traceback (most recent call last):
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\distributed\protocol\pickle.py", line 77, in dumps 
    result = cloudpickle.dumps(x, **dump_kwargs)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\cloudpickle\cloudpickle_fast.py", line 73, in dumps
    cp.dump(obj)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\cloudpickle\cloudpickle_fast.py", line 632, in dump
    return Pickler.dump(self, obj)
TypeError: cannot pickle 'sqlalchemy.cprocessors.UnicodeResultProcessor' object

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\distributed\protocol\pickle.py", line 81, in dumps 
    result = cloudpickle.dumps(x, **dump_kwargs)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\cloudpickle\cloudpickle_fast.py", line 73, in dumps
    cp.dump(obj)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\cloudpickle\cloudpickle_fast.py", line 632, in dump
    return Pickler.dump(self, obj)
TypeError: cannot pickle 'sqlalchemy.cprocessors.UnicodeResultProcessor' object
Traceback (most recent call last):
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\distributed\protocol\pickle.py", line 77, in dumps
    result = cloudpickle.dumps(x, **dump_kwargs)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\cloudpickle\cloudpickle_fast.py", line 73, in dumps
    cp.dump(obj)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\cloudpickle\cloudpickle_fast.py", line 632, in dump
    return Pickler.dump(self, obj)
TypeError: cannot pickle 'sqlalchemy.cprocessors.UnicodeResultProcessor' object

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\distributed\protocol\serialize.py", line 350, in serialize
    header, frames = dumps(x, context=context) if wants_context else dumps(x)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\distributed\protocol\serialize.py", line 73, in pickle_dumps
    frames[0] = pickle.dumps(
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\distributed\protocol\pickle.py", line 81, in dumps
    result = cloudpickle.dumps(x, **dump_kwargs)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\cloudpickle\cloudpickle_fast.py", line 73, in dumps
    cp.dump(obj)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\cloudpickle\cloudpickle_fast.py", line 632, in dump
    return Pickler.dump(self, obj)
TypeError: cannot pickle 'sqlalchemy.cprocessors.UnicodeResultProcessor' object

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "c:\Users\IN10011418\OneDrive - R1\Scripts\PYTHON\Sample.py", line 537, in <module>
    futures = client.map(insert_records, chunks)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\distributed\protocol\serialize.py", line 372, in serialize
    raise TypeError(msg, str(x)[:10000]) from exc
TypeError: ('Could not serialize object of type HighLevelGraph', '<ToPickle: HighLevelGraph with 1 layers.\n<dask.highlevelgraph.HighLevelGraph object at 0x1c680010820>\n 0. 1954192685376\n>')
