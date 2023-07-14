Traceback (most recent call last):
  File "c:\Users\IN10011418\OneDrive - R1\Scripts\PYTHON\Sample.py", line 478, in <module>
    YourTable = type('YourTable', (Base,),
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\ext\declarative\api.py", line 76, in __init__
    _as_declarative(cls, classname, cls.__dict__)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\ext\declarative\base.py", line 131, in _as_declarative
    _MapperConfig.setup_mapping(cls, classname, dict_)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\ext\declarative\base.py", line 160, in setup_mapping  
    cfg_cls(cls_, classname, dict_)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\ext\declarative\base.py", line 194, in __init__       
    self._early_mapping()
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\ext\declarative\base.py", line 199, in _early_mapping 
    self.map()
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\ext\declarative\base.py", line 695, in map
    self.cls.__mapper__ = mp_ = mapper_cls(
  File "<string>", line 2, in mapper
  File "<string>", line 2, in __init__
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\util\deprecations.py", line 139, in warned
    return fn(*args, **kwargs)
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\orm\mapper.py", line 723, in __init__
    self._configure_pks()
  File "C:\Users\IN10011418\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\orm\mapper.py", line 1409, in _configure_pks
    raise sa_exc.ArgumentError(
sqlalchemy.exc.ArgumentError: Mapper mapped class YourTable->MFS_Export_GenesysRaw could not assemble any primary key columns for mapped table 'MFS_Export_GenesysRaw'
