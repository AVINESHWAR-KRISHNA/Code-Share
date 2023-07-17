
---------------------------------------------------------------------------
MemoryError                               Traceback (most recent call last)
Cell In[60], line 1
----> 1 cnx.execute(stmt, CHUNK.to_dict(orient='records'))

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\engine\base.py:1412, in Connection.execute(self, statement, parameters, execution_options)
   1410     raise exc.ObjectNotExecutableError(statement) from err
   1411 else:
-> 1412     return meth(
   1413         self,
   1414         distilled_parameters,
   1415         execution_options or NO_OPTIONS,
   1416     )

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\sql\elements.py:483, in ClauseElement._execute_on_connection(self, connection, distilled_params, execution_options)
    481     if TYPE_CHECKING:
    482         assert isinstance(self, Executable)
--> 483     return connection._execute_clauseelement(
    484         self, distilled_params, execution_options
    485     )
    486 else:
    487     raise exc.ObjectNotExecutableError(self)

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\engine\base.py:1635, in Connection._execute_clauseelement(self, elem, distilled_parameters, execution_options)
   1623 compiled_cache: Optional[CompiledCacheType] = execution_options.get(
   1624     "compiled_cache", self.engine._compiled_cache
   1625 )
   1627 compiled_sql, extracted_params, cache_hit = elem._compile_w_cache(
   1628     dialect=dialect,
   1629     compiled_cache=compiled_cache,
   (...)
   1633     linting=self.dialect.compiler_linting | compiler.WARN_LINTING,
   1634 )
-> 1635 ret = self._execute_context(
   1636     dialect,
   1637     dialect.execution_ctx_cls._init_compiled,
   1638     compiled_sql,
   1639     distilled_parameters,
   1640     execution_options,
   1641     compiled_sql,
   1642     distilled_parameters,
   1643     elem,
   1644     extracted_params,
   1645     cache_hit=cache_hit,
   1646 )
   1647 if has_events:
   1648     self.dispatch.after_execute(
   1649         self,
   1650         elem,
   (...)
   1654         ret,
   1655     )

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\engine\base.py:1844, in Connection._execute_context(self, dialect, constructor, statement, parameters, execution_options, *args, **kw)
   1839     return self._exec_insertmany_context(
   1840         dialect,
   1841         context,
   1842     )
   1843 else:
-> 1844     return self._exec_single_context(
   1845         dialect, context, statement, parameters
   1846     )

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\engine\base.py:1984, in Connection._exec_single_context(self, dialect, context, statement, parameters)
   1981     result = context._setup_result_proxy()
   1983 except BaseException as e:
-> 1984     self._handle_dbapi_exception(
   1985         e, str_statement, effective_parameters, cursor, context
   1986     )
   1988 return result

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\engine\base.py:2342, in Connection._handle_dbapi_exception(self, e, statement, parameters, cursor, context, is_sub_exec)
   2340     else:
   2341         assert exc_info[1] is not None
-> 2342         raise exc_info[1].with_traceback(exc_info[2])
   2343 finally:
   2344     del self._reentrant_error

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\engine\base.py:1934, in Connection._exec_single_context(self, dialect, context, statement, parameters)
   1932                 break
   1933     if not evt_handled:
-> 1934         self.dialect.do_executemany(
   1935             cursor,
   1936             str_statement,
   1937             effective_parameters,
   1938             context,
   1939         )
   1940 elif not effective_parameters and context.no_parameters:
   1941     if self.dialect._has_events:

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\dialects\mssql\pyodbc.py:716, in MSDialect_pyodbc.do_executemany(self, cursor, statement, parameters, context)
    714 if self.fast_executemany:
    715     cursor.fast_executemany = True
--> 716 super().do_executemany(cursor, statement, parameters, context=context)

File ~\AppData\Local\Programs\Python\Python39\lib\site-packages\sqlalchemy\engine\default.py:918, in DefaultDialect.do_executemany(self, cursor, statement, parameters, context)
    917 def do_executemany(self, cursor, statement, parameters, context=None):
--> 918     cursor.executemany(statement, parameters)

MemoryError: 
