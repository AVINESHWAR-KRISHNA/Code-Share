

SELECT

tl.resource_type,
tl.resource_database_id,
DB_NAME (tl.resource_database_id) AS database_name,
tl.resource_associated_entity_id,
OBJECT_NAME (tl.resource_associated_entity_id, tl.resource_database_id) AS object_name,
tl.request_mode,
tl.request_status,
tl.request_session_id,
es.host_name,
es.program_name,
es.login_name

FROM
sys.dm_tran_locks AS tl
INNER JOIN sys.dm_exec_sessions AS es ON tl.request session id = es.session id

WHERE
tl.resource_associated_entity_id = OBJECT_ID('TableName')
AND tl.resource_database_id = DB_ID('DatabaseName')
AND tl.resource_type = 'OBJECT';
