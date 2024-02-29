

--- Looking at all currently Active Requests and the statements that are running
select 
a.session_id,
start_time,
b.host_name,
b.program_name,
DB_NAME(a.database_id) as DatabaseName,
a.status,
blocking_session_id,
wait_type,
wait_time,
wait_resource,
a.cpu_time,
a.total_elapsed_time,
scheduler_id,
a.reads,
a.writes,
 (SELECT TOP 1 SUBSTRING(s2.text,statement_start_offset / 2+1 , 
      ( (CASE WHEN statement_end_offset = -1 
         THEN (LEN(CONVERT(nvarchar(max),s2.text)) * 2) 
         ELSE statement_end_offset END)  - statement_start_offset) / 2+1))  AS sql_statement

, s2.text
, S3.query_plan
from 
sys.dm_exec_requests  a inner join 
sys.dm_exec_sessions b on a.session_id = b.session_id
CROSS APPLY sys.dm_exec_sql_text(a.sql_handle) AS s2 
CROSS APPLY sys.dm_exec_query_plan(a.sql_handle) as S3 
