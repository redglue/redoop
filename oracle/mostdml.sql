select owner, object_name, count(1) from v$active_session_history ash, dba_objects dbo
 where sql_opname in ('DELETE', 'UPDATE', 'INSERT')
 and dbo.object_id = ash.current_obj#
 group by owner, object_name, sql_plan_operation, sql_opname
 order by 3 desc
 
