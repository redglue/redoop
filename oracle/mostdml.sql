select owner, object_name, count(1) as NUM_OPERATIONS from v$active_session_history ash, dba_objects dbo
 where sql_opname in ('DELETE', 'UPDATE', 'INSERT')
 and dbo.object_id = ash.current_obj#
 and owner not in ('SYS', 'SYSTEM')
 group by owner, object_name, sql_plan_operation, sql_opname
 order by 3 desc
