select object_owner,object_name, count(1) from GV$SQL_PLAN where operation='TABLE ACCESS' 
and options='FULL' and object_owner not in ('SYS', 'SYSTEM')
group by object_owner,object_name