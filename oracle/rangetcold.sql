with range_partitioned as (
select pp.table_owner, pp.table_name, pp.partition_name, inmemory from ALL_PART_TABLES pt, ALL_TAB_PARTITIONS pp
where pt.owner not in ('SYS', 'SYSTEM', 'AUDSYS')
and pt.owner = pp.table_owner
and pt.partitioning_type = 'RANGE'
)
select rt.table_owner, rt.table_name, rt.partition_name, object_type, max(db_block_changes_delta) BLOCK_CHANGES, max(physical_writes_delta) PHYSICAL_WRITES
from dba_hist_seg_stat ss, dba_objects dbo, range_partitioned rt, DBA_HIST_SNAPSHOT dbh
where rt.table_name = dbo.object_name
and dbo.object_id = ss.obj#
and dbh.snap_id = ss.snap_id
--3 months
and END_INTERVAL_TIME >= sysdate - 90
group by to_char(END_INTERVAL_TIME, 'mon-yyyy'), rt.table_owner, rt.table_name, rt.partition_name, object_type
order by BLOCK_CHANGES asc, PHYSICAL_WRITES asc
