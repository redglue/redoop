select  OWNER, TABLE_NAME, PARTITION_NAME, SEGMENT_TYPE, SIZE_GB from ( select owner, segment_name as table_name, partition_name,  segment_type, bytes / 1024 / 1024 / 1024 "SIZE_GB"
  from dba_segments where segment_type in ('TABLE', 'TABLE PARTITION')
  and owner not in ('SYS', 'SYSTEM', 'AUDSYS')
  order by bytes desc)
where rownum < 25
