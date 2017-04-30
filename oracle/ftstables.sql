with fts as (
select dbo.object_name, max(table_scans_total) TABLE_SCANS from dba_hist_seg_stat ss, dba_objects dbo
where table_scans_total > 0
and dbo.object_id = ss.obj#
and object_type = 'TABLE'
group by dbo.object_name
order by max(table_scans_total) desc
)
select * from fts where rownum <= 15
