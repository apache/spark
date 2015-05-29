--HIVE-3926 PPD on virtual column of partitioned table is not working

explain extended
select * from srcpart where BLOCK__OFFSET__INSIDE__FILE<100;
select * from srcpart where BLOCK__OFFSET__INSIDE__FILE<100;

explain extended
select b.* from src a join
  (select *,BLOCK__OFFSET__INSIDE__FILE from srcpart where BLOCK__OFFSET__INSIDE__FILE<100) b
    on a.key=b.key AND b.BLOCK__OFFSET__INSIDE__FILE<50 order by ds,hr,BLOCK__OFFSET__INSIDE__FILE;

select b.* from src a join
  (select *,BLOCK__OFFSET__INSIDE__FILE from srcpart where BLOCK__OFFSET__INSIDE__FILE<100) b
    on a.key=b.key AND b.BLOCK__OFFSET__INSIDE__FILE<50 order by ds,hr,BLOCK__OFFSET__INSIDE__FILE;
