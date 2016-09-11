set hive.stats.dbclass=counter;
set hive.stats.autogather=true;

DROP TABLE lineitem;
CREATE TABLE lineitem (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      DOUBLE,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DOUBLE,
                                L_RETURNFLAG    STRING,
                                L_LINESTATUS    STRING,
                                l_shipdate      STRING,
                                L_COMMITDATE    STRING,
                                L_RECEIPTDATE   STRING,
                                L_SHIPINSTRUCT  STRING,
                                L_SHIPMODE      STRING,
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '../../data/files/lineitem.txt' OVERWRITE INTO TABLE lineitem;

CREATE INDEX lineitem_lshipdate_idx ON TABLE lineitem(l_shipdate) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(l_shipdate)");
ALTER INDEX lineitem_lshipdate_idx ON lineitem REBUILD;

explain select l_shipdate, count(l_shipdate)
from lineitem
group by l_shipdate;

select l_shipdate, count(l_shipdate)
from lineitem
group by l_shipdate
order by l_shipdate;

set hive.optimize.index.groupby=true;

explain select l_shipdate, count(l_shipdate)
from lineitem
group by l_shipdate;

select l_shipdate, count(l_shipdate)
from lineitem
group by l_shipdate
order by l_shipdate;

set hive.optimize.index.groupby=false;


explain select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(l_shipdate) as monthly_shipments
from lineitem
group by year(l_shipdate), month(l_shipdate) 
order by year, month;

select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(l_shipdate) as monthly_shipments
from lineitem
group by year(l_shipdate), month(l_shipdate) 
order by year, month;

set hive.optimize.index.groupby=true;

explain select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(l_shipdate) as monthly_shipments
from lineitem
group by year(l_shipdate), month(l_shipdate) 
order by year, month;

select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(l_shipdate) as monthly_shipments
from lineitem
group by year(l_shipdate), month(l_shipdate) 
order by year, month;

explain select lastyear.month,
        thisyear.month,
        (thisyear.monthly_shipments - lastyear.monthly_shipments) /
lastyear.monthly_shipments as monthly_shipments_delta
   from (select year(l_shipdate) as year,
                month(l_shipdate) as month,
                count(l_shipdate) as monthly_shipments
           from lineitem
          where year(l_shipdate) = 1997
          group by year(l_shipdate), month(l_shipdate)
        )  lastyear join
        (select year(l_shipdate) as year,
                month(l_shipdate) as month,
                count(l_shipdate) as monthly_shipments
           from lineitem
          where year(l_shipdate) = 1998
          group by year(l_shipdate), month(l_shipdate)
        )  thisyear
  on lastyear.month = thisyear.month;

explain  select l_shipdate, cnt
from (select l_shipdate, count(l_shipdate) as cnt from lineitem group by l_shipdate
union all
select l_shipdate, l_orderkey as cnt
from lineitem) dummy;

CREATE TABLE tbl(key int, value int);
CREATE INDEX tbl_key_idx ON TABLE tbl(key) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(key)");
ALTER INDEX tbl_key_idx ON tbl REBUILD;

EXPLAIN select key, count(key) from tbl where key = 1 group by key;
EXPLAIN select key, count(key) from tbl group by key;

EXPLAIN select count(1) from tbl;
EXPLAIN select count(key) from tbl;

EXPLAIN select key FROM tbl GROUP BY key;
EXPLAIN select key FROM tbl GROUP BY value, key;
EXPLAIN select key FROM tbl WHERE key = 3 GROUP BY key;
EXPLAIN select key FROM tbl WHERE value = 2 GROUP BY key;
EXPLAIN select key FROM tbl GROUP BY key, substr(key,2,3);

EXPLAIN select key, value FROM tbl GROUP BY value, key;
EXPLAIN select key, value FROM tbl WHERE value = 1 GROUP BY key, value;

EXPLAIN select DISTINCT key FROM tbl;
EXPLAIN select DISTINCT key FROM tbl;
EXPLAIN select DISTINCT key FROM tbl;
EXPLAIN select DISTINCT key, value FROM tbl;
EXPLAIN select DISTINCT key, value FROM tbl WHERE value = 2;
EXPLAIN select DISTINCT key, value FROM tbl WHERE value = 2 AND key = 3;
EXPLAIN select DISTINCT key, value FROM tbl WHERE value = key;
EXPLAIN select DISTINCT key, substr(value,2,3) FROM tbl WHERE value = key;
EXPLAIN select DISTINCT key, substr(value,2,3) FROM tbl;

EXPLAIN select * FROM (select DISTINCT key, value FROM tbl) v1 WHERE v1.value = 2;

DROP TABLE tbl;

CREATE TABLE tblpart (key int, value string) PARTITIONED BY (ds string, hr int);
INSERT OVERWRITE TABLE tblpart PARTITION (ds='2008-04-08', hr=11) SELECT key, value FROM srcpart WHERE ds = '2008-04-08' AND hr = 11;
INSERT OVERWRITE TABLE tblpart PARTITION (ds='2008-04-08', hr=12) SELECT key, value FROM srcpart WHERE ds = '2008-04-08' AND hr = 12;
INSERT OVERWRITE TABLE tblpart PARTITION (ds='2008-04-09', hr=11) SELECT key, value FROM srcpart WHERE ds = '2008-04-09' AND hr = 11;
INSERT OVERWRITE TABLE tblpart PARTITION (ds='2008-04-09', hr=12) SELECT key, value FROM srcpart WHERE ds = '2008-04-09' AND hr = 12;

CREATE INDEX tbl_part_index ON TABLE tblpart(key) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(key)");

ALTER INDEX tbl_part_index ON tblpart PARTITION (ds='2008-04-08', hr=11) REBUILD;
EXPLAIN SELECT key, count(key) FROM tblpart WHERE ds='2008-04-09' AND hr=12 AND key < 10 GROUP BY key;

ALTER INDEX tbl_part_index ON tblpart PARTITION (ds='2008-04-08', hr=12) REBUILD;
ALTER INDEX tbl_part_index ON tblpart PARTITION (ds='2008-04-09', hr=11) REBUILD;
ALTER INDEX tbl_part_index ON tblpart PARTITION (ds='2008-04-09', hr=12) REBUILD;
EXPLAIN SELECT key, count(key) FROM tblpart WHERE ds='2008-04-09' AND hr=12 AND key < 10 GROUP BY key;

DROP INDEX tbl_part_index on tblpart;
DROP TABLE tblpart;

CREATE TABLE tbl(key int, value int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'; 
LOAD DATA LOCAL INPATH '../../data/files/tbl.txt' OVERWRITE INTO TABLE tbl;

CREATE INDEX tbl_key_idx ON TABLE tbl(key) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(key)");
ALTER INDEX tbl_key_idx ON tbl REBUILD;

set hive.optimize.index.groupby=false;
explain select key, count(key) from tbl group by key order by key;
select key, count(key) from tbl group by key order by key;
set hive.optimize.index.groupby=true;
explain select key, count(key) from tbl group by key order by key;
select key, count(key) from tbl group by key order by key;
DROP TABLE tbl;