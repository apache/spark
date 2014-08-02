-- partition spec for non-partitioned table
TRUNCATE TABLE src partition (ds='2008-04-08', hr='11');
