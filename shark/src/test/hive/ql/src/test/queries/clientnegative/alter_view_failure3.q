-- should fail:  can't use ALTER VIEW on a table
ALTER VIEW srcpart ADD PARTITION (ds='2012-12-31', hr='23');
