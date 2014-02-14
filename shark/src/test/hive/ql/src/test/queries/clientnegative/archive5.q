set hive.archive.enabled = true;
-- Tests creating a partition where the partition value will collide with the
-- a intermediate directory

ALTER TABLE srcpart ADD PARTITION (ds='2008-04-08', hr='14_INTERMEDIATE_ORIGINAL')
