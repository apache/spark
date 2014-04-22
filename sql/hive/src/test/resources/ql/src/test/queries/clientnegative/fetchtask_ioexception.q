CREATE TABLE fetchtask_ioexception (
  KEY STRING,
  VALUE STRING) STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH '../data/files/kv1_broken.seq' OVERWRITE INTO TABLE fetchtask_ioexception;

SELECT * FROM fetchtask_ioexception;
