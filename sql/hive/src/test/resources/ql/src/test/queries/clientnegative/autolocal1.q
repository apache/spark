set mapred.job.tracker=abracadabra;
set hive.exec.mode.local.auto.inputbytes.max=1;
set hive.exec.mode.local.auto=true;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.20)
-- hadoop0.23 changes the behavior of JobClient initialization
-- in hadoop0.20, JobClient initialization tries to get JobTracker's address
-- this throws the expected IllegalArgumentException
-- in hadoop0.23, JobClient initialization only initializes cluster
-- and get user group information
-- not attempts to get JobTracker's address
-- no IllegalArgumentException thrown in JobClient Initialization
-- an exception is thrown when JobClient submitJob

SELECT key FROM src; 
