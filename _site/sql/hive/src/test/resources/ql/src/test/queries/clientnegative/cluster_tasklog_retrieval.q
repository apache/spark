-- TaskLog retrieval upon Null Pointer Exception in Cluster

CREATE TEMPORARY FUNCTION evaluate_npe AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFEvaluateNPE';

FROM src
SELECT evaluate_npe(src.key) LIMIT 1;
