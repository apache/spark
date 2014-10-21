create external table external1 (a int, b int) partitioned by (ds string);

-- trucate for non-managed table
TRUNCATE TABLE external1;
