EXPLAIN
SELECT CASE src_thrift.lint[0]
        WHEN 0 THEN src_thrift.lint[0] + 1
        WHEN 1 THEN src_thrift.lint[0] + 2
        WHEN 2 THEN 100
        ELSE 5
       END,
       CASE src_thrift.lstring[0]
        WHEN '0' THEN 'zero'
        WHEN '10' THEN CONCAT(src_thrift.lstring[0], " is ten")
        ELSE 'default'
       END,
       (CASE src_thrift.lstring[0]
        WHEN '0' THEN src_thrift.lstring
        ELSE NULL
       END)[0]
FROM src_thrift LIMIT 3;

SELECT CASE src_thrift.lint[0]
        WHEN 0 THEN src_thrift.lint[0] + 1
        WHEN 1 THEN src_thrift.lint[0] + 2
        WHEN 2 THEN 100
        ELSE 5
       END,
       CASE src_thrift.lstring[0]
        WHEN '0' THEN 'zero'
        WHEN '10' THEN CONCAT(src_thrift.lstring[0], " is ten")
        ELSE 'default'
       END,
       (CASE src_thrift.lstring[0]
        WHEN '0' THEN src_thrift.lstring
        ELSE NULL
       END)[0]
FROM src_thrift LIMIT 3;
