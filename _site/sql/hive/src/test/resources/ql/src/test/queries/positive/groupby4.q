FROM src
SELECT substr(src.key,1,1) GROUP BY substr(src.key,1,1)
