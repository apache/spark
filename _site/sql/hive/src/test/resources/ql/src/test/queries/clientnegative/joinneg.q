EXPLAIN FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = b.key)
SELECT Y.*;
