EXPLAIN FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
SELECT Y.*;

FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
SELECT Y.*;
