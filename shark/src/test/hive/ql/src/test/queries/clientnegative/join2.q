SELECT /*+ MAPJOIN(x) */ x.key, x.value, y.value
FROM src1 x LEFT OUTER JOIN src y ON (x.key = y.key);



