FROM src src1 JOIN src src2 ON src1.key = src2.key
INSERT OVERWRITE TABLE dest1 SELECT key
