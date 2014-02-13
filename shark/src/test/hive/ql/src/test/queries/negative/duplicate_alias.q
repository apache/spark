FROM src a JOIN src a ON (a.key = a.key)
INSERT OVERWRITE TABLE dest1 SELECT a.key, a.value
