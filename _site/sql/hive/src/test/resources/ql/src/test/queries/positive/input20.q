FROM (
  FROM src
  MAP src.key % 2, src.key % 5
  USING 'cat'
  CLUSTER BY key
) tmap
REDUCE tmap.key, tmap.value
USING 'uniq -c | sed "s@^ *@@" | sed "s@\t@_@" | sed "s@ @\t@"'
AS key, value
