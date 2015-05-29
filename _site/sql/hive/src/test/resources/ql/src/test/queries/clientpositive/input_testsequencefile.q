set mapred.output.compress=true;
set mapred.output.compression.type=BLOCK;

CREATE TABLE dest4_sequencefile(key INT, value STRING) STORED AS SEQUENCEFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest4_sequencefile SELECT src.key, src.value;

FROM src
INSERT OVERWRITE TABLE dest4_sequencefile SELECT src.key, src.value;

set mapred.output.compress=false;
SELECT dest4_sequencefile.* FROM dest4_sequencefile;
