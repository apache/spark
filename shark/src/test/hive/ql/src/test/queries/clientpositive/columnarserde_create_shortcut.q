CREATE TABLE columnarserde_create_shortcut(a array<int>, b array<string>, c map<string,string>, d int, e string) STORED AS RCFILE;

EXPLAIN
FROM src_thrift
INSERT OVERWRITE TABLE columnarserde_create_shortcut SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring DISTRIBUTE BY 1;

FROM src_thrift
INSERT OVERWRITE TABLE columnarserde_create_shortcut SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring DISTRIBUTE BY 1;

SELECT columnarserde_create_shortcut.* FROM columnarserde_create_shortcut CLUSTER BY 1;

SELECT columnarserde_create_shortcut.a[0], columnarserde_create_shortcut.b[0], columnarserde_create_shortcut.c['key2'], columnarserde_create_shortcut.d, columnarserde_create_shortcut.e FROM columnarserde_create_shortcut CLUSTER BY 1;

CREATE table columnShortcutTable (key STRING, value STRING) STORED AS RCFILE;

FROM src
INSERT OVERWRITE TABLE columnShortcutTable SELECT src.key, src.value LIMIT 10;
describe columnShortcutTable;
SELECT columnShortcutTable.* FROM columnShortcutTable ORDER BY key ASC, value ASC;

ALTER TABLE columnShortcutTable ADD COLUMNS (c string);
SELECT columnShortcutTable.* FROM columnShortcutTable ORDER BY key ASC, value ASC;
ALTER TABLE columnShortcutTable REPLACE COLUMNS (key int);
SELECT columnShortcutTable.* FROM columnShortcutTable ORDER BY key ASC;
