
CREATE TABLE tmp_select(a INT, b STRING);
DESCRIBE tmp_select;

INSERT OVERWRITE TABLE tmp_select SELECT key, value FROM src;

SELECT a, b FROM tmp_select ORDER BY a;


