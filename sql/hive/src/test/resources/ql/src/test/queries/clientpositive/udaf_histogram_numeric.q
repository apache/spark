
SELECT histogram_numeric(cast(substr(src.value,5) AS double), 2) FROM src;
SELECT histogram_numeric(cast(substr(src.value,5) AS double), 3) FROM src;
SELECT histogram_numeric(cast(substr(src.value,5) AS double), 20) FROM src;
SELECT histogram_numeric(cast(substr(src.value,5) AS double), 200) FROM src;
