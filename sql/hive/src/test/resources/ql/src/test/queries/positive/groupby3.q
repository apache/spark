FROM src
SELECT sum(substr(src.value,5)), avg(substr(src.value,5)), avg(DISTINCT substr(src.value,5)), max(substr(src.value,5)), min(substr(src.value,5))
