set hive.groupby.orderby.position.alias=true;

-- invalid position alias in order by
SELECT src.key, src.value FROM src ORDER BY 0;
