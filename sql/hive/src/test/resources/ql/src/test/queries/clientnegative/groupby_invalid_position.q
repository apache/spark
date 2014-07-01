set hive.groupby.orderby.position.alias=true;

-- invalid position alias in group by
SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY 3;
