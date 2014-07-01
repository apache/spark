set hive.groupby.orderby.position.alias=true;

-- position alias is not supported when SELECT *
SELECT src.* FROM src ORDER BY 1;
