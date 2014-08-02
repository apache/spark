SELECT src.key FROM src LATERAL VIEW explode(array(1,2,3)) AS myTable JOIN src b ON src.key;
