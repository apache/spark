select /*+ MAPJOIN(a) ,MAPJOIN(b)*/ * from src a join src b on (a.key=b.key and a.value=b.value);
