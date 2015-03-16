set zzz=5;
set zzz;

set system:xxx=5;
set system:xxx;

set system:yyy=${system:xxx};
set system:yyy;

set go=${hiveconf:zzz};
set go;

set hive.variable.substitute=false;
set raw=${hiveconf:zzz};
set raw;

set hive.variable.substitute=true;

EXPLAIN SELECT * FROM src where key=${hiveconf:zzz};
SELECT * FROM src where key=${hiveconf:zzz};

set a=1;
set b=a;
set c=${hiveconf:${hiveconf:b}};
set c;

set jar=${system:maven.local.repository}/org/apache/derby/derby/${system:derby.version}/derby-${system:derby.version}.jar;

add file ${hiveconf:jar};
delete file ${hiveconf:jar};
list file;
