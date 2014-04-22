-- should fail: hive.map.aggr.hash.min.reduction accepts float type
desc src;

set hive.conf.validation=true;
set hive.map.aggr.hash.min.reduction=false;
