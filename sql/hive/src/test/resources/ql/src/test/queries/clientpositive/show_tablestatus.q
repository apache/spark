set hive.support.quoted.identifiers=none;
EXPLAIN 
SHOW TABLE EXTENDED IN default LIKE `src`;

SHOW TABLE EXTENDED IN default LIKE `src`;

SHOW TABLE EXTENDED from default LIKE `src`;

SHOW TABLE EXTENDED LIKE `src`;

SHOW TABLE EXTENDED LIKE `src.?`;

SHOW TABLE EXTENDED from default LIKE `src.?`;

SHOW TABLE EXTENDED LIKE `^s.*`;

SHOW TABLE EXTENDED from default LIKE `^s.*`;

SHOW TABLE EXTENDED LIKE `srcpart` PARTITION(ds='2008-04-08', hr=11);

SHOW TABLE EXTENDED from default LIKE src;