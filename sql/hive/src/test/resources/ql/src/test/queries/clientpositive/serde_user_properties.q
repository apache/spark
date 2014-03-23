-- HIVE-2906 Table properties in SQL

explain extended select key from src;
explain extended select a.key from src a;
explain extended select a.key from src tablesample(1 percent) a;
explain extended select key from src ('user.defined.key'='some.value');
explain extended select key from src ('user.defined.key'='some.value') tablesample(1 percent);
explain extended select a.key from src ('user.defined.key'='some.value') a;
explain extended select a.key from src ('user.defined.key'='some.value') tablesample(1 percent) a;
