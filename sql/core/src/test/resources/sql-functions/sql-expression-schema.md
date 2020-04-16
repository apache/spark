## Summary
  - Number of queries: 324
  - Number of expressions that missing example: 37
  - Expressions missing examples: and,string,tinyint,double,smallint,date,decimal,boolean,float,binary,bigint,int,timestamp,cume_dist,current_date,current_timestamp,now,dense_rank,input_file_block_length,input_file_block_start,input_file_name,lag,lead,monotonically_increasing_id,ntile,struct,!,not,or,percent_rank,rank,row_number,spark_partition_id,version,window,positive,count_min_sketch
## Schema of Built-in Functions
| No | Class name | Function name or alias | Query example | Output schema |
| -- | ---------- | ---------------------- | ------------- | ------------- |
| 1 | org.apache.spark.sql.catalyst.expressions.Abs | abs | SELECT abs(-1) | struct<abs(-1):int> |
| 2 | org.apache.spark.sql.catalyst.expressions.Acos | acos | SELECT acos(1) | struct<ACOS(CAST(1 AS DOUBLE)):double> |
| 3 | org.apache.spark.sql.catalyst.expressions.Acosh | acosh | SELECT acosh(1) | struct<ACOSH(CAST(1 AS DOUBLE)):double> |
| 4 | org.apache.spark.sql.catalyst.expressions.Add | + | SELECT 1 + 2 | struct<(1 + 2):int> |
| 5 | org.apache.spark.sql.catalyst.expressions.AddMonths | add_months | SELECT add_months('2016-08-31', 1) | struct<add_months(CAST(2016-08-31 AS DATE), 1):date> |
| 6 | org.apache.spark.sql.catalyst.expressions.And | and | Example is missing | Example is missing |
| 7 | org.apache.spark.sql.catalyst.expressions.ArrayAggregate | aggregate | SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) | struct<aggregate(array(1, 2, 3), 0, lambdafunction((namedlambdavariable() + namedlambdavariable()), namedlambdavariable(), namedlambdavariable()), lambdafunction(namedlambdavariable(), namedlambdavariable())):int> |
| 8 | org.apache.spark.sql.catalyst.expressions.ArrayContains | array_contains | SELECT array_contains(array(1, 2, 3), 2) | struct<array_contains(array(1, 2, 3), 2):boolean> |
| 9 | org.apache.spark.sql.catalyst.expressions.ArrayDistinct | array_distinct | SELECT array_distinct(array(1, 2, 3, null, 3)) | struct<array_distinct(array(1, 2, 3, CAST(NULL AS INT), 3)):array<int>> |
| 10 | org.apache.spark.sql.catalyst.expressions.ArrayExcept | array_except | SELECT array_except(array(1, 2, 3), array(1, 3, 5)) | struct<array_except(array(1, 2, 3), array(1, 3, 5)):array<int>> |
| 11 | org.apache.spark.sql.catalyst.expressions.ArrayExists | exists | SELECT exists(array(1, 2, 3), x -> x % 2 == 0) | struct<exists(array(1, 2, 3), lambdafunction(((namedlambdavariable() % 2) = 0), namedlambdavariable())):boolean> |
| 12 | org.apache.spark.sql.catalyst.expressions.ArrayFilter | filter | SELECT filter(array(1, 2, 3), x -> x % 2 == 1) | struct<filter(array(1, 2, 3), lambdafunction(((namedlambdavariable() % 2) = 1), namedlambdavariable())):array<int>> |
| 13 | org.apache.spark.sql.catalyst.expressions.ArrayForAll | forall | SELECT forall(array(1, 2, 3), x -> x % 2 == 0) | struct<forall(array(1, 2, 3), lambdafunction(((namedlambdavariable() % 2) = 0), namedlambdavariable())):boolean> |
| 14 | org.apache.spark.sql.catalyst.expressions.ArrayIntersect | array_intersect | SELECT array_intersect(array(1, 2, 3), array(1, 3, 5)) | struct<array_intersect(array(1, 2, 3), array(1, 3, 5)):array<int>> |
| 15 | org.apache.spark.sql.catalyst.expressions.ArrayJoin | array_join | SELECT array_join(array('hello', 'world'), ' ') | struct<array_join(array(hello, world),  ):string> |
| 16 | org.apache.spark.sql.catalyst.expressions.ArrayMax | array_max | SELECT array_max(array(1, 20, null, 3)) | struct<array_max(array(1, 20, CAST(NULL AS INT), 3)):int> |
| 17 | org.apache.spark.sql.catalyst.expressions.ArrayMin | array_min | SELECT array_min(array(1, 20, null, 3)) | struct<array_min(array(1, 20, CAST(NULL AS INT), 3)):int> |
| 18 | org.apache.spark.sql.catalyst.expressions.ArrayPosition | array_position | SELECT array_position(array(3, 2, 1), 1) | struct<array_position(array(3, 2, 1), 1):bigint> |
| 19 | org.apache.spark.sql.catalyst.expressions.ArrayRemove | array_remove | SELECT array_remove(array(1, 2, 3, null, 3), 3) | struct<array_remove(array(1, 2, 3, CAST(NULL AS INT), 3), 3):array<int>> |
| 20 | org.apache.spark.sql.catalyst.expressions.ArrayRepeat | array_repeat | SELECT array_repeat('123', 2) | struct<array_repeat(123, 2):array<string>> |
| 21 | org.apache.spark.sql.catalyst.expressions.ArraySort | array_sort | SELECT array_sort(array(5, 6, 1), (left, right) -> case when left < right then -1 when left > right then 1 else 0 end) | struct<array_sort(array(5, 6, 1), lambdafunction(CASE WHEN (namedlambdavariable() < namedlambdavariable()) THEN -1 WHEN (namedlambdavariable() > namedlambdavariable()) THEN 1 ELSE 0 END, namedlambdavariable(), namedlambdavariable())):array<int>> |
| 22 | org.apache.spark.sql.catalyst.expressions.ArrayTransform | transform | SELECT transform(array(1, 2, 3), x -> x + 1) | struct<transform(array(1, 2, 3), lambdafunction((namedlambdavariable() + 1), namedlambdavariable())):array<int>> |
| 23 | org.apache.spark.sql.catalyst.expressions.ArrayUnion | array_union | SELECT array_union(array(1, 2, 3), array(1, 3, 5)) | struct<array_union(array(1, 2, 3), array(1, 3, 5)):array<int>> |
| 24 | org.apache.spark.sql.catalyst.expressions.ArraysOverlap | arrays_overlap | SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5)) | struct<arrays_overlap(array(1, 2, 3), array(3, 4, 5)):boolean> |
| 25 | org.apache.spark.sql.catalyst.expressions.ArraysZip | arrays_zip | SELECT arrays_zip(array(1, 2, 3), array(2, 3, 4)) | struct<arrays_zip(array(1, 2, 3), array(2, 3, 4)):array<struct<0:int,1:int>>> |
| 26 | org.apache.spark.sql.catalyst.expressions.Ascii | ascii | SELECT ascii('222') | struct<ascii(222):int> |
| 27 | org.apache.spark.sql.catalyst.expressions.Asin | asin | SELECT asin(0) | struct<ASIN(CAST(0 AS DOUBLE)):double> |
| 28 | org.apache.spark.sql.catalyst.expressions.Asinh | asinh | SELECT asinh(0) | struct<ASINH(CAST(0 AS DOUBLE)):double> |
| 29 | org.apache.spark.sql.catalyst.expressions.AssertTrue | assert_true | SELECT assert_true(0 < 1) | struct<assert_true((0 < 1)):null> |
| 30 | org.apache.spark.sql.catalyst.expressions.Atan | atan | SELECT atan(0) | struct<ATAN(CAST(0 AS DOUBLE)):double> |
| 31 | org.apache.spark.sql.catalyst.expressions.Atan2 | atan2 | SELECT atan2(0, 0) | struct<ATAN2(CAST(0 AS DOUBLE), CAST(0 AS DOUBLE)):double> |
| 32 | org.apache.spark.sql.catalyst.expressions.Atanh | atanh | SELECT atanh(0) | struct<ATANH(CAST(0 AS DOUBLE)):double> |
| 33 | org.apache.spark.sql.catalyst.expressions.BRound | bround | SELECT bround(2.5, 0) | struct<bround(2.5, 0):decimal(2,0)> |
| 34 | org.apache.spark.sql.catalyst.expressions.Base64 | base64 | SELECT base64('Spark SQL') | struct<base64(CAST(Spark SQL AS BINARY)):string> |
| 35 | org.apache.spark.sql.catalyst.expressions.Bin | bin | SELECT bin(13) | struct<bin(CAST(13 AS BIGINT)):string> |
| 36 | org.apache.spark.sql.catalyst.expressions.BitLength | bit_length | SELECT bit_length('Spark SQL') | struct<bit_length(Spark SQL):int> |
| 37 | org.apache.spark.sql.catalyst.expressions.BitwiseAnd | & | SELECT 3 & 5 | struct<(3 & 5):int> |
| 38 | org.apache.spark.sql.catalyst.expressions.BitwiseCount | bit_count | SELECT bit_count(0) | struct<bit_count(0):int> |
| 39 | org.apache.spark.sql.catalyst.expressions.BitwiseNot | ~ | SELECT ~ 0 | struct<~0:int> |
| 40 | org.apache.spark.sql.catalyst.expressions.BitwiseOr | | | SELECT 3 | 5 | struct<(3 | 5):int> |
| 41 | org.apache.spark.sql.catalyst.expressions.BitwiseXor | ^ | SELECT 3 ^ 5 | struct<(3 ^ 5):int> |
| 42 | org.apache.spark.sql.catalyst.expressions.CaseWhen | when | SELECT CASE WHEN 1 > 0 THEN 1 WHEN 2 > 0 THEN 2.0 ELSE 1.2 END | struct<CASE WHEN (1 > 0) THEN CAST(1 AS DECIMAL(11,1)) WHEN (2 > 0) THEN CAST(2.0 AS DECIMAL(11,1)) ELSE CAST(1.2 AS DECIMAL(11,1)) END:decimal(11,1)> |
| 43 | org.apache.spark.sql.catalyst.expressions.Cast | string | Example is missing | Example is missing |
| 44 | org.apache.spark.sql.catalyst.expressions.Cast | cast | SELECT cast('10' as int) | struct<CAST(10 AS INT):int> |
| 45 | org.apache.spark.sql.catalyst.expressions.Cast | tinyint | Example is missing | Example is missing |
| 46 | org.apache.spark.sql.catalyst.expressions.Cast | double | Example is missing | Example is missing |
| 47 | org.apache.spark.sql.catalyst.expressions.Cast | smallint | Example is missing | Example is missing |
| 48 | org.apache.spark.sql.catalyst.expressions.Cast | date | Example is missing | Example is missing |
| 49 | org.apache.spark.sql.catalyst.expressions.Cast | decimal | Example is missing | Example is missing |
| 50 | org.apache.spark.sql.catalyst.expressions.Cast | boolean | Example is missing | Example is missing |
| 51 | org.apache.spark.sql.catalyst.expressions.Cast | float | Example is missing | Example is missing |
| 52 | org.apache.spark.sql.catalyst.expressions.Cast | binary | Example is missing | Example is missing |
| 53 | org.apache.spark.sql.catalyst.expressions.Cast | bigint | Example is missing | Example is missing |
| 54 | org.apache.spark.sql.catalyst.expressions.Cast | int | Example is missing | Example is missing |
| 55 | org.apache.spark.sql.catalyst.expressions.Cast | timestamp | Example is missing | Example is missing |
| 56 | org.apache.spark.sql.catalyst.expressions.Cbrt | cbrt | SELECT cbrt(27.0) | struct<CBRT(CAST(27.0 AS DOUBLE)):double> |
| 57 | org.apache.spark.sql.catalyst.expressions.Ceil | ceil | SELECT ceil(-0.1) | struct<CEIL(-0.1):decimal(1,0)> |
| 58 | org.apache.spark.sql.catalyst.expressions.Ceil | ceiling | SELECT ceiling(-0.1) | struct<ceiling(-0.1):decimal(1,0)> |
| 59 | org.apache.spark.sql.catalyst.expressions.Chr | char | SELECT char(65) | struct<char(CAST(65 AS BIGINT)):string> |
| 60 | org.apache.spark.sql.catalyst.expressions.Chr | chr | SELECT chr(65) | struct<chr(CAST(65 AS BIGINT)):string> |
| 61 | org.apache.spark.sql.catalyst.expressions.Coalesce | coalesce | SELECT coalesce(NULL, 1, NULL) | struct<coalesce(CAST(NULL AS INT), 1, CAST(NULL AS INT)):int> |
| 62 | org.apache.spark.sql.catalyst.expressions.Concat | concat | SELECT concat('Spark', 'SQL') | struct<concat(Spark, SQL):string> |
| 63 | org.apache.spark.sql.catalyst.expressions.ConcatWs | concat_ws | SELECT concat_ws(' ', 'Spark', 'SQL') | struct<concat_ws( , Spark, SQL):string> |
| 64 | org.apache.spark.sql.catalyst.expressions.Conv | conv | SELECT conv('100', 2, 10) | struct<conv(100, 2, 10):string> |
| 65 | org.apache.spark.sql.catalyst.expressions.Cos | cos | SELECT cos(0) | struct<COS(CAST(0 AS DOUBLE)):double> |
| 66 | org.apache.spark.sql.catalyst.expressions.Cosh | cosh | SELECT cosh(0) | struct<COSH(CAST(0 AS DOUBLE)):double> |
| 67 | org.apache.spark.sql.catalyst.expressions.Cot | cot | SELECT cot(1) | struct<COT(CAST(1 AS DOUBLE)):double> |
| 68 | org.apache.spark.sql.catalyst.expressions.Crc32 | crc32 | SELECT crc32('Spark') | struct<crc32(CAST(Spark AS BINARY)):bigint> |
| 69 | org.apache.spark.sql.catalyst.expressions.CreateArray | array | SELECT array(1, 2, 3) | struct<array(1, 2, 3):array<int>> |
| 70 | org.apache.spark.sql.catalyst.expressions.CreateMap | map | SELECT map(1.0, '2', 3.0, '4') | struct<map(1.0, 2, 3.0, 4):map<decimal(2,1),string>> |
| 71 | org.apache.spark.sql.catalyst.expressions.CreateNamedStruct | named_struct | SELECT named_struct("a", 1, "b", 2, "c", 3) | struct<named_struct(a, 1, b, 2, c, 3):struct<a:int,b:int,c:int>> |
| 72 | org.apache.spark.sql.catalyst.expressions.CsvToStructs | from_csv | SELECT from_csv('1, 0.8', 'a INT, b DOUBLE') | struct<from_csv(1, 0.8):struct<a:int,b:double>> |
| 73 | org.apache.spark.sql.catalyst.expressions.Cube | cube | SELECT name, age, count(*) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY cube(name, age) | struct<name:string,age:int,count(1):bigint> |
| 74 | org.apache.spark.sql.catalyst.expressions.CumeDist | cume_dist | Example is missing | Example is missing |
| 75 | org.apache.spark.sql.catalyst.expressions.CurrentDatabase | current_database | SELECT current_database() | struct<current_database():string> |
| 76 | org.apache.spark.sql.catalyst.expressions.CurrentDate | current_date | Example is missing | Example is missing |
| 77 | org.apache.spark.sql.catalyst.expressions.CurrentTimestamp | current_timestamp | Example is missing | Example is missing |
| 78 | org.apache.spark.sql.catalyst.expressions.CurrentTimestamp | now | Example is missing | Example is missing |
| 79 | org.apache.spark.sql.catalyst.expressions.DateAdd | date_add | SELECT date_add('2016-07-30', 1) | struct<date_add(CAST(2016-07-30 AS DATE), 1):date> |
| 80 | org.apache.spark.sql.catalyst.expressions.DateDiff | datediff | SELECT datediff('2009-07-31', '2009-07-30') | struct<datediff(CAST(2009-07-31 AS DATE), CAST(2009-07-30 AS DATE)):int> |
| 81 | org.apache.spark.sql.catalyst.expressions.DateFormatClass | date_format | SELECT date_format('2016-04-08', 'y') | struct<date_format(CAST(2016-04-08 AS TIMESTAMP), y):string> |
| 82 | org.apache.spark.sql.catalyst.expressions.DatePart | date_part | SELECT date_part('YEAR', TIMESTAMP '2019-08-12 01:00:00.123456') | struct<date_part('YEAR', TIMESTAMP '2019-08-12 01:00:00.123456'):int> |
| 83 | org.apache.spark.sql.catalyst.expressions.DateSub | date_sub | SELECT date_sub('2016-07-30', 1) | struct<date_sub(CAST(2016-07-30 AS DATE), 1):date> |
| 84 | org.apache.spark.sql.catalyst.expressions.DayOfMonth | day | SELECT day('2009-07-30') | struct<day(CAST(2009-07-30 AS DATE)):int> |
| 85 | org.apache.spark.sql.catalyst.expressions.DayOfMonth | dayofmonth | SELECT dayofmonth('2009-07-30') | struct<dayofmonth(CAST(2009-07-30 AS DATE)):int> |
| 86 | org.apache.spark.sql.catalyst.expressions.DayOfWeek | dayofweek | SELECT dayofweek('2009-07-30') | struct<dayofweek(CAST(2009-07-30 AS DATE)):int> |
| 87 | org.apache.spark.sql.catalyst.expressions.DayOfYear | dayofyear | SELECT dayofyear('2016-04-09') | struct<dayofyear(CAST(2016-04-09 AS DATE)):int> |
| 88 | org.apache.spark.sql.catalyst.expressions.Decode | decode | SELECT decode(encode('abc', 'utf-8'), 'utf-8') | struct<decode(encode(abc, utf-8), utf-8):string> |
| 89 | org.apache.spark.sql.catalyst.expressions.DenseRank | dense_rank | Example is missing | Example is missing |
| 90 | org.apache.spark.sql.catalyst.expressions.Divide | / | SELECT 3 / 2 | struct<(CAST(3 AS DOUBLE) / CAST(2 AS DOUBLE)):double> |
| 91 | org.apache.spark.sql.catalyst.expressions.ElementAt | element_at | SELECT element_at(array(1, 2, 3), 2) | struct<element_at(array(1, 2, 3), 2):int> |
| 92 | org.apache.spark.sql.catalyst.expressions.Elt | elt | SELECT elt(1, 'scala', 'java') | struct<elt(1, scala, java):string> |
| 93 | org.apache.spark.sql.catalyst.expressions.Encode | encode | SELECT encode('abc', 'utf-8') | struct<encode(abc, utf-8):binary> |
| 94 | org.apache.spark.sql.catalyst.expressions.EqualNullSafe | <=> | SELECT 2 <=> 2 | struct<(2 <=> 2):boolean> |
| 95 | org.apache.spark.sql.catalyst.expressions.EqualTo | = | SELECT 2 = 2 | struct<(2 = 2):boolean> |
| 96 | org.apache.spark.sql.catalyst.expressions.EqualTo | == | SELECT 2 == 2 | struct<(2 = 2):boolean> |
| 97 | org.apache.spark.sql.catalyst.expressions.EulerNumber | e | SELECT e() | struct<E():double> |
| 98 | org.apache.spark.sql.catalyst.expressions.Exp | exp | SELECT exp(0) | struct<EXP(CAST(0 AS DOUBLE)):double> |
| 99 | org.apache.spark.sql.catalyst.expressions.Explode | explode | SELECT explode(array(10, 20)) | struct<col:int> |
| 100 | org.apache.spark.sql.catalyst.expressions.Explode | explode_outer | SELECT explode_outer(array(10, 20)) | struct<col:int> |
| 101 | org.apache.spark.sql.catalyst.expressions.Expm1 | expm1 | SELECT expm1(0) | struct<EXPM1(CAST(0 AS DOUBLE)):double> |
| 102 | org.apache.spark.sql.catalyst.expressions.Factorial | factorial | SELECT factorial(5) | struct<factorial(5):bigint> |
| 103 | org.apache.spark.sql.catalyst.expressions.FindInSet | find_in_set | SELECT find_in_set('ab','abc,b,ab,c,def') | struct<find_in_set(ab, abc,b,ab,c,def):int> |
| 104 | org.apache.spark.sql.catalyst.expressions.Flatten | flatten | SELECT flatten(array(array(1, 2), array(3, 4))) | struct<flatten(array(array(1, 2), array(3, 4))):array<int>> |
| 105 | org.apache.spark.sql.catalyst.expressions.Floor | floor | SELECT floor(-0.1) | struct<FLOOR(-0.1):decimal(1,0)> |
| 106 | org.apache.spark.sql.catalyst.expressions.FormatNumber | format_number | SELECT format_number(12332.123456, 4) | struct<format_number(12332.123456, 4):string> |
| 107 | org.apache.spark.sql.catalyst.expressions.FormatString | printf | SELECT printf("Hello World %d %s", 100, "days") | struct<printf(Hello World %d %s, 100, days):string> |
| 108 | org.apache.spark.sql.catalyst.expressions.FormatString | format_string | SELECT format_string("Hello World %d %s", 100, "days") | struct<format_string(Hello World %d %s, 100, days):string> |
| 109 | org.apache.spark.sql.catalyst.expressions.FromUTCTimestamp | from_utc_timestamp | SELECT from_utc_timestamp('2016-08-31', 'Asia/Seoul') | struct<from_utc_timestamp(CAST(2016-08-31 AS TIMESTAMP), Asia/Seoul):timestamp> |
| 110 | org.apache.spark.sql.catalyst.expressions.FromUnixTime | from_unixtime | SELECT from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') | struct<from_unixtime(CAST(0 AS BIGINT), yyyy-MM-dd HH:mm:ss):string> |
| 111 | org.apache.spark.sql.catalyst.expressions.GetJsonObject | get_json_object | SELECT get_json_object('{"a":"b"}', '$.a') | struct<get_json_object({"a":"b"}, $.a):string> |
| 112 | org.apache.spark.sql.catalyst.expressions.GreaterThan | > | SELECT 2 > 1 | struct<(2 > 1):boolean> |
| 113 | org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual | >= | SELECT 2 >= 1 | struct<(2 >= 1):boolean> |
| 114 | org.apache.spark.sql.catalyst.expressions.Greatest | greatest | SELECT greatest(10, 9, 2, 4, 3) | struct<greatest(10, 9, 2, 4, 3):int> |
| 115 | org.apache.spark.sql.catalyst.expressions.Grouping | grouping | SELECT name, grouping(name), sum(age) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY cube(name) | struct<name:string,grouping(name):tinyint,sum(age):bigint> |
| 116 | org.apache.spark.sql.catalyst.expressions.GroupingID | grouping_id | SELECT name, grouping_id(), sum(age), avg(height) FROM VALUES (2, 'Alice', 165), (5, 'Bob', 180) people(age, name, height) GROUP BY cube(name, height) | struct<name:string,grouping_id():bigint,sum(age):bigint,avg(height):double> |
| 117 | org.apache.spark.sql.catalyst.expressions.Hex | hex | SELECT hex(17) | struct<hex(CAST(17 AS BIGINT)):string> |
| 118 | org.apache.spark.sql.catalyst.expressions.Hour | hour | SELECT hour('2009-07-30 12:58:59') | struct<hour(CAST(2009-07-30 12:58:59 AS TIMESTAMP)):int> |
| 119 | org.apache.spark.sql.catalyst.expressions.Hypot | hypot | SELECT hypot(3, 4) | struct<HYPOT(CAST(3 AS DOUBLE), CAST(4 AS DOUBLE)):double> |
| 120 | org.apache.spark.sql.catalyst.expressions.If | if | SELECT if(1 < 2, 'a', 'b') | struct<(IF((1 < 2), a, b)):string> |
| 121 | org.apache.spark.sql.catalyst.expressions.IfNull | ifnull | SELECT ifnull(NULL, array('2')) | struct<ifnull(NULL, array('2')):array<string>> |
| 122 | org.apache.spark.sql.catalyst.expressions.In | in | SELECT 1 in(1, 2, 3) | struct<(1 IN (1, 2, 3)):boolean> |
| 123 | org.apache.spark.sql.catalyst.expressions.InitCap | initcap | SELECT initcap('sPark sql') | struct<initcap(sPark sql):string> |
| 124 | org.apache.spark.sql.catalyst.expressions.Inline | inline | SELECT inline(array(struct(1, 'a'), struct(2, 'b'))) | struct<col1:int,col2:string> |
| 125 | org.apache.spark.sql.catalyst.expressions.Inline | inline_outer | SELECT inline_outer(array(struct(1, 'a'), struct(2, 'b'))) | struct<col1:int,col2:string> |
| 126 | org.apache.spark.sql.catalyst.expressions.InputFileBlockLength | input_file_block_length | Example is missing | Example is missing |
| 127 | org.apache.spark.sql.catalyst.expressions.InputFileBlockStart | input_file_block_start | Example is missing | Example is missing |
| 128 | org.apache.spark.sql.catalyst.expressions.InputFileName | input_file_name | Example is missing | Example is missing |
| 129 | org.apache.spark.sql.catalyst.expressions.IntegralDivide | div | SELECT 3 div 2 | struct<(3 div 2):bigint> |
| 130 | org.apache.spark.sql.catalyst.expressions.IsNaN | isnan | SELECT isnan(cast('NaN' as double)) | struct<isnan(CAST(NaN AS DOUBLE)):boolean> |
| 131 | org.apache.spark.sql.catalyst.expressions.IsNotNull | isnotnull | SELECT isnotnull(1) | struct<(1 IS NOT NULL):boolean> |
| 132 | org.apache.spark.sql.catalyst.expressions.IsNull | isnull | SELECT isnull(1) | struct<(1 IS NULL):boolean> |
| 133 | org.apache.spark.sql.catalyst.expressions.JsonObjectKeys | json_object_keys | Select json_object_keys('{}') | struct<json_object_keys({}):array<string>> |
| 134 | org.apache.spark.sql.catalyst.expressions.JsonToStructs | from_json | SELECT from_json('{"a":1, "b":0.8}', 'a INT, b DOUBLE') | struct<from_json({"a":1, "b":0.8}):struct<a:int,b:double>> |
| 135 | org.apache.spark.sql.catalyst.expressions.JsonTuple | json_tuple | SELECT json_tuple('{"a":1, "b":2}', 'a', 'b') | struct<c0:string,c1:string> |
| 136 | org.apache.spark.sql.catalyst.expressions.Lag | lag | Example is missing | Example is missing |
| 137 | org.apache.spark.sql.catalyst.expressions.LastDay | last_day | SELECT last_day('2009-01-12') | struct<last_day(CAST(2009-01-12 AS DATE)):date> |
| 138 | org.apache.spark.sql.catalyst.expressions.Lead | lead | Example is missing | Example is missing |
| 139 | org.apache.spark.sql.catalyst.expressions.Least | least | SELECT least(10, 9, 2, 4, 3) | struct<least(10, 9, 2, 4, 3):int> |
| 140 | org.apache.spark.sql.catalyst.expressions.Left | left | SELECT left('Spark SQL', 3) | struct<left('Spark SQL', 3):string> |
| 141 | org.apache.spark.sql.catalyst.expressions.Length | character_length | SELECT character_length('Spark SQL ') | struct<character_length(Spark SQL ):int> |
| 142 | org.apache.spark.sql.catalyst.expressions.Length | char_length | SELECT char_length('Spark SQL ') | struct<char_length(Spark SQL ):int> |
| 143 | org.apache.spark.sql.catalyst.expressions.Length | length | SELECT length('Spark SQL ') | struct<length(Spark SQL ):int> |
| 144 | org.apache.spark.sql.catalyst.expressions.LengthOfJsonArray | json_array_length | SELECT json_array_length('[1,2,3,4]') | struct<json_array_length([1,2,3,4]):int> |
| 145 | org.apache.spark.sql.catalyst.expressions.LessThan | < | SELECT 1 < 2 | struct<(1 < 2):boolean> |
| 146 | org.apache.spark.sql.catalyst.expressions.LessThanOrEqual | <= | SELECT 2 <= 2 | struct<(2 <= 2):boolean> |
| 147 | org.apache.spark.sql.catalyst.expressions.Levenshtein | levenshtein | SELECT levenshtein('kitten', 'sitting') | struct<levenshtein(kitten, sitting):int> |
| 148 | org.apache.spark.sql.catalyst.expressions.Like | like | SELECT like('Spark', '_park') | struct<Spark LIKE _park:boolean> |
| 149 | org.apache.spark.sql.catalyst.expressions.Log | ln | SELECT ln(1) | struct<LOG(CAST(1 AS DOUBLE)):double> |
| 150 | org.apache.spark.sql.catalyst.expressions.Log10 | log10 | SELECT log10(10) | struct<LOG10(CAST(10 AS DOUBLE)):double> |
| 151 | org.apache.spark.sql.catalyst.expressions.Log1p | log1p | SELECT log1p(0) | struct<LOG1P(CAST(0 AS DOUBLE)):double> |
| 152 | org.apache.spark.sql.catalyst.expressions.Log2 | log2 | SELECT log2(2) | struct<LOG2(CAST(2 AS DOUBLE)):double> |
| 153 | org.apache.spark.sql.catalyst.expressions.Logarithm | log | SELECT log(10, 100) | struct<LOG(CAST(10 AS DOUBLE), CAST(100 AS DOUBLE)):double> |
| 154 | org.apache.spark.sql.catalyst.expressions.Lower | lcase | SELECT lcase('SparkSql') | struct<lower(SparkSql):string> |
| 155 | org.apache.spark.sql.catalyst.expressions.Lower | lower | SELECT lower('SparkSql') | struct<lower(SparkSql):string> |
| 156 | org.apache.spark.sql.catalyst.expressions.MakeDate | make_date | SELECT make_date(2013, 7, 15) | struct<make_date(2013, 7, 15):date> |
| 157 | org.apache.spark.sql.catalyst.expressions.MakeInterval | make_interval | SELECT make_interval(100, 11, 1, 1, 12, 30, 01.001001) | struct<make_interval(100, 11, 1, 1, 12, 30, CAST(1.001001 AS DECIMAL(8,6))):interval> |
| 158 | org.apache.spark.sql.catalyst.expressions.MakeTimestamp | make_timestamp | SELECT make_timestamp(2014, 12, 28, 6, 30, 45.887) | struct<make_timestamp(2014, 12, 28, 6, 30, CAST(45.887 AS DECIMAL(8,6))):timestamp> |
| 159 | org.apache.spark.sql.catalyst.expressions.MapConcat | map_concat | SELECT map_concat(map(1, 'a', 2, 'b'), map(3, 'c')) | struct<map_concat(map(1, a, 2, b), map(3, c)):map<int,string>> |
| 160 | org.apache.spark.sql.catalyst.expressions.MapEntries | map_entries | SELECT map_entries(map(1, 'a', 2, 'b')) | struct<map_entries(map(1, a, 2, b)):array<struct<key:int,value:string>>> |
| 161 | org.apache.spark.sql.catalyst.expressions.MapFilter | map_filter | SELECT map_filter(map(1, 0, 2, 2, 3, -1), (k, v) -> k > v) | struct<map_filter(map(1, 0, 2, 2, 3, -1), lambdafunction((namedlambdavariable() > namedlambdavariable()), namedlambdavariable(), namedlambdavariable())):map<int,int>> |
| 162 | org.apache.spark.sql.catalyst.expressions.MapFromArrays | map_from_arrays | SELECT map_from_arrays(array(1.0, 3.0), array('2', '4')) | struct<map_from_arrays(array(1.0, 3.0), array(2, 4)):map<decimal(2,1),string>> |
| 163 | org.apache.spark.sql.catalyst.expressions.MapFromEntries | map_from_entries | SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b'))) | struct<map_from_entries(array(named_struct(col1, 1, col2, a), named_struct(col1, 2, col2, b))):map<int,string>> |
| 164 | org.apache.spark.sql.catalyst.expressions.MapKeys | map_keys | SELECT map_keys(map(1, 'a', 2, 'b')) | struct<map_keys(map(1, a, 2, b)):array<int>> |
| 165 | org.apache.spark.sql.catalyst.expressions.MapValues | map_values | SELECT map_values(map(1, 'a', 2, 'b')) | struct<map_values(map(1, a, 2, b)):array<string>> |
| 166 | org.apache.spark.sql.catalyst.expressions.MapZipWith | map_zip_with | SELECT map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2)) | struct<map_zip_with(map(1, a, 2, b), map(1, x, 2, y), lambdafunction(concat(namedlambdavariable(), namedlambdavariable()), namedlambdavariable(), namedlambdavariable(), namedlambdavariable())):map<int,string>> |
| 167 | org.apache.spark.sql.catalyst.expressions.Md5 | md5 | SELECT md5('Spark') | struct<md5(CAST(Spark AS BINARY)):string> |
| 168 | org.apache.spark.sql.catalyst.expressions.Minute | minute | SELECT minute('2009-07-30 12:58:59') | struct<minute(CAST(2009-07-30 12:58:59 AS TIMESTAMP)):int> |
| 169 | org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID | monotonically_increasing_id | Example is missing | Example is missing |
| 170 | org.apache.spark.sql.catalyst.expressions.Month | month | SELECT month('2016-07-30') | struct<month(CAST(2016-07-30 AS DATE)):int> |
| 171 | org.apache.spark.sql.catalyst.expressions.MonthsBetween | months_between | SELECT months_between('1997-02-28 10:30:00', '1996-10-30') | struct<months_between(CAST(1997-02-28 10:30:00 AS TIMESTAMP), CAST(1996-10-30 AS TIMESTAMP), true):double> |
| 172 | org.apache.spark.sql.catalyst.expressions.Multiply | * | SELECT 2 * 3 | struct<(2 * 3):int> |
| 173 | org.apache.spark.sql.catalyst.expressions.Murmur3Hash | hash | SELECT hash('Spark', array(123), 2) | struct<hash(Spark, array(123), 2):int> |
| 174 | org.apache.spark.sql.catalyst.expressions.NTile | ntile | Example is missing | Example is missing |
| 175 | org.apache.spark.sql.catalyst.expressions.NaNvl | nanvl | SELECT nanvl(cast('NaN' as double), 123) | struct<nanvl(CAST(NaN AS DOUBLE), CAST(123 AS DOUBLE)):double> |
| 176 | org.apache.spark.sql.catalyst.expressions.NamedStruct | struct | Example is missing | Example is missing |
| 177 | org.apache.spark.sql.catalyst.expressions.NextDay | next_day | SELECT next_day('2015-01-14', 'TU') | struct<next_day(CAST(2015-01-14 AS DATE), TU):date> |
| 178 | org.apache.spark.sql.catalyst.expressions.Not | ! | Example is missing | Example is missing |
| 179 | org.apache.spark.sql.catalyst.expressions.Not | not | Example is missing | Example is missing |
| 180 | org.apache.spark.sql.catalyst.expressions.NullIf | nullif | SELECT nullif(2, 2) | struct<nullif(2, 2):int> |
| 181 | org.apache.spark.sql.catalyst.expressions.Nvl | nvl | SELECT nvl(NULL, array('2')) | struct<nvl(NULL, array('2')):array<string>> |
| 182 | org.apache.spark.sql.catalyst.expressions.Nvl2 | nvl2 | SELECT nvl2(NULL, 2, 1) | struct<nvl2(NULL, 2, 1):int> |
| 183 | org.apache.spark.sql.catalyst.expressions.OctetLength | octet_length | SELECT octet_length('Spark SQL') | struct<octet_length(Spark SQL):int> |
| 184 | org.apache.spark.sql.catalyst.expressions.Or | or | Example is missing | Example is missing |
| 185 | org.apache.spark.sql.catalyst.expressions.Overlay | overlay | SELECT overlay('Spark SQL' PLACING '_' FROM 6) | struct<overlay(Spark SQL, _, 6, -1):string> |
| 186 | org.apache.spark.sql.catalyst.expressions.ParseToDate | to_date | SELECT to_date('2009-07-30 04:17:52') | struct<to_date('2009-07-30 04:17:52'):date> |
| 187 | org.apache.spark.sql.catalyst.expressions.ParseToTimestamp | to_timestamp | SELECT to_timestamp('2016-12-31 00:12:00') | struct<to_timestamp('2016-12-31 00:12:00'):timestamp> |
| 188 | org.apache.spark.sql.catalyst.expressions.ParseUrl | parse_url | SELECT parse_url('http://spark.apache.org/path?query=1', 'HOST') | struct<parse_url(http://spark.apache.org/path?query=1, HOST):string> |
| 189 | org.apache.spark.sql.catalyst.expressions.PercentRank | percent_rank | Example is missing | Example is missing |
| 190 | org.apache.spark.sql.catalyst.expressions.Pi | pi | SELECT pi() | struct<PI():double> |
| 191 | org.apache.spark.sql.catalyst.expressions.Pmod | pmod | SELECT pmod(10, 3) | struct<pmod(10, 3):int> |
| 192 | org.apache.spark.sql.catalyst.expressions.PosExplode | posexplode_outer | SELECT posexplode_outer(array(10,20)) | struct<pos:int,col:int> |
| 193 | org.apache.spark.sql.catalyst.expressions.PosExplode | posexplode | SELECT posexplode(array(10,20)) | struct<pos:int,col:int> |
| 194 | org.apache.spark.sql.catalyst.expressions.Pow | pow | SELECT pow(2, 3) | struct<pow(CAST(2 AS DOUBLE), CAST(3 AS DOUBLE)):double> |
| 195 | org.apache.spark.sql.catalyst.expressions.Pow | power | SELECT power(2, 3) | struct<POWER(CAST(2 AS DOUBLE), CAST(3 AS DOUBLE)):double> |
| 196 | org.apache.spark.sql.catalyst.expressions.Quarter | quarter | SELECT quarter('2016-08-31') | struct<quarter(CAST(2016-08-31 AS DATE)):int> |
| 197 | org.apache.spark.sql.catalyst.expressions.RLike | rlike | SELECT '%SystemDrive%\Users\John' rlike '%SystemDrive%\\Users.*' | struct<%SystemDrive%UsersJohn RLIKE %SystemDrive%\Users.*:boolean> |
| 198 | org.apache.spark.sql.catalyst.expressions.Rank | rank | Example is missing | Example is missing |
| 199 | org.apache.spark.sql.catalyst.expressions.RegExpExtract | regexp_extract | SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1) | struct<regexp_extract(100-200, (\d+)-(\d+), 1):string> |
| 200 | org.apache.spark.sql.catalyst.expressions.RegExpReplace | regexp_replace | SELECT regexp_replace('100-200', '(\\d+)', 'num') | struct<regexp_replace(100-200, (\d+), num):string> |
| 201 | org.apache.spark.sql.catalyst.expressions.Remainder | % | SELECT 2 % 1.8 | struct<(CAST(CAST(2 AS DECIMAL(1,0)) AS DECIMAL(2,1)) % CAST(1.8 AS DECIMAL(2,1))):decimal(2,1)> |
| 202 | org.apache.spark.sql.catalyst.expressions.Remainder | mod | SELECT 2 % 1.8 | struct<(CAST(CAST(2 AS DECIMAL(1,0)) AS DECIMAL(2,1)) % CAST(1.8 AS DECIMAL(2,1))):decimal(2,1)> |
| 203 | org.apache.spark.sql.catalyst.expressions.Reverse | reverse | SELECT reverse('Spark SQL') | struct<reverse(Spark SQL):string> |
| 204 | org.apache.spark.sql.catalyst.expressions.Right | right | SELECT right('Spark SQL', 3) | struct<right('Spark SQL', 3):string> |
| 205 | org.apache.spark.sql.catalyst.expressions.Rint | rint | SELECT rint(12.3456) | struct<ROUND(CAST(12.3456 AS DOUBLE)):double> |
| 206 | org.apache.spark.sql.catalyst.expressions.Rollup | rollup | SELECT name, age, count(*) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY rollup(name, age) | struct<name:string,age:int,count(1):bigint> |
| 207 | org.apache.spark.sql.catalyst.expressions.Round | round | SELECT round(2.5, 0) | struct<round(2.5, 0):decimal(2,0)> |
| 208 | org.apache.spark.sql.catalyst.expressions.RowNumber | row_number | Example is missing | Example is missing |
| 209 | org.apache.spark.sql.catalyst.expressions.SchemaOfCsv | schema_of_csv | SELECT schema_of_csv('1,abc') | struct<schema_of_csv(1,abc):string> |
| 210 | org.apache.spark.sql.catalyst.expressions.SchemaOfJson | schema_of_json | SELECT schema_of_json('[{"col":0}]') | struct<schema_of_json([{"col":0}]):string> |
| 211 | org.apache.spark.sql.catalyst.expressions.Second | second | SELECT second('2009-07-30 12:58:59') | struct<second(CAST(2009-07-30 12:58:59 AS TIMESTAMP)):int> |
| 212 | org.apache.spark.sql.catalyst.expressions.Sentences | sentences | SELECT sentences('Hi there! Good morning.') | struct<sentences(Hi there! Good morning., , ):array<array<string>>> |
| 213 | org.apache.spark.sql.catalyst.expressions.Sequence | sequence | SELECT sequence(1, 5) | struct<sequence(1, 5):array<int>> |
| 214 | org.apache.spark.sql.catalyst.expressions.Sha1 | sha1 | SELECT sha1('Spark') | struct<sha1(CAST(Spark AS BINARY)):string> |
| 215 | org.apache.spark.sql.catalyst.expressions.Sha1 | sha | SELECT sha('Spark') | struct<sha(CAST(Spark AS BINARY)):string> |
| 216 | org.apache.spark.sql.catalyst.expressions.Sha2 | sha2 | SELECT sha2('Spark', 256) | struct<sha2(CAST(Spark AS BINARY), 256):string> |
| 217 | org.apache.spark.sql.catalyst.expressions.ShiftLeft | shiftleft | SELECT shiftleft(2, 1) | struct<shiftleft(2, 1):int> |
| 218 | org.apache.spark.sql.catalyst.expressions.ShiftRight | shiftright | SELECT shiftright(4, 1) | struct<shiftright(4, 1):int> |
| 219 | org.apache.spark.sql.catalyst.expressions.ShiftRightUnsigned | shiftrightunsigned | SELECT shiftrightunsigned(4, 1) | struct<shiftrightunsigned(4, 1):int> |
| 220 | org.apache.spark.sql.catalyst.expressions.Signum | signum | SELECT signum(40) | struct<SIGNUM(CAST(40 AS DOUBLE)):double> |
| 221 | org.apache.spark.sql.catalyst.expressions.Signum | sign | SELECT sign(40) | struct<sign(CAST(40 AS DOUBLE)):double> |
| 222 | org.apache.spark.sql.catalyst.expressions.Sin | sin | SELECT sin(0) | struct<SIN(CAST(0 AS DOUBLE)):double> |
| 223 | org.apache.spark.sql.catalyst.expressions.Sinh | sinh | SELECT sinh(0) | struct<SINH(CAST(0 AS DOUBLE)):double> |
| 224 | org.apache.spark.sql.catalyst.expressions.Size | size | SELECT size(array('b', 'd', 'c', 'a')) | struct<size(array(b, d, c, a)):int> |
| 225 | org.apache.spark.sql.catalyst.expressions.Size | cardinality | SELECT cardinality(array('b', 'd', 'c', 'a')) | struct<cardinality(array(b, d, c, a)):int> |
| 226 | org.apache.spark.sql.catalyst.expressions.Slice | slice | SELECT slice(array(1, 2, 3, 4), 2, 2) | struct<slice(array(1, 2, 3, 4), 2, 2):array<int>> |
| 227 | org.apache.spark.sql.catalyst.expressions.SortArray | sort_array | SELECT sort_array(array('b', 'd', null, 'c', 'a'), true) | struct<sort_array(array(b, d, CAST(NULL AS STRING), c, a), true):array<string>> |
| 228 | org.apache.spark.sql.catalyst.expressions.SoundEx | soundex | SELECT soundex('Miller') | struct<soundex(Miller):string> |
| 229 | org.apache.spark.sql.catalyst.expressions.SparkPartitionID | spark_partition_id | Example is missing | Example is missing |
| 230 | org.apache.spark.sql.catalyst.expressions.SparkVersion | version | Example is missing | Example is missing |
| 231 | org.apache.spark.sql.catalyst.expressions.Sqrt | sqrt | SELECT sqrt(4) | struct<SQRT(CAST(4 AS DOUBLE)):double> |
| 232 | org.apache.spark.sql.catalyst.expressions.Stack | stack | SELECT stack(2, 1, 2, 3) | struct<col0:int,col1:int> |
| 233 | org.apache.spark.sql.catalyst.expressions.StringInstr | instr | SELECT instr('SparkSQL', 'SQL') | struct<instr(SparkSQL, SQL):int> |
| 234 | org.apache.spark.sql.catalyst.expressions.StringLPad | lpad | SELECT lpad('hi', 5, '??') | struct<lpad(hi, 5, ??):string> |
| 235 | org.apache.spark.sql.catalyst.expressions.StringLocate | position | SELECT position('bar', 'foobarbar') | struct<locate(bar, foobarbar, 1):int> |
| 236 | org.apache.spark.sql.catalyst.expressions.StringLocate | locate | SELECT locate('bar', 'foobarbar') | struct<locate(bar, foobarbar, 1):int> |
| 237 | org.apache.spark.sql.catalyst.expressions.StringRPad | rpad | SELECT rpad('hi', 5, '??') | struct<rpad(hi, 5, ??):string> |
| 238 | org.apache.spark.sql.catalyst.expressions.StringRepeat | repeat | SELECT repeat('123', 2) | struct<repeat(123, 2):string> |
| 239 | org.apache.spark.sql.catalyst.expressions.StringReplace | replace | SELECT replace('ABCabc', 'abc', 'DEF') | struct<replace(ABCabc, abc, DEF):string> |
| 240 | org.apache.spark.sql.catalyst.expressions.StringSpace | space | SELECT concat(space(2), '1') | struct<concat(space(2), 1):string> |
| 241 | org.apache.spark.sql.catalyst.expressions.StringSplit | split | SELECT split('oneAtwoBthreeC', '[ABC]') | struct<split(oneAtwoBthreeC, [ABC], -1):array<string>> |
| 242 | org.apache.spark.sql.catalyst.expressions.StringToMap | str_to_map | SELECT str_to_map('a:1,b:2,c:3', ',', ':') | struct<str_to_map(a:1,b:2,c:3, ,, :):map<string,string>> |
| 243 | org.apache.spark.sql.catalyst.expressions.StringTranslate | translate | SELECT translate('AaBbCc', 'abc', '123') | struct<translate(AaBbCc, abc, 123):string> |
| 244 | org.apache.spark.sql.catalyst.expressions.StringTrim | trim | SELECT trim('    SparkSQL   ') | struct<trim(    SparkSQL   ):string> |
| 245 | org.apache.spark.sql.catalyst.expressions.StringTrimLeft | ltrim | SELECT ltrim('    SparkSQL   ') | struct<ltrim(    SparkSQL   ):string> |
| 246 | org.apache.spark.sql.catalyst.expressions.StringTrimRight | rtrim | SELECT rtrim('    SparkSQL   ') | struct<rtrim(    SparkSQL   ):string> |
| 247 | org.apache.spark.sql.catalyst.expressions.StructsToCsv | to_csv | SELECT to_csv(named_struct('a', 1, 'b', 2)) | struct<to_csv(named_struct(a, 1, b, 2)):string> |
| 248 | org.apache.spark.sql.catalyst.expressions.StructsToJson | to_json | SELECT to_json(named_struct('a', 1, 'b', 2)) | struct<to_json(named_struct(a, 1, b, 2)):string> |
| 249 | org.apache.spark.sql.catalyst.expressions.Substring | substr | SELECT substr('Spark SQL', 5) | struct<substr(Spark SQL, 5, 2147483647):string> |
| 250 | org.apache.spark.sql.catalyst.expressions.Substring | substring | SELECT substring('Spark SQL', 5) | struct<substring(Spark SQL, 5, 2147483647):string> |
| 251 | org.apache.spark.sql.catalyst.expressions.SubstringIndex | substring_index | SELECT substring_index('www.apache.org', '.', 2) | struct<substring_index(www.apache.org, ., 2):string> |
| 252 | org.apache.spark.sql.catalyst.expressions.Subtract | - | SELECT 2 - 1 | struct<(2 - 1):int> |
| 253 | org.apache.spark.sql.catalyst.expressions.Tan | tan | SELECT tan(0) | struct<TAN(CAST(0 AS DOUBLE)):double> |
| 254 | org.apache.spark.sql.catalyst.expressions.Tanh | tanh | SELECT tanh(0) | struct<TANH(CAST(0 AS DOUBLE)):double> |
| 255 | org.apache.spark.sql.catalyst.expressions.TimeWindow | window | Example is missing | Example is missing |
| 256 | org.apache.spark.sql.catalyst.expressions.ToDegrees | degrees | SELECT degrees(3.141592653589793) | struct<DEGREES(CAST(3.141592653589793 AS DOUBLE)):double> |
| 257 | org.apache.spark.sql.catalyst.expressions.ToRadians | radians | SELECT radians(180) | struct<RADIANS(CAST(180 AS DOUBLE)):double> |
| 258 | org.apache.spark.sql.catalyst.expressions.ToUTCTimestamp | to_utc_timestamp | SELECT to_utc_timestamp('2016-08-31', 'Asia/Seoul') | struct<to_utc_timestamp(CAST(2016-08-31 AS TIMESTAMP), Asia/Seoul):timestamp> |
| 259 | org.apache.spark.sql.catalyst.expressions.ToUnixTimestamp | to_unix_timestamp | SELECT to_unix_timestamp('2016-04-08', 'yyyy-MM-dd') | struct<to_unix_timestamp(2016-04-08, yyyy-MM-dd):bigint> |
| 260 | org.apache.spark.sql.catalyst.expressions.TransformKeys | transform_keys | SELECT transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> k + 1) | struct<transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), lambdafunction((namedlambdavariable() + 1), namedlambdavariable(), namedlambdavariable())):map<int,int>> |
| 261 | org.apache.spark.sql.catalyst.expressions.TransformValues | transform_values | SELECT transform_values(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> v + 1) | struct<transform_values(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), lambdafunction((namedlambdavariable() + 1), namedlambdavariable(), namedlambdavariable())):map<int,int>> |
| 262 | org.apache.spark.sql.catalyst.expressions.TruncDate | trunc | SELECT trunc('2019-08-04', 'week') | struct<trunc(CAST(2019-08-04 AS DATE), week):date> |
| 263 | org.apache.spark.sql.catalyst.expressions.TruncTimestamp | date_trunc | SELECT date_trunc('YEAR', '2015-03-05T09:32:05.359') | struct<date_trunc(YEAR, CAST(2015-03-05T09:32:05.359 AS TIMESTAMP)):timestamp> |
| 264 | org.apache.spark.sql.catalyst.expressions.TypeOf | typeof | SELECT typeof(1) | struct<typeof(1):string> |
| 265 | org.apache.spark.sql.catalyst.expressions.UnBase64 | unbase64 | SELECT unbase64('U3BhcmsgU1FM') | struct<unbase64(U3BhcmsgU1FM):binary> |
| 266 | org.apache.spark.sql.catalyst.expressions.UnaryMinus | negative | SELECT negative(1) | struct<(- 1):int> |
| 267 | org.apache.spark.sql.catalyst.expressions.UnaryPositive | positive | Example is missing | Example is missing |
| 268 | org.apache.spark.sql.catalyst.expressions.Unhex | unhex | SELECT decode(unhex('537061726B2053514C'), 'UTF-8') | struct<decode(unhex(537061726B2053514C), UTF-8):string> |
| 269 | org.apache.spark.sql.catalyst.expressions.Upper | ucase | SELECT ucase('SparkSql') | struct<ucase(SparkSql):string> |
| 270 | org.apache.spark.sql.catalyst.expressions.Upper | upper | SELECT upper('SparkSql') | struct<upper(SparkSql):string> |
| 271 | org.apache.spark.sql.catalyst.expressions.WeekDay | weekday | SELECT weekday('2009-07-30') | struct<weekday(CAST(2009-07-30 AS DATE)):int> |
| 272 | org.apache.spark.sql.catalyst.expressions.WeekOfYear | weekofyear | SELECT weekofyear('2008-02-20') | struct<weekofyear(CAST(2008-02-20 AS DATE)):int> |
| 273 | org.apache.spark.sql.catalyst.expressions.XxHash64 | xxhash64 | SELECT xxhash64('Spark', array(123), 2) | struct<xxhash64(Spark, array(123), 2):bigint> |
| 274 | org.apache.spark.sql.catalyst.expressions.Year | year | SELECT year('2016-07-30') | struct<year(CAST(2016-07-30 AS DATE)):int> |
| 275 | org.apache.spark.sql.catalyst.expressions.ZipWith | zip_with | SELECT zip_with(array(1, 2, 3), array('a', 'b', 'c'), (x, y) -> (y, x)) | struct<zip_with(array(1, 2, 3), array(a, b, c), lambdafunction(named_struct(y, namedlambdavariable(), x, namedlambdavariable()), namedlambdavariable(), namedlambdavariable())):array<struct<y:string,x:int>>> |
| 276 | org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile | approx_percentile | SELECT approx_percentile(10.0, array(0.5, 0.4, 0.1), 100) | struct<approx_percentile(10.0, array(0.5, 0.4, 0.1), 100):array<decimal(3,1)>> |
| 277 | org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile | percentile_approx | SELECT percentile_approx(10.0, array(0.5, 0.4, 0.1), 100) | struct<percentile_approx(10.0, array(0.5, 0.4, 0.1), 100):array<decimal(3,1)>> |
| 278 | org.apache.spark.sql.catalyst.expressions.aggregate.Average | avg | SELECT avg(col) FROM VALUES (1), (2), (3) AS tab(col) | struct<avg(col):double> |
| 279 | org.apache.spark.sql.catalyst.expressions.aggregate.Average | mean | SELECT mean(col) FROM VALUES (1), (2), (3) AS tab(col) | struct<mean(col):double> |
| 280 | org.apache.spark.sql.catalyst.expressions.aggregate.BitAndAgg | bit_and | SELECT bit_and(col) FROM VALUES (3), (5) AS tab(col) | struct<bit_and(col):int> |
| 281 | org.apache.spark.sql.catalyst.expressions.aggregate.BitOrAgg | bit_or | SELECT bit_or(col) FROM VALUES (3), (5) AS tab(col) | struct<bit_or(col):int> |
| 282 | org.apache.spark.sql.catalyst.expressions.aggregate.BitXorAgg | bit_xor | SELECT bit_xor(col) FROM VALUES (3), (5) AS tab(col) | struct<bit_xor(col):int> |
| 283 | org.apache.spark.sql.catalyst.expressions.aggregate.BoolAnd | every | SELECT every(col) FROM VALUES (true), (true), (true) AS tab(col) | struct<every(col):boolean> |
| 284 | org.apache.spark.sql.catalyst.expressions.aggregate.BoolAnd | bool_and | SELECT bool_and(col) FROM VALUES (true), (true), (true) AS tab(col) | struct<bool_and(col):boolean> |
| 285 | org.apache.spark.sql.catalyst.expressions.aggregate.BoolOr | bool_or | SELECT bool_or(col) FROM VALUES (true), (false), (false) AS tab(col) | struct<bool_or(col):boolean> |
| 286 | org.apache.spark.sql.catalyst.expressions.aggregate.BoolOr | some | SELECT some(col) FROM VALUES (true), (false), (false) AS tab(col) | struct<some(col):boolean> |
| 287 | org.apache.spark.sql.catalyst.expressions.aggregate.BoolOr | any | SELECT any(col) FROM VALUES (true), (false), (false) AS tab(col) | struct<any(col):boolean> |
| 288 | org.apache.spark.sql.catalyst.expressions.aggregate.CollectList | collect_list | SELECT collect_list(col) FROM VALUES (1), (2), (1) AS tab(col) | struct<collect_list(col):array<int>> |
| 289 | org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet | collect_set | SELECT collect_set(col) FROM VALUES (1), (2), (1) AS tab(col) | struct<collect_set(col):array<int>> |
| 290 | org.apache.spark.sql.catalyst.expressions.aggregate.Corr | corr | SELECT corr(c1, c2) FROM VALUES (3, 2), (3, 3), (6, 4) as tab(c1, c2) | struct<corr(CAST(c1 AS DOUBLE), CAST(c2 AS DOUBLE)):double> |
| 291 | org.apache.spark.sql.catalyst.expressions.aggregate.Count | count | SELECT count(*) FROM VALUES (NULL), (5), (5), (20) AS tab(col) | struct<count(1):bigint> |
| 292 | org.apache.spark.sql.catalyst.expressions.aggregate.CountIf | count_if | SELECT count_if(col % 2 = 0) FROM VALUES (NULL), (0), (1), (2), (3) AS tab(col) | struct<count_if(((col % 2) = 0)):bigint> |
| 293 | org.apache.spark.sql.catalyst.expressions.aggregate.CountMinSketchAgg | count_min_sketch | Example is missing | Example is missing |
| 294 | org.apache.spark.sql.catalyst.expressions.aggregate.CovPopulation | covar_pop | SELECT covar_pop(c1, c2) FROM VALUES (1,1), (2,2), (3,3) AS tab(c1, c2) | struct<covar_pop(CAST(c1 AS DOUBLE), CAST(c2 AS DOUBLE)):double> |
| 295 | org.apache.spark.sql.catalyst.expressions.aggregate.CovSample | covar_samp | SELECT covar_samp(c1, c2) FROM VALUES (1,1), (2,2), (3,3) AS tab(c1, c2) | struct<covar_samp(CAST(c1 AS DOUBLE), CAST(c2 AS DOUBLE)):double> |
| 296 | org.apache.spark.sql.catalyst.expressions.aggregate.First | first_value | SELECT first_value(col) FROM VALUES (10), (5), (20) AS tab(col) | struct<first_value(col, false):int> |
| 297 | org.apache.spark.sql.catalyst.expressions.aggregate.First | first | SELECT first(col) FROM VALUES (10), (5), (20) AS tab(col) | struct<first(col, false):int> |
| 298 | org.apache.spark.sql.catalyst.expressions.aggregate.HyperLogLogPlusPlus | approx_count_distinct | SELECT approx_count_distinct(col1) FROM VALUES (1), (1), (2), (2), (3) tab(col1) | struct<approx_count_distinct(col1):bigint> |
| 299 | org.apache.spark.sql.catalyst.expressions.aggregate.Kurtosis | kurtosis | SELECT kurtosis(col) FROM VALUES (-10), (-20), (100), (1000) AS tab(col) | struct<kurtosis(CAST(col AS DOUBLE)):double> |
| 300 | org.apache.spark.sql.catalyst.expressions.aggregate.Last | last_value | SELECT last_value(col) FROM VALUES (10), (5), (20) AS tab(col) | struct<last_value(col, false):int> |
| 301 | org.apache.spark.sql.catalyst.expressions.aggregate.Last | last | SELECT last(col) FROM VALUES (10), (5), (20) AS tab(col) | struct<last(col, false):int> |
| 302 | org.apache.spark.sql.catalyst.expressions.aggregate.Max | max | SELECT max(col) FROM VALUES (10), (50), (20) AS tab(col) | struct<max(col):int> |
| 303 | org.apache.spark.sql.catalyst.expressions.aggregate.MaxBy | max_by | SELECT max_by(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y) | struct<maxby(x, y):string> |
| 304 | org.apache.spark.sql.catalyst.expressions.aggregate.Min | min | SELECT min(col) FROM VALUES (10), (-1), (20) AS tab(col) | struct<min(col):int> |
| 305 | org.apache.spark.sql.catalyst.expressions.aggregate.MinBy | min_by | SELECT min_by(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y) | struct<minby(x, y):string> |
| 306 | org.apache.spark.sql.catalyst.expressions.aggregate.Percentile | percentile | SELECT percentile(col, 0.3) FROM VALUES (0), (10) AS tab(col) | struct<percentile(col, CAST(0.3 AS DOUBLE), 1):double> |
| 307 | org.apache.spark.sql.catalyst.expressions.aggregate.Skewness | skewness | SELECT skewness(col) FROM VALUES (-10), (-20), (100), (1000) AS tab(col) | struct<skewness(CAST(col AS DOUBLE)):double> |
| 308 | org.apache.spark.sql.catalyst.expressions.aggregate.StddevPop | stddev_pop | SELECT stddev_pop(col) FROM VALUES (1), (2), (3) AS tab(col) | struct<stddev_pop(CAST(col AS DOUBLE)):double> |
| 309 | org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp | stddev_samp | SELECT stddev_samp(col) FROM VALUES (1), (2), (3) AS tab(col) | struct<stddev_samp(CAST(col AS DOUBLE)):double> |
| 310 | org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp | stddev | SELECT stddev(col) FROM VALUES (1), (2), (3) AS tab(col) | struct<stddev(CAST(col AS DOUBLE)):double> |
| 311 | org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp | std | SELECT std(col) FROM VALUES (1), (2), (3) AS tab(col) | struct<std(CAST(col AS DOUBLE)):double> |
| 312 | org.apache.spark.sql.catalyst.expressions.aggregate.Sum | sum | SELECT sum(col) FROM VALUES (5), (10), (15) AS tab(col) | struct<sum(col):bigint> |
| 313 | org.apache.spark.sql.catalyst.expressions.aggregate.VariancePop | var_pop | SELECT var_pop(col) FROM VALUES (1), (2), (3) AS tab(col) | struct<var_pop(CAST(col AS DOUBLE)):double> |
| 314 | org.apache.spark.sql.catalyst.expressions.aggregate.VarianceSamp | var_samp | SELECT var_samp(col) FROM VALUES (1), (2), (3) AS tab(col) | struct<var_samp(CAST(col AS DOUBLE)):double> |
| 315 | org.apache.spark.sql.catalyst.expressions.aggregate.VarianceSamp | variance | SELECT variance(col) FROM VALUES (1), (2), (3) AS tab(col) | struct<variance(CAST(col AS DOUBLE)):double> |
| 316 | org.apache.spark.sql.catalyst.expressions.xml.XPathBoolean | xpath_boolean | SELECT xpath_boolean('<a><b>1</b></a>','a/b') | struct<xpath_boolean(<a><b>1</b></a>, a/b):boolean> |
| 317 | org.apache.spark.sql.catalyst.expressions.xml.XPathDouble | xpath_number | SELECT xpath_number('<a><b>1</b><b>2</b></a>', 'sum(a/b)') | struct<xpath_number(<a><b>1</b><b>2</b></a>, sum(a/b)):double> |
| 318 | org.apache.spark.sql.catalyst.expressions.xml.XPathDouble | xpath_double | SELECT xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)') | struct<xpath_double(<a><b>1</b><b>2</b></a>, sum(a/b)):double> |
| 319 | org.apache.spark.sql.catalyst.expressions.xml.XPathFloat | xpath_float | SELECT xpath_float('<a><b>1</b><b>2</b></a>', 'sum(a/b)') | struct<xpath_float(<a><b>1</b><b>2</b></a>, sum(a/b)):float> |
| 320 | org.apache.spark.sql.catalyst.expressions.xml.XPathInt | xpath_int | SELECT xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)') | struct<xpath_int(<a><b>1</b><b>2</b></a>, sum(a/b)):int> |
| 321 | org.apache.spark.sql.catalyst.expressions.xml.XPathList | xpath | SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>','a/b/text()') | struct<xpath(<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>, a/b/text()):array<string>> |
| 322 | org.apache.spark.sql.catalyst.expressions.xml.XPathLong | xpath_long | SELECT xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)') | struct<xpath_long(<a><b>1</b><b>2</b></a>, sum(a/b)):bigint> |
| 323 | org.apache.spark.sql.catalyst.expressions.xml.XPathShort | xpath_short | SELECT xpath_short('<a><b>1</b><b>2</b></a>', 'sum(a/b)') | struct<xpath_short(<a><b>1</b><b>2</b></a>, sum(a/b)):smallint> |
| 324 | org.apache.spark.sql.catalyst.expressions.xml.XPathString | xpath_string | SELECT xpath_string('<a><b>b</b><c>cc</c></a>','a/c') | struct<xpath_string(<a><b>b</b><c>cc</c></a>, a/c):string> |