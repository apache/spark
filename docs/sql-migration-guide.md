---
layout: global
title: "Migration Guide: SQL, Datasets and DataFrame"
displayTitle: "Migration Guide: SQL, Datasets and DataFrame"
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}

## Upgrading from Spark SQL 3.3 to 3.4
  
  - Since Spark 3.4, Number or Number(\*) from Teradata will be treated as Decimal(38,18). In Spark 3.3 or earlier, Number or Number(\*) from Teradata will be treated as Decimal(38, 0), in which case the fractional part will be removed.
  - Since Spark 3.4, v1 database, table, permanent view and function identifier will include 'spark_catalog' as the catalog name if database is defined, e.g. a table identifier will be: `spark_catalog.default.t`. To restore the legacy behavior, set `spark.sql.legacy.v1IdentifierNoCatalog` to `true`.
  - Since Spark 3.4, when ANSI SQL mode(configuration `spark.sql.ansi.enabled`) is on, Spark SQL always returns NULL result on getting a map value with a non-existing key. In Spark 3.3 or earlier, there will be an error.

## Upgrading from Spark SQL 3.2 to 3.3

  - Since Spark 3.3, the `histogram_numeric` function in Spark SQL returns an output type of an array of structs (x, y), where the type of the 'x' field in the return value is propagated from the input values consumed in the aggregate function. In Spark 3.2 or earlier, 'x' always had double type. Optionally, use the configuration `spark.sql.legacy.histogramNumericPropagateInputType` since Spark 3.3 to revert back to the previous behavior. 

  - Since Spark 3.3, `DayTimeIntervalType` in Spark SQL is mapped to Arrow's `Duration` type in `ArrowWriter` and `ArrowColumnVector` developer APIs. Previously, `DayTimeIntervalType` was mapped to Arrow's `Interval` type which does not match with the types of other languages Spark SQL maps. For example, `DayTimeIntervalType` is mapped to `java.time.Duration` in Java.

  - Since Spark 3.3, the functions `lpad` and `rpad` have been overloaded to support byte sequences. When the first argument is a byte sequence, the optional padding pattern must also be a byte sequence and the result is a BINARY value. The default padding pattern in this case is the zero byte. To restore the legacy behavior of always returning string types, set `spark.sql.legacy.lpadRpadAlwaysReturnString` to `true`.

  - Since Spark 3.3, Spark turns a non-nullable schema into nullable for API `DataFrameReader.schema(schema: StructType).json(jsonDataset: Dataset[String])` and `DataFrameReader.schema(schema: StructType).csv(csvDataset: Dataset[String])` when the schema is specified by the user and contains non-nullable fields. To restore the legacy behavior of respecting the nullability, set `spark.sql.legacy.respectNullabilityInTextDatasetConversion` to `true`.

  - Since Spark 3.3, when the date or timestamp pattern is not specified, Spark converts an input string to a date/timestamp using the `CAST` expression approach. The changes affect CSV/JSON datasources and parsing of partition values. In Spark 3.2 or earlier, when the date or timestamp pattern is not set, Spark uses the default patterns: `yyyy-MM-dd` for dates and `yyyy-MM-dd HH:mm:ss` for timestamps. After the changes, Spark still recognizes the pattern together with
    
    Date patterns:
      * `[+-]yyyy*`
      * `[+-]yyyy*-[m]m`
      * `[+-]yyyy*-[m]m-[d]d`
      * `[+-]yyyy*-[m]m-[d]d `
      * `[+-]yyyy*-[m]m-[d]d *`
      * `[+-]yyyy*-[m]m-[d]dT*`
    
    Timestamp patterns:
      * `[+-]yyyy*`
      * `[+-]yyyy*-[m]m`
      * `[+-]yyyy*-[m]m-[d]d`
      * `[+-]yyyy*-[m]m-[d]d `
      * `[+-]yyyy*-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
      * `[+-]yyyy*-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
      * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
      * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`

  - Since Spark 3.3, the `strfmt` in `format_string(strfmt, obj, ...)` and `printf(strfmt, obj, ...)` will no longer support to use "0$" to specify the first argument, the first argument should always reference by "1$" when use argument index to indicating the position of the argument in the argument list.

  - Since Spark 3.3, nulls are written as empty strings in CSV data source by default. In Spark 3.2 or earlier, nulls were written as empty strings as quoted empty strings, `""`. To restore the previous behavior, set `nullValue` to `""`, or set the configuration `spark.sql.legacy.nullValueWrittenAsQuotedEmptyStringCsv` to `true`.

  - Since Spark 3.3, DESCRIBE FUNCTION fails if the function does not exist. In Spark 3.2 or earlier, DESCRIBE FUNCTION can still run and print "Function: func_name not found".

  - Since Spark 3.3, the table property `external` becomes reserved. Certain commands will fail if you specify the `external` property, such as `CREATE TABLE ... TBLPROPERTIES` and `ALTER TABLE ... SET TBLPROPERTIES`. In Spark 3.2 and earlier, the table property `external` is silently ignored. You can set `spark.sql.legacy.notReserveProperties` to `true` to restore the old behavior.

  - Since Spark 3.3, DROP FUNCTION fails if the function name matches one of the built-in functions' name and is not qualified. In Spark 3.2 or earlier, DROP FUNCTION can still drop a persistent function even if the name is not qualified and is the same as a built-in function's name.

  - Since Spark 3.3, when reading values from a JSON attribute defined as `FloatType` or `DoubleType`, the strings `"+Infinity"`, `"+INF"`, and `"-INF"` are now parsed to the appropriate values, in addition to the already supported `"Infinity"` and `"-Infinity"` variations. This change was made to improve consistency with Jackson's parsing of the unquoted versions of these values. Also, the `allowNonNumericNumbers` option is now respected so these strings will now be considered invalid if this option is disabled.

  - Since Spark 3.3, Spark will try to use built-in data source writer instead of Hive serde in `INSERT OVERWRITE DIRECTORY`. This behavior is effective only if `spark.sql.hive.convertMetastoreParquet` or `spark.sql.hive.convertMetastoreOrc` is enabled respectively for Parquet and ORC formats. To restore the behavior before Spark 3.3, you can set `spark.sql.hive.convertMetastoreInsertDir` to `false`.
  
  - Since Spark 3.3, the precision of the return type of round-like functions has been fixed. This may cause Spark throw `AnalysisException` of the `CANNOT_UP_CAST_DATATYPE` error class when using views created by prior versions. In such cases, you need to recreate the views using ALTER VIEW AS or CREATE OR REPLACE VIEW AS with newer Spark versions.

## Upgrading from Spark SQL 3.1 to 3.2

  - Since Spark 3.2, ADD FILE/JAR/ARCHIVE commands require each path to be enclosed by `"` or `'` if the path contains whitespaces.

  - Since Spark 3.2, all the supported JDBC dialects use StringType for ROWID. In Spark 3.1 or earlier, Oracle dialect uses StringType and the other dialects use LongType.

  - In Spark 3.2, PostgreSQL JDBC dialect uses StringType for MONEY and MONEY[] is not supported due to the JDBC driver for PostgreSQL can't handle those types properly. In Spark 3.1 or earlier, DoubleType and ArrayType of DoubleType are used respectively.

  - In Spark 3.2, `spark.sql.adaptive.enabled` is enabled by default. To restore the behavior before Spark 3.2, you can set `spark.sql.adaptive.enabled` to `false`.

  - In Spark 3.2, the following meta-characters are escaped in the `show()` action. In Spark 3.1 or earlier, the following metacharacters are output as it is.
    * `\n` (new line)
    * `\r` (carrige ret)
    * `\t` (horizontal tab)
    * `\f` (form feed)
    * `\b` (backspace)
    * `\u000B` (vertical tab)
    * `\u0007` (bell)

  - In Spark 3.2, `ALTER TABLE .. RENAME TO PARTITION` throws `PartitionAlreadyExistsException` instead of `AnalysisException` for tables from Hive external when the target partition already exists.

  - In Spark 3.2, script transform default FIELD DELIMIT is `\u0001` for no serde mode, serde property `field.delim` is `\t` for Hive serde mode when user specifies serde. In Spark 3.1 or earlier, the default FIELD DELIMIT is `\t`, serde property `field.delim` is `\u0001` for Hive serde mode when user specifies serde.

  - In Spark 3.2, the auto-generated `Cast` (such as those added by type coercion rules) will be stripped when generating column alias names. E.g., `sql("SELECT floor(1)").columns` will be `FLOOR(1)` instead of `FLOOR(CAST(1 AS DOUBLE))`.
  
  - In Spark 3.2, the output schema of `SHOW TABLES` becomes `namespace: string, tableName: string, isTemporary: boolean`. In Spark 3.1 or earlier, the `namespace` field was named `database` for the builtin catalog, and there is no `isTemporary` field for v2 catalogs. To restore the old schema with the builtin catalog, you can set `spark.sql.legacy.keepCommandOutputSchema` to `true`.
  
  - In Spark 3.2, the output schema of `SHOW TABLE EXTENDED` becomes `namespace: string, tableName: string, isTemporary: boolean, information: string`. In Spark 3.1 or earlier, the `namespace` field was named `database` for the builtin catalog, and no change for the v2 catalogs. To restore the old schema with the builtin catalog, you can set `spark.sql.legacy.keepCommandOutputSchema` to `true`.

  - In Spark 3.2, the output schema of `SHOW TBLPROPERTIES` becomes `key: string, value: string` whether you specify the table property key or not. In Spark 3.1 and earlier, the output schema of `SHOW TBLPROPERTIES` is `value: string` when you specify the table property key. To restore the old schema with the builtin catalog, you can set `spark.sql.legacy.keepCommandOutputSchema` to `true`.

  - In Spark 3.2, the output schema of `DESCRIBE NAMESPACE` becomes `info_name: string, info_value: string`. In Spark 3.1 or earlier, the `info_name` field was named `database_description_item` and the `info_value` field was named `database_description_value` for the builtin catalog. To restore the old schema with the builtin catalog, you can set `spark.sql.legacy.keepCommandOutputSchema` to `true`.

  - In Spark 3.2, table refreshing clears cached data of the table as well as of all its dependents such as views while keeping the dependents cached. The following commands perform table refreshing:
    * `ALTER TABLE .. ADD PARTITION`
    * `ALTER TABLE .. RENAME PARTITION`
    * `ALTER TABLE .. DROP PARTITION`
    * `ALTER TABLE .. RECOVER PARTITIONS`
    * `MSCK REPAIR TABLE`
    * `LOAD DATA`
    * `REFRESH TABLE`
    * `TRUNCATE TABLE`
    * and the method `spark.catalog.refreshTable`
  In Spark 3.1 and earlier, table refreshing leaves dependents uncached.

  - In Spark 3.2, the usage of `count(tblName.*)` is blocked to avoid producing ambiguous results. Because `count(*)` and `count(tblName.*)` will output differently if there is any null values. To restore the behavior before Spark 3.2, you can set `spark.sql.legacy.allowStarWithSingleTableIdentifierInCount` to `true`.

  - In Spark 3.2, we support typed literals in the partition spec of INSERT and ADD/DROP/RENAME PARTITION. For example, `ADD PARTITION(dt = date'2020-01-01')` adds a partition with date value `2020-01-01`. In Spark 3.1 and earlier, the partition value will be parsed as string value `date '2020-01-01'`, which is an illegal date value, and we add a partition with null value at the end.
      
  - In Spark 3.2, `DataFrameNaFunctions.replace()` no longer uses exact string match for the input column names, to match the SQL syntax and support qualified column names. Input column name having a dot in the name (not nested) needs to be escaped with backtick \`. Now, it throws `AnalysisException` if the column is not found in the data frame schema. It also throws `IllegalArgumentException` if the input column name is a nested column. In Spark 3.1 and earlier, it used to ignore invalid input column name and nested column name.

  - In Spark 3.2, the dates subtraction expression such as `date1 - date2` returns values of `DayTimeIntervalType`. In Spark 3.1 and earlier, the returned type is `CalendarIntervalType`. To restore the behavior before Spark 3.2, you can set `spark.sql.legacy.interval.enabled` to `true`.

  - In Spark 3.2, the timestamps subtraction expression such as `timestamp '2021-03-31 23:48:00' - timestamp '2021-01-01 00:00:00'` returns values of `DayTimeIntervalType`. In Spark 3.1 and earlier, the type of the same expression is `CalendarIntervalType`. To restore the behavior before Spark 3.2, you can set `spark.sql.legacy.interval.enabled` to `true`.

  - In Spark 3.2, `CREATE TABLE .. LIKE ..` command can not use reserved properties. You need their specific clauses to specify them, for example, `CREATE TABLE test1 LIKE test LOCATION 'some path'`. You can set `spark.sql.legacy.notReserveProperties` to `true` to ignore the `ParseException`, in this case, these properties will be silently removed, for example: `TBLPROPERTIES('owner'='yao')` will have no effect. In Spark version 3.1 and below, the reserved properties can be used in `CREATE TABLE .. LIKE ..` command but have no side effects, for example, `TBLPROPERTIES('location'='/tmp')` does not change the location of the table but only create a headless property just like `'a'='b'`.

  - In Spark 3.2, `TRANSFORM` operator can't support alias in inputs. In Spark 3.1 and earlier, we can write script transform like `SELECT TRANSFORM(a AS c1, b AS c2) USING 'cat' FROM TBL`.

  - In Spark 3.2, `TRANSFORM` operator can support `ArrayType/MapType/StructType` without Hive SerDe, in this mode, we use `StructsToJosn` to convert `ArrayType/MapType/StructType` column to `STRING` and use `JsonToStructs` to parse `STRING` to `ArrayType/MapType/StructType`. In Spark 3.1, Spark just support case `ArrayType/MapType/StructType` column as `STRING` but can't support parse `STRING` to `ArrayType/MapType/StructType` output columns.

  - In Spark 3.2, the unit-to-unit interval literals like `INTERVAL '1-1' YEAR TO MONTH` and the unit list interval literals like `INTERVAL '3' DAYS '1' HOUR` are converted to ANSI interval types: `YearMonthIntervalType` or `DayTimeIntervalType`. In Spark 3.1 and earlier, such interval literals are converted to `CalendarIntervalType`. To restore the behavior before Spark 3.2, you can set `spark.sql.legacy.interval.enabled` to `true`.

  - In Spark 3.2, the unit list interval literals can not mix year-month fields (YEAR and MONTH) and day-time fields (WEEK, DAY, ..., MICROSECOND). For example, `INTERVAL 1 month 1 hour` is invalid in Spark 3.2. In Spark 3.1 and earlier, there is no such limitation and the literal returns value of `CalendarIntervalType`. To restore the behavior before Spark 3.2, you can set `spark.sql.legacy.interval.enabled` to `true`.

  - In Spark 3.2, Spark supports `DayTimeIntervalType` and `YearMonthIntervalType` as inputs and outputs of `TRANSFORM` clause in Hive `SERDE` mode, the behavior is different between Hive `SERDE` mode and `ROW FORMAT DELIMITED` mode when these two types are used as inputs. In Hive `SERDE` mode, `DayTimeIntervalType` column is converted to `HiveIntervalDayTime`, its string format is `[-]?d h:m:s.n`, but in `ROW FORMAT DELIMITED` mode the format is `INTERVAL '[-]?d h:m:s.n' DAY TO TIME`. In Hive `SERDE` mode, `YearMonthIntervalType` column is converted to `HiveIntervalYearMonth`, its string format is `[-]?y-m`, but in `ROW FORMAT DELIMITED` mode the format is `INTERVAL '[-]?y-m' YEAR TO MONTH`.

  - In Spark 3.2, `hash(0) == hash(-0)` for floating point types. Previously, different values were generated.

  - In Spark 3.2, `CREATE TABLE AS SELECT` with non-empty `LOCATION` will throw `AnalysisException`. To restore the behavior before Spark 3.2, you can set `spark.sql.legacy.allowNonEmptyLocationInCTAS` to `true`.

  - In Spark 3.2, special datetime values such as `epoch`, `today`, `yesterday`, `tomorrow`, and `now` are supported in typed literals or in cast of foldable strings only, for instance, `select timestamp'now'` or `select cast('today' as date)`. In Spark 3.1 and 3.0, such special values are supported in any casts of strings to dates/timestamps. To keep these special values as dates/timestamps in Spark 3.1 and 3.0, you should replace them manually, e.g. `if (c in ('now', 'today'), current_date(), cast(c as date))`.
  
  - In Spark 3.2, `FloatType` is mapped to `FLOAT` in MySQL. Prior to this, it used to be mapped to `REAL`, which is by default a synonym to `DOUBLE PRECISION` in MySQL. 

  - In Spark 3.2, the query executions triggered by `DataFrameWriter` are always named `command` when being sent to `QueryExecutionListener`. In Spark 3.1 and earlier, the name is one of `save`, `insertInto`, `saveAsTable`.
  
  - In Spark 3.2, `Dataset.unionByName` with `allowMissingColumns` set to true will add missing nested fields to the end of structs. In Spark 3.1, nested struct fields are sorted alphabetically.

  - In Spark 3.2, create/alter view will fail if the input query output columns contain auto-generated alias. This is necessary to make sure the query output column names are stable across different spark versions. To restore the behavior before Spark 3.2, set `spark.sql.legacy.allowAutoGeneratedAliasForView` to `true`.

  - In Spark 3.2, date +/- interval with only day-time fields such as `date '2011-11-11' + interval 12 hours` returns timestamp. In Spark 3.1 and earlier, the same expression returns date. To restore the behavior before Spark 3.2, you can use `cast` to convert timestamp as date.

## Upgrading from Spark SQL 3.0 to 3.1

  - In Spark 3.1, statistical aggregation function includes `std`, `stddev`, `stddev_samp`, `variance`, `var_samp`, `skewness`, `kurtosis`, `covar_samp`, `corr` will return `NULL` instead of `Double.NaN` when `DivideByZero` occurs during expression evaluation, for example, when `stddev_samp` applied on a single element set. In Spark version 3.0 and earlier, it will return `Double.NaN` in such case. To restore the behavior before Spark 3.1, you can set `spark.sql.legacy.statisticalAggregate` to `true`.

  - In Spark 3.1, grouping_id() returns long values. In Spark version 3.0 and earlier, this function returns int values. To restore the behavior before Spark 3.1, you can set `spark.sql.legacy.integerGroupingId` to `true`.

  - In Spark 3.1, SQL UI data adopts the `formatted` mode for the query plan explain results. To restore the behavior before Spark 3.1, you can set `spark.sql.ui.explainMode` to `extended`.
  
  - In Spark 3.1, `from_unixtime`, `unix_timestamp`,`to_unix_timestamp`, `to_timestamp` and `to_date` will fail if the specified datetime pattern is invalid. In Spark 3.0 or earlier, they result `NULL`.
  
  - In Spark 3.1, the Parquet, ORC, Avro and JSON datasources throw the exception `org.apache.spark.sql.AnalysisException: Found duplicate column(s) in the data schema` in read if they detect duplicate names in top-level columns as well in nested structures. The datasources take into account the SQL config `spark.sql.caseSensitive` while detecting column name duplicates.

  - In Spark 3.1, structs and maps are wrapped by the `{}` brackets in casting them to strings. For instance, the `show()` action and the `CAST` expression use such brackets. In Spark 3.0 and earlier, the `[]` brackets are used for the same purpose. To restore the behavior before Spark 3.1, you can set `spark.sql.legacy.castComplexTypesToString.enabled` to `true`.

  - In Spark 3.1, NULL elements of structures, arrays and maps are converted to "null" in casting them to strings. In Spark 3.0 or earlier, NULL elements are converted to empty strings. To restore the behavior before Spark 3.1, you can set `spark.sql.legacy.castComplexTypesToString.enabled` to `true`.

  - In Spark 3.1, when `spark.sql.ansi.enabled` is false, Spark always returns null if the sum of decimal type column overflows. In Spark 3.0 or earlier, in the case, the sum of decimal type column may return null or incorrect result, or even fails at runtime (depending on the actual query plan execution).

  - In Spark 3.1, `path` option cannot coexist when the following methods are called with path parameter(s): `DataFrameReader.load()`, `DataFrameWriter.save()`, `DataStreamReader.load()`, or `DataStreamWriter.start()`. In addition, `paths` option cannot coexist for `DataFrameReader.load()`. For example, `spark.read.format("csv").option("path", "/tmp").load("/tmp2")` or `spark.read.option("path", "/tmp").csv("/tmp2")` will throw `org.apache.spark.sql.AnalysisException`. In Spark version 3.0 and below, `path` option is overwritten if one path parameter is passed to above methods; `path` option is added to the overall paths if multiple path parameters are passed to `DataFrameReader.load()`. To restore the behavior before Spark 3.1, you can set `spark.sql.legacy.pathOptionBehavior.enabled` to `true`.

  - In Spark 3.1, `IllegalArgumentException` is returned for the incomplete interval literals, e.g. `INTERVAL '1'`, `INTERVAL '1 DAY 2'`, which are invalid. In Spark 3.0, these literals result in `NULL`s.

  - In Spark 3.1, we remove the built-in Hive 1.2. You need to migrate your custom SerDes to Hive 2.3. See [HIVE-15167](https://issues.apache.org/jira/browse/HIVE-15167) for more details.
  
  - In Spark 3.1, loading and saving of timestamps from/to parquet files fails if the timestamps are before 1900-01-01 00:00:00Z, and loaded (saved) as the INT96 type. In Spark 3.0, the actions don't fail but might lead to shifting of the input timestamps due to rebasing from/to Julian to/from Proleptic Gregorian calendar. To restore the behavior before Spark 3.1, you can set `spark.sql.legacy.parquet.int96RebaseModeInRead` or/and `spark.sql.legacy.parquet.int96RebaseModeInWrite` to `LEGACY`.
  
  - In Spark 3.1, the `schema_of_json` and `schema_of_csv` functions return the schema in the SQL format in which field names are quoted. In Spark 3.0, the function returns a catalog string without field quoting and in lower case. 

  - In Spark 3.1, refreshing a table will trigger an uncache operation for all other caches that reference the table, even if the table itself is not cached. In Spark 3.0 the operation will only be triggered if the table itself is cached.
  
  - In Spark 3.1, creating or altering a permanent view will capture runtime SQL configs and store them as view properties. These configs will be applied during the parsing and analysis phases of the view resolution. To restore the behavior before Spark 3.1, you can set `spark.sql.legacy.useCurrentConfigsForView` to `true`.

  - In Spark 3.1, the temporary view will have same behaviors with the permanent view, i.e. capture and store runtime SQL configs, SQL text, catalog and namespace. The capatured view properties will be applied during the parsing and analysis phases of the view resolution. To restore the behavior before Spark 3.1, you can set `spark.sql.legacy.storeAnalyzedPlanForView` to `true`.

  - In Spark 3.1, temporary view created via `CACHE TABLE ... AS SELECT` will also have the same behavior with permanent view. In particular, when the temporary view is dropped, Spark will invalidate all its cache dependents, as well as the cache for the temporary view itself. This is different from Spark 3.0 and below, which only does the latter. To restore the previous behavior, you can set `spark.sql.legacy.storeAnalyzedPlanForView` to `true`.

  - Since Spark 3.1, CHAR/CHARACTER and VARCHAR types are supported in the table schema. Table scan/insertion will respect the char/varchar semantic. If char/varchar is used in places other than table schema, an exception will be thrown (CAST is an exception that simply treats char/varchar as string like before). To restore the behavior before Spark 3.1, which treats them as STRING types and ignores a length parameter, e.g. `CHAR(4)`, you can set `spark.sql.legacy.charVarcharAsString` to `true`.

  - In Spark 3.1, `AnalysisException` is replaced by its sub-classes that are thrown for tables from Hive external catalog in the following situations:
    * `ALTER TABLE .. ADD PARTITION` throws `PartitionsAlreadyExistException` if new partition exists already
    * `ALTER TABLE .. DROP PARTITION` throws `NoSuchPartitionsException` for not existing partitions

## Upgrading from Spark SQL 3.0.1 to 3.0.2

  - In Spark 3.0.2, `AnalysisException` is replaced by its sub-classes that are thrown for tables from Hive external catalog in the following situations:
    * `ALTER TABLE .. ADD PARTITION` throws `PartitionsAlreadyExistException` if new partition exists already
    * `ALTER TABLE .. DROP PARTITION` throws `NoSuchPartitionsException` for not existing partitions

  - In Spark 3.0.2, `PARTITION(col=null)` is always parsed as a null literal in the partition spec. In Spark 3.0.1 or earlier, it is parsed as a string literal of its text representation, e.g., string "null", if the partition column is string type. To restore the legacy behavior, you can set `spark.sql.legacy.parseNullPartitionSpecAsStringLiteral` as true.

  - In Spark 3.0.2, the output schema of `SHOW DATABASES` becomes `namespace: string`. In Spark version 3.0.1 and earlier, the schema was `databaseName: string`. Since Spark 3.0.2, you can restore the old schema by setting `spark.sql.legacy.keepCommandOutputSchema` to `true`.

## Upgrading from Spark SQL 3.0 to 3.0.1

- In Spark 3.0, JSON datasource and JSON function `schema_of_json` infer TimestampType from string values if they match to the pattern defined by the JSON option `timestampFormat`. Since version 3.0.1, the timestamp type inference is disabled by default. Set the JSON option `inferTimestamp` to `true` to enable such type inference.

- In Spark 3.0, when casting string to integral types(tinyint, smallint, int and bigint), datetime types(date, timestamp and interval) and boolean type, the leading and trailing characters (<= ASCII 32) will be trimmed. For example, `cast('\b1\b' as int)` results `1`. Since Spark 3.0.1, only the leading and trailing whitespace ASCII characters will be trimmed. For example, `cast('\t1\t' as int)` results `1` but `cast('\b1\b' as int)` results `NULL`.

## Upgrading from Spark SQL 2.4 to 3.0

### Dataset/DataFrame APIs

  - In Spark 3.0, the Dataset and DataFrame API `unionAll` is no longer deprecated. It is an alias for `union`.

  - In Spark 2.4 and below, `Dataset.groupByKey` results to a grouped dataset with key attribute is wrongly named as "value", if the key is non-struct type, for example, int, string, array, etc. This is counterintuitive and makes the schema of aggregation queries unexpected. For example, the schema of `ds.groupByKey(...).count()` is `(value, count)`. Since Spark 3.0, we name the grouping attribute to "key". The old behavior is preserved under a newly added configuration `spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue` with a default value of `false`.

  - In Spark 3.0, the column metadata will always be propagated in the API `Column.name` and `Column.as`. In Spark version 2.4 and earlier, the metadata of `NamedExpression` is set as the `explicitMetadata` for the new column at the time the API is called, it won't change even if the underlying `NamedExpression` changes metadata. To restore the behavior before Spark 3.0, you can use the API `as(alias: String, metadata: Metadata)` with explicit metadata.

### DDL Statements

  - In Spark 3.0, when inserting a value into a table column with a different data type, the type coercion is performed as per ANSI SQL standard. Certain unreasonable type conversions such as converting `string` to `int` and `double` to `boolean` are disallowed. A runtime exception is thrown if the value is out-of-range for the data type of the column. In Spark version 2.4 and below, type conversions during table insertion are allowed as long as they are valid `Cast`. When inserting an out-of-range value to an integral field, the low-order bits of the value is inserted(the same as Java/Scala numeric type casting). For example, if 257 is inserted to a field of byte type, the result is 1. The behavior is controlled by the option `spark.sql.storeAssignmentPolicy`, with a default value as "ANSI". Setting the option as "Legacy" restores the previous behavior.

  - The `ADD JAR` command previously returned a result set with the single value 0. It now returns an empty result set.

  - Spark 2.4 and below: the `SET` command works without any warnings even if the specified key is for `SparkConf` entries and it has no effect because the command does not update `SparkConf`, but the behavior might confuse users. In 3.0, the command fails if a `SparkConf` key is used. You can disable such a check by setting `spark.sql.legacy.setCommandRejectsSparkCoreConfs` to `false`.

  - Refreshing a cached table would trigger a table uncache operation and then a table cache (lazily) operation. In Spark version 2.4 and below, the cache name and storage level are not preserved before the uncache operation. Therefore, the cache name and storage level could be changed unexpectedly. In Spark 3.0, cache name and storage level are first preserved for cache recreation. It helps to maintain a consistent cache behavior upon table refreshing.

  - In Spark 3.0, the properties listing below become reserved; commands fail if you specify reserved properties in places like `CREATE DATABASE ... WITH DBPROPERTIES` and `ALTER TABLE ... SET TBLPROPERTIES`. You need their specific clauses to specify them, for example, `CREATE DATABASE test COMMENT 'any comment' LOCATION 'some path'`. You can set `spark.sql.legacy.notReserveProperties` to `true` to ignore the `ParseException`, in this case, these properties will be silently removed, for example: `SET DBPROPERTIES('location'='/tmp')` will have no effect. In Spark version 2.4 and below, these properties are neither reserved nor have side effects, for example, `SET DBPROPERTIES('location'='/tmp')` do not change the location of the database but only create a headless property just like `'a'='b'`.

    | Property (case sensitive) | Database Reserved | Table Reserved | Remarks |
    | ------------------------- | ----------------- | -------------- | ------- |
    | provider                 | no                | yes            | For tables, use the `USING` clause to specify it. Once set, it can't be changed. |
    | location                 | yes               | yes            | For databases and tables, use the `LOCATION` clause to specify it. |
    | owner                    | yes               | yes            | For databases and tables, it is determined by the user who runs spark and create the table. |

 
  - In Spark 3.0, you can use `ADD FILE` to add file directories as well. Earlier you could add only single files using this command. To restore the behavior of earlier versions, set `spark.sql.legacy.addSingleFileInAddFile` to `true`.

  - In Spark 3.0, `SHOW TBLPROPERTIES` throws `AnalysisException` if the table does not exist. In Spark version 2.4 and below, this scenario caused `NoSuchTableException`.

  - In Spark 3.0, `SHOW CREATE TABLE table_identifier` always returns Spark DDL, even when the given table is a Hive SerDe table. For generating Hive DDL, use `SHOW CREATE TABLE table_identifier AS SERDE` command instead.

  - In Spark 3.0, column of CHAR type is not allowed in non-Hive-Serde tables, and CREATE/ALTER TABLE commands will fail if CHAR type is detected. Please use STRING type instead. In Spark version 2.4 and below, CHAR type is treated as STRING type and the length parameter is simply ignored.

### UDFs and Built-in Functions

  - In Spark 3.0, the `date_add` and `date_sub` functions accepts only int, smallint, tinyint as the 2nd argument; fractional and non-literal strings are not valid anymore, for example: `date_add(cast('1964-05-23' as date), '12.34')` causes `AnalysisException`. Note that, string literals are still allowed, but Spark will throw `AnalysisException` if the string content is not a valid integer. In Spark version 2.4 and below, if the 2nd argument is fractional or string value, it is coerced to int value, and the result is a date value of `1964-06-04`.

  - In Spark 3.0, the function `percentile_approx` and its alias `approx_percentile` only accept integral value with range in `[1, 2147483647]` as its 3rd argument `accuracy`, fractional and string types are disallowed, for example, `percentile_approx(10.0, 0.2, 1.8D)` causes `AnalysisException`. In Spark version 2.4 and below, if `accuracy` is fractional or string value, it is coerced to an int value, `percentile_approx(10.0, 0.2, 1.8D)` is operated as `percentile_approx(10.0, 0.2, 1)` which results in `10.0`.

  - In Spark 3.0, an analysis exception is thrown when hash expressions are applied on elements of `MapType`. To restore the behavior before Spark 3.0, set `spark.sql.legacy.allowHashOnMapType` to `true`.

  - In Spark 3.0, when the `array`/`map` function is called without any parameters, it returns an empty collection with `NullType` as element type. In Spark version 2.4 and below, it returns an empty collection with `StringType` as element type. To restore the behavior before Spark 3.0, you can set `spark.sql.legacy.createEmptyCollectionUsingStringType` to `true`.

  - In Spark 3.0, the `from_json` functions supports two modes - `PERMISSIVE` and `FAILFAST`. The modes can be set via the `mode` option. The default mode became `PERMISSIVE`. In previous versions, behavior of `from_json` did not conform to either `PERMISSIVE` nor `FAILFAST`, especially in processing of malformed JSON records. For example, the JSON string `{"a" 1}` with the schema `a INT` is converted to `null` by previous versions but Spark 3.0 converts it to `Row(null)`.

  - In Spark version 2.4 and below, you can create map values with map type key via built-in function such as `CreateMap`, `MapFromArrays`, etc. In Spark 3.0, it's not allowed to create map values with map type key with these built-in functions. Users can use `map_entries` function to convert map to array<struct<key, value>> as a workaround. In addition, users can still read map values with map type key from data source or Java/Scala collections, though it is discouraged.

  - In Spark version 2.4 and below, you can create a map with duplicated keys via built-in functions like `CreateMap`, `StringToMap`, etc. The behavior of map with duplicated keys is undefined, for example, map look up respects the duplicated key appears first, `Dataset.collect` only keeps the duplicated key appears last, `MapKeys` returns duplicated keys, etc. In Spark 3.0, Spark throws `RuntimeException` when duplicated keys are found. You can set `spark.sql.mapKeyDedupPolicy` to `LAST_WIN` to deduplicate map keys with last wins policy. Users may still read map values with duplicated keys from data sources which do not enforce it (for example, Parquet), the behavior is undefined.

  - In Spark 3.0, using `org.apache.spark.sql.functions.udf(AnyRef, DataType)` is not allowed by default. Remove the return type parameter to automatically switch to typed Scala udf is recommended, or set `spark.sql.legacy.allowUntypedScalaUDF` to true to keep using it. In Spark version 2.4 and below, if `org.apache.spark.sql.functions.udf(AnyRef, DataType)` gets a Scala closure with primitive-type argument, the returned UDF returns null if the input values is null. However, in Spark 3.0, the UDF returns the default value of the Java type if the input value is null. For example, `val f = udf((x: Int) => x, IntegerType)`, `f($"x")` returns null in Spark 2.4 and below if column `x` is null, and return 0 in Spark 3.0. This behavior change is introduced because Spark 3.0 is built with Scala 2.12 by default.

  - In Spark 3.0, a higher-order function `exists` follows the three-valued boolean logic, that is, if the `predicate` returns any `null`s and no `true` is obtained, then `exists` returns `null` instead of `false`. For example, `exists(array(1, null, 3), x -> x % 2 == 0)` is `null`. The previous behavior can be restored by setting `spark.sql.legacy.followThreeValuedLogicInArrayExists` to `false`.

  - In Spark 3.0, the `add_months` function does not adjust the resulting date to a last day of month if the original date is a last day of months. For example, `select add_months(DATE'2019-02-28', 1)` results `2019-03-28`. In Spark version 2.4 and below, the resulting date is adjusted when the original date is a last day of months. For example, adding a month to `2019-02-28` results in `2019-03-31`.

  - In Spark version 2.4 and below, the `current_timestamp` function returns a timestamp with millisecond resolution only. In Spark 3.0, the function can return the result with microsecond resolution if the underlying clock available on the system offers such resolution.

  - In Spark 3.0, a 0-argument Java UDF is executed in the executor side identically with other UDFs. In Spark version 2.4 and below, the 0-argument Java UDF alone was executed in the driver side, and the result was propagated to executors, which might be more performant in some cases but caused inconsistency with a correctness issue in some cases.

  - The result of `java.lang.Math`'s `log`, `log1p`, `exp`, `expm1`, and `pow` may vary across platforms. In Spark 3.0, the result of the equivalent SQL functions (including related SQL functions like `LOG10`) return values consistent with `java.lang.StrictMath`. In virtually all cases this makes no difference in the return value, and the difference is very small, but may not exactly match `java.lang.Math` on x86 platforms in cases like, for example, `log(3.0)`, whose value varies between `Math.log()` and `StrictMath.log()`.

  - In Spark 3.0, the `cast` function processes string literals such as 'Infinity', '+Infinity', '-Infinity', 'NaN', 'Inf', '+Inf', '-Inf' in a case-insensitive manner when casting the literals to `Double` or `Float` type to ensure greater compatibility with other database systems. This behavior change is illustrated in the table below:

    | Operation | Result before Spark 3.0 | Result in Spark 3.0 |
    | --------- | ----------------------- | ------------------- |
    | CAST('infinity' AS DOUBLE) | NULL | Double.PositiveInfinity |
    | CAST('+infinity' AS DOUBLE) | NULL | Double.PositiveInfinity |
    | CAST('inf' AS DOUBLE) | NULL | Double.PositiveInfinity |
    | CAST('inf' AS DOUBLE) | NULL | Double.PositiveInfinity |
    | CAST('-infinity' AS DOUBLE) | NULL | Double.NegativeInfinity |
    | CAST('-inf' AS DOUBLE) | NULL | Double.NegativeInfinity |
    | CAST('infinity' AS FLOAT) | NULL | Float.PositiveInfinity |
    | CAST('+infinity' AS FLOAT) | NULL | Float.PositiveInfinity |
    | CAST('inf' AS FLOAT) | NULL | Float.PositiveInfinity |
    | CAST('+inf' AS FLOAT) | NULL | Float.PositiveInfinity |
    | CAST('-infinity' AS FLOAT) | NULL | Float.NegativeInfinity |
    | CAST('-inf' AS FLOAT) | NULL | Float.NegativeInfinity |
    | CAST('nan' AS DOUBLE) | NULL | Double.NaN |
    | CAST('nan' AS FLOAT) | NULL | Float.NaN |

  - In Spark 3.0, when casting interval values to string type, there is no "interval" prefix, for example, `1 days 2 hours`. In Spark version 2.4 and below, the string contains the "interval" prefix like `interval 1 days 2 hours`.

  - In Spark 3.0, when casting string value to integral types(tinyint, smallint, int and bigint), datetime types(date, timestamp and interval) and boolean type, the leading and trailing whitespaces (<= ASCII 32) will be trimmed before converted to these type values, for example, `cast(' 1\t' as int)` results `1`, `cast(' 1\t' as boolean)` results `true`, `cast('2019-10-10\t as date)` results the date value `2019-10-10`. In Spark version 2.4 and below, when casting string to integrals and booleans, it does not trim the whitespaces from both ends; the foregoing results is `null`, while to datetimes, only the trailing spaces (= ASCII 32) are removed.

### Query Engine

  - In Spark version 2.4 and below, SQL queries such as `FROM <table>` or `FROM <table> UNION ALL FROM <table>` are supported by accident. In hive-style `FROM <table> SELECT <expr>`, the `SELECT` clause is not negligible. Neither Hive nor Presto support this syntax. These queries are treated as invalid in Spark 3.0.

  - In Spark 3.0, the interval literal syntax does not allow multiple from-to units anymore. For example, `SELECT INTERVAL '1-1' YEAR TO MONTH '2-2' YEAR TO MONTH'` throws parser exception.

  - In Spark 3.0, numbers written in scientific notation(for example, `1E2`) would be parsed as Double. In Spark version 2.4 and below, they're parsed as Decimal. To restore the behavior before Spark 3.0, you can set `spark.sql.legacy.exponentLiteralAsDecimal.enabled` to `true`.

  - In Spark 3.0, day-time interval strings are converted to intervals with respect to the `from` and `to` bounds. If an input string does not match to the pattern defined by specified bounds, the `ParseException` exception is thrown. For example, `interval '2 10:20' hour to minute` raises the exception because the expected format is `[+|-]h[h]:[m]m`. In Spark version 2.4, the `from` bound was not taken into account, and the `to` bound was used to truncate the resulted interval. For instance, the day-time interval string from the showed example is converted to `interval 10 hours 20 minutes`. To restore the behavior before Spark 3.0, you can set `spark.sql.legacy.fromDayTimeString.enabled` to `true`.

  - In Spark 3.0, negative scale of decimal is not allowed by default, for example, data type of literal like `1E10BD` is `DecimalType(11, 0)`. In Spark version 2.4 and below, it was `DecimalType(2, -9)`. To restore the behavior before Spark 3.0, you can set `spark.sql.legacy.allowNegativeScaleOfDecimal` to `true`.

  - In Spark 3.0, the unary arithmetic operator plus(`+`) only accepts string, numeric and interval type values as inputs. Besides, `+` with an integral string representation is coerced to a double value, for example, `+'1'` returns `1.0`. In Spark version 2.4 and below, this operator is ignored. There is no type checking for it, thus, all type values with a `+` prefix are valid, for example, `+ array(1, 2)` is valid and results `[1, 2]`. Besides, there is no type coercion for it at all, for example, in Spark 2.4, the result of `+'1'` is string `1`.

  - In Spark 3.0, Dataset query fails if it contains ambiguous column reference that is caused by self join. A typical example: `val df1 = ...; val df2 = df1.filter(...);`, then `df1.join(df2, df1("a") > df2("a"))` returns an empty result which is quite confusing. This is because Spark cannot resolve Dataset column references that point to tables being self joined, and `df1("a")` is exactly the same as `df2("a")` in Spark. To restore the behavior before Spark 3.0, you can set `spark.sql.analyzer.failAmbiguousSelfJoin` to `false`.

  - In Spark 3.0, `spark.sql.legacy.ctePrecedencePolicy` is introduced to control the behavior for name conflicting in the nested WITH clause. By default value `EXCEPTION`, Spark throws an AnalysisException, it forces users to choose the specific substitution order they wanted. If set to `CORRECTED` (which is recommended), inner CTE definitions take precedence over outer definitions. For example, set the config to `false`, `WITH t AS (SELECT 1), t2 AS (WITH t AS (SELECT 2) SELECT * FROM t) SELECT * FROM t2` returns `2`, while setting it to `LEGACY`, the result is `1` which is the behavior in version 2.4 and below.

  - In Spark 3.0, configuration `spark.sql.crossJoin.enabled` become internal configuration, and is true by default, so by default spark won't raise exception on sql with implicit cross join.

  - In Spark version 2.4 and below, float/double -0.0 is semantically equal to 0.0, but -0.0 and 0.0 are considered as different values when used in aggregate grouping keys, window partition keys, and join keys. In Spark 3.0, this bug is fixed. For example, `Seq(-0.0, 0.0).toDF("d").groupBy("d").count()` returns `[(0.0, 2)]` in Spark 3.0, and `[(0.0, 1), (-0.0, 1)]` in Spark 2.4 and below.

  - In Spark version 2.4 and below, invalid time zone ids are silently ignored and replaced by GMT time zone, for example, in the from_utc_timestamp function. In Spark 3.0, such time zone ids are rejected, and Spark throws `java.time.DateTimeException`.

  - In Spark 3.0, Proleptic Gregorian calendar is used in parsing, formatting, and converting dates and timestamps as well as in extracting sub-components like years, days and so on. Spark 3.0 uses Java 8 API classes from the `java.time` packages that are based on [ISO chronology](https://docs.oracle.com/javase/8/docs/api/java/time/chrono/IsoChronology.html). In Spark version 2.4 and below, those operations are performed using the hybrid calendar ([Julian + Gregorian](https://docs.oracle.com/javase/7/docs/api/java/util/GregorianCalendar.html). The changes impact on the results for dates before October 15, 1582 (Gregorian) and affect on the following Spark 3.0 API:

    * Parsing/formatting of timestamp/date strings. This effects on CSV/JSON datasources and on the `unix_timestamp`, `date_format`, `to_unix_timestamp`, `from_unixtime`, `to_date`, `to_timestamp` functions when patterns specified by users is used for parsing and formatting. In Spark 3.0, we define our own pattern strings in [Datetime Patterns for Formatting and Parsing](sql-ref-datetime-pattern.html),
     which is implemented via [DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) under the hood. New implementation performs strict checking of its input. For example, the `2015-07-22 10:00:00` timestamp cannot be parse if pattern is `yyyy-MM-dd` because the parser does not consume whole input. Another example is the `31/01/2015 00:00` input cannot be parsed by the `dd/MM/yyyy hh:mm` pattern because `hh` supposes hours in the range `1-12`. In Spark version 2.4 and below, `java.text.SimpleDateFormat` is used for timestamp/date string conversions, and the supported patterns are described in [SimpleDateFormat](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html). The old behavior can be restored by setting `spark.sql.legacy.timeParserPolicy` to `LEGACY`.

    * The `weekofyear`, `weekday`, `dayofweek`, `date_trunc`, `from_utc_timestamp`, `to_utc_timestamp`, and `unix_timestamp` functions use java.time API for calculation week number of year, day number of week as well for conversion from/to TimestampType values in UTC time zone.

    * The JDBC options `lowerBound` and `upperBound` are converted to TimestampType/DateType values in the same way as casting strings to TimestampType/DateType values. The conversion is based on Proleptic Gregorian calendar, and time zone defined by the SQL config `spark.sql.session.timeZone`. In Spark version 2.4 and below, the conversion is based on the hybrid calendar (Julian + Gregorian) and on default system time zone.

    * Formatting `TIMESTAMP` and `DATE` literals.

    * Creating typed `TIMESTAMP` and `DATE` literals from strings. In Spark 3.0, string conversion to typed `TIMESTAMP`/`DATE` literals is performed via casting to `TIMESTAMP`/`DATE` values. For example, `TIMESTAMP '2019-12-23 12:59:30'` is semantically equal to `CAST('2019-12-23 12:59:30' AS TIMESTAMP)`. When the input string does not contain information about time zone, the time zone from the SQL config `spark.sql.session.timeZone` is used in that case. In Spark version 2.4 and below, the conversion is based on JVM system time zone. The different sources of the default time zone may change the behavior of typed `TIMESTAMP` and `DATE` literals.

  - In Spark 3.0, `TIMESTAMP` literals are converted to strings using the SQL config `spark.sql.session.timeZone`. In Spark version 2.4 and below, the conversion uses the default time zone of the Java virtual machine.

  - In Spark 3.0, Spark casts `String` to `Date/Timestamp` in binary comparisons with dates/timestamps. The previous behavior of casting `Date/Timestamp` to `String` can be restored by setting `spark.sql.legacy.typeCoercion.datetimeToString.enabled` to `true`.

  - In Spark 3.0, special values are supported in conversion from strings to dates and timestamps. Those values are simply notational shorthands that are converted to ordinary date or timestamp values when read. The following string values are supported for dates:
    * `epoch [zoneId]` - 1970-01-01
    * `today [zoneId]` - the current date in the time zone specified by `spark.sql.session.timeZone`
    * `yesterday [zoneId]` - the current date - 1
    * `tomorrow [zoneId]` - the current date + 1
    * `now` - the date of running the current query. It has the same notion as today

    For example `SELECT date 'tomorrow' - date 'yesterday';` should output `2`. Here are special timestamp values:
    * `epoch [zoneId]` - 1970-01-01 00:00:00+00 (Unix system time zero)
    * `today [zoneId]` - midnight today
    * `yesterday [zoneId]` - midnight yesterday
    * `tomorrow [zoneId]` - midnight tomorrow
    * `now` - current query start time

    For example `SELECT timestamp 'tomorrow';`.

  - Since Spark 3.0, when using `EXTRACT` expression to extract the second field from date/timestamp values, the result will be a `DecimalType(8, 6)` value with 2 digits for second part, and 6 digits for the fractional part with microsecond precision. e.g. `extract(second from to_timestamp('2019-09-20 10:10:10.1'))` results `10.100000`.  In Spark version 2.4 and earlier, it returns an `IntegerType` value and the result for the former example is `10`.

  - In Spark 3.0, datetime pattern letter `F` is **aligned day of week in month** that represents the concept of the count of days within the period of a week where the weeks are aligned to the start of the month. In Spark version 2.4 and earlier, it is **week of month** that represents the concept of the count of weeks within the month where weeks start on a fixed day-of-week, e.g. `2020-07-30` is 30 days (4 weeks and 2 days) after the first day of the month, so `date_format(date '2020-07-30', 'F')` returns 2 in Spark 3.0, but as a week count in Spark 2.x, it returns 5 because it locates in the 5th week of July 2020, where week one is 2020-07-01 to 07-04.

  - In Spark 3.0, Spark will try to use built-in data source writer instead of Hive serde in `CTAS`. This behavior is effective only if `spark.sql.hive.convertMetastoreParquet` or `spark.sql.hive.convertMetastoreOrc` is enabled respectively for Parquet and ORC formats. To restore the behavior before Spark 3.0, you can set `spark.sql.hive.convertMetastoreCtas` to `false`.

  - In Spark 3.0, Spark will try to use built-in data source writer instead of Hive serde to process inserting into partitioned ORC/Parquet tables created by using the HiveSQL syntax. This behavior is effective only if `spark.sql.hive.convertMetastoreParquet` or `spark.sql.hive.convertMetastoreOrc` is enabled respectively for Parquet and ORC formats. To restore the behavior before Spark 3.0, you can set `spark.sql.hive.convertInsertingPartitionedTable` to `false`.

### Data Sources

  - In Spark version 2.4 and below, when reading a Hive SerDe table with Spark native data sources(parquet/orc), Spark infers the actual file schema and update the table schema in metastore. In Spark 3.0, Spark doesn't infer the schema anymore. This should not cause any problems to end users, but if it does, set `spark.sql.hive.caseSensitiveInferenceMode` to `INFER_AND_SAVE`.

  - In Spark version 2.4 and below, partition column value is converted as null if it can't be casted to corresponding user provided schema. In 3.0, partition column value is validated with user provided schema. An exception is thrown if the validation fails. You can disable such validation by setting  `spark.sql.sources.validatePartitionColumns` to `false`.

  - In Spark 3.0, if files or subdirectories disappear during recursive directory listing (that is, they appear in an intermediate listing but then cannot be read or listed during later phases of the recursive directory listing, due to either concurrent file deletions or object store consistency issues) then the listing will fail with an exception unless `spark.sql.files.ignoreMissingFiles` is `true` (default `false`). In previous versions, these missing files or subdirectories would be ignored. Note that this change of behavior only applies during initial table file listing (or during `REFRESH TABLE`), not during query execution: the net change is that `spark.sql.files.ignoreMissingFiles` is now obeyed during table file listing / query planning, not only at query execution time.

  - In Spark version 2.4 and below, the parser of JSON data source treats empty strings as null for some data types such as `IntegerType`. For `FloatType`, `DoubleType`, `DateType` and `TimestampType`, it fails on empty strings and throws exceptions. Spark 3.0 disallows empty strings and will throw an exception for data types except for `StringType` and `BinaryType`. The previous behavior of allowing an empty string can be restored by setting `spark.sql.legacy.json.allowEmptyString.enabled` to `true`.

  - In Spark version 2.4 and below, JSON datasource and JSON functions like `from_json` convert a bad JSON record to a row with all `null`s in the PERMISSIVE mode when specified schema is `StructType`. In Spark 3.0, the returned row can contain non-`null` fields if some of JSON column values were parsed and converted to desired types successfully.

  - In Spark 3.0, JSON datasource and JSON function `schema_of_json` infer TimestampType from string values if they match to the pattern defined by the JSON option `timestampFormat`. Set JSON option `inferTimestamp` to `false` to disable such type inference.

  - In Spark version 2.4 and below, CSV datasource converts a malformed CSV string to a row with all `null`s in the PERMISSIVE mode. In Spark 3.0, the returned row can contain non-`null` fields if some of CSV column values were parsed and converted to desired types successfully.

  - In Spark 3.0, when Avro files are written with user provided schema, the fields are matched by field names between catalyst schema and Avro schema instead of positions.

  - In Spark 3.0, when Avro files are written with user provided non-nullable schema, even the catalyst schema is nullable, Spark is still able to write the files. However, Spark throws runtime NullPointerException if any of the records contains null.

  - In Spark version 2.4 and below, CSV datasource can detect encoding of input files automatically when the files have BOM at the beginning. For instance, CSV datasource can recognize UTF-8, UTF-16BE, UTF-16LE, UTF-32BE and UTF-32LE in the multi-line mode (the CSV option `multiLine` is set to `true`). In Spark 3.0, CSV datasource reads input files in encoding specified via the CSV option `encoding` which has the default value of UTF-8. In this way, if file encoding doesn't match to the encoding specified via the CSV option, Spark loads the file incorrectly. To solve the issue, users should either set correct encoding via the CSV option `encoding` or set the option to `null` which fallbacks to encoding auto-detection as in Spark versions before 3.0. 

### Others

  - In Spark version 2.4, when a Spark session is created via `cloneSession()`, the newly created Spark session inherits its configuration from its parent `SparkContext` even though the same configuration may exist with a different value in its parent Spark session. In Spark 3.0, the configurations of a parent `SparkSession` have a higher precedence over the parent `SparkContext`. You can restore the old behavior by setting `spark.sql.legacy.sessionInitWithConfigDefaults` to `true`.

  - In Spark 3.0, if `hive.default.fileformat` is not found in `Spark SQL configuration` then it falls back to the `hive-site.xml` file present in the `Hadoop configuration` of `SparkContext`.

  - In Spark 3.0, we pad decimal numbers with trailing zeros to the scale of the column for `spark-sql` interface, for example:

    | Query | Spark 2.4 | Spark 3.0 |
    | ----- | --------- | --------- |
    |`SELECT CAST(1 AS decimal(38, 18));` | 1 | 1.000000000000000000 |

  - In Spark 3.0, we upgraded the built-in Hive from 1.2 to 2.3 and it brings following impacts:

    * You may need to set `spark.sql.hive.metastore.version` and `spark.sql.hive.metastore.jars` according to the version of the Hive metastore you want to connect to. For example: set `spark.sql.hive.metastore.version` to `1.2.1` and `spark.sql.hive.metastore.jars` to `maven` if your Hive metastore version is 1.2.1.

    * You need to migrate your custom SerDes to Hive 2.3 or build your own Spark with `hive-1.2` profile. See [HIVE-15167](https://issues.apache.org/jira/browse/HIVE-15167) for more details.

    * The decimal string representation can be different between Hive 1.2 and Hive 2.3 when using `TRANSFORM` operator in SQL for script transformation, which depends on hive's behavior. In Hive 1.2, the string representation omits trailing zeroes. But in Hive 2.3, it is always padded to 18 digits with trailing zeroes if necessary.

## Upgrading from Spark SQL 2.4.7 to 2.4.8

  - In Spark 2.4.8, `AnalysisException` is replaced by its sub-classes that are thrown for tables from Hive external catalog in the following situations:
    * `ALTER TABLE .. ADD PARTITION` throws `PartitionsAlreadyExistException` if new partition exists already
    * `ALTER TABLE .. DROP PARTITION` throws `NoSuchPartitionsException` for not existing partitions
    
## Upgrading from Spark SQL 2.4.5 to 2.4.6

  - In Spark 2.4.6, the `RESET` command does not reset the static SQL configuration values to the default. It only clears the runtime SQL configuration values.

## Upgrading from Spark SQL 2.4.4 to 2.4.5

  - Since Spark 2.4.5, `TRUNCATE TABLE` command tries to set back original permission and ACLs during re-creating the table/partition paths. To restore the behaviour of earlier versions, set `spark.sql.truncateTable.ignorePermissionAcl.enabled` to `true`.

  - Since Spark 2.4.5, `spark.sql.legacy.mssqlserver.numericMapping.enabled` configuration is added in order to support the legacy MsSQLServer dialect mapping behavior using IntegerType and DoubleType for SMALLINT and REAL JDBC types, respectively. To restore the behaviour of 2.4.3 and earlier versions, set `spark.sql.legacy.mssqlserver.numericMapping.enabled` to `true`.

## Upgrading from Spark SQL 2.4.3 to 2.4.4

  - Since Spark 2.4.4, according to [MsSqlServer Guide](https://docs.microsoft.com/en-us/sql/connect/jdbc/using-basic-data-types?view=sql-server-2017), MsSQLServer JDBC Dialect uses ShortType and FloatType for SMALLINT and REAL, respectively. Previously, IntegerType and DoubleType is used.

## Upgrading from Spark SQL 2.4 to 2.4.1

  - The value of `spark.executor.heartbeatInterval`, when specified without units like "30" rather than "30s", was
    inconsistently interpreted as both seconds and milliseconds in Spark 2.4.0 in different parts of the code.
    Unitless values are now consistently interpreted as milliseconds. Applications that set values like "30"
    need to specify a value with units like "30s" now, to avoid being interpreted as milliseconds; otherwise,
    the extremely short interval that results will likely cause applications to fail.

  - When turning a Dataset to another Dataset, Spark will up cast the fields in the original Dataset to the type of corresponding fields in the target DataSet. In version 2.4 and earlier, this up cast is not very strict, e.g. `Seq("str").toDS.as[Int]` fails, but `Seq("str").toDS.as[Boolean]` works and throw NPE during execution. In Spark 3.0, the up cast is stricter and turning String into something else is not allowed, i.e. `Seq("str").toDS.as[Boolean]` will fail during analysis. To restore the behavior before 2.4.1, set `spark.sql.legacy.looseUpcast` to `true`.

## Upgrading from Spark SQL 2.3 to 2.4

  - In Spark version 2.3 and earlier, the second parameter to array_contains function is implicitly promoted to the element type of first array type parameter. This type promotion can be lossy and may cause `array_contains` function to return wrong result. This problem has been addressed in 2.4 by employing a safer type promotion mechanism. This can cause some change in behavior and are illustrated in the table below.
    <table class="table">
        <tr>
          <th>
            <b>Query</b>
          </th>
          <th>
            <b>Spark 2.3 or Prior</b>
          </th>
          <th>
            <b>Spark 2.4</b>
          </th>
          <th>
            <b>Remarks</b>
          </th>
        </tr>
        <tr>
          <td>
            <code>SELECT array_contains(array(1), 1.34D);</code>
          </td>
          <td>
            <code>true</code>
          </td>
          <td>
            <code>false</code>
          </td>
          <td>
            In Spark 2.4, left and right parameters are promoted to array type of double type and double type respectively.
          </td>
        </tr>
        <tr>
          <td>
            <code>SELECT array_contains(array(1), '1');</code>
          </td>
          <td>
            <code>true</code>
          </td>
          <td>
            <code>AnalysisException</code> is thrown.
          </td>
          <td>
            Explicit cast can be used in arguments to avoid the exception. In Spark 2.4, <code>AnalysisException</code> is thrown since integer type can not be promoted to string type in a loss-less manner.
          </td>
        </tr>
        <tr>
          <td>
            <code>SELECT array_contains(array(1), 'anystring');</code>
          </td>
          <td>
            <code>null</code>
          </td>
          <td>
            <code>AnalysisException</code> is thrown.
          </td>
          <td>
            Explicit cast can be used in arguments to avoid the exception. In Spark 2.4, <code>AnalysisException</code> is thrown since integer type can not be promoted to string type in a loss-less manner.
          </td>
        </tr>
    </table>

  - Since Spark 2.4, when there is a struct field in front of the IN operator before a subquery, the inner query must contain a struct field as well. In previous versions, instead, the fields of the struct were compared to the output of the inner query. For example, if `a` is a `struct(a string, b int)`, in Spark 2.4 `a in (select (1 as a, 'a' as b) from range(1))` is a valid query, while `a in (select 1, 'a' from range(1))` is not. In previous version it was the opposite.

  - In versions 2.2.1+ and 2.3, if `spark.sql.caseSensitive` is set to true, then the `CURRENT_DATE` and `CURRENT_TIMESTAMP` functions incorrectly became case-sensitive and would resolve to columns (unless typed in lower case). In Spark 2.4 this has been fixed and the functions are no longer case-sensitive.

  - Since Spark 2.4, Spark will evaluate the set operations referenced in a query by following a precedence rule as per the SQL standard. If the order is not specified by parentheses, set operations are performed from left to right with the exception that all INTERSECT operations are performed before any UNION, EXCEPT or MINUS operations. The old behaviour of giving equal precedence to all the set operations are preserved under a newly added configuration `spark.sql.legacy.setopsPrecedence.enabled` with a default value of `false`. When this property is set to `true`, spark will evaluate the set operators from left to right as they appear in the query given no explicit ordering is enforced by usage of parenthesis.

  - Since Spark 2.4, Spark will display table description column Last Access value as UNKNOWN when the value was Jan 01 1970.

  - Since Spark 2.4, Spark maximizes the usage of a vectorized ORC reader for ORC files by default. To do that, `spark.sql.orc.impl` and `spark.sql.orc.filterPushdown` change their default values to `native` and `true` respectively. ORC files created by native ORC writer cannot be read by some old Apache Hive releases. Use `spark.sql.orc.impl=hive` to create the files shared with Hive 2.1.1 and older.

  - Since Spark 2.4, writing an empty dataframe to a directory launches at least one write task, even if physically the dataframe has no partition. This introduces a small behavior change that for self-describing file formats like Parquet and Orc, Spark creates a metadata-only file in the target directory when writing a 0-partition dataframe, so that schema inference can still work if users read that directory later. The new behavior is more reasonable and more consistent regarding writing empty dataframe.

  - Since Spark 2.4, expression IDs in UDF arguments do not appear in column names. For example, a column name in Spark 2.4 is not `UDF:f(col0 AS colA#28)` but ``UDF:f(col0 AS `colA`)``.

  - Since Spark 2.4, writing a dataframe with an empty or nested empty schema using any file formats (parquet, orc, json, text, csv etc.) is not allowed. An exception is thrown when attempting to write dataframes with empty schema.

  - Since Spark 2.4, Spark compares a DATE type with a TIMESTAMP type after promotes both sides to TIMESTAMP. To set `false` to `spark.sql.legacy.compareDateTimestampInTimestamp` restores the previous behavior. This option will be removed in Spark 3.0.

  - Since Spark 2.4, creating a managed table with nonempty location is not allowed. An exception is thrown when attempting to create a managed table with nonempty location. To set `true` to `spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation` restores the previous behavior. This option will be removed in Spark 3.0.

  - Since Spark 2.4, renaming a managed table to existing location is not allowed. An exception is thrown when attempting to rename a managed table to existing location.

  - Since Spark 2.4, the type coercion rules can automatically promote the argument types of the variadic SQL functions (e.g., IN/COALESCE) to the widest common type, no matter how the input arguments order. In prior Spark versions, the promotion could fail in some specific orders (e.g., TimestampType, IntegerType and StringType) and throw an exception.

  - Since Spark 2.4, Spark has enabled non-cascading SQL cache invalidation in addition to the traditional cache invalidation mechanism. The non-cascading cache invalidation mechanism allows users to remove a cache without impacting its dependent caches. This new cache invalidation mechanism is used in scenarios where the data of the cache to be removed is still valid, e.g., calling unpersist() on a Dataset, or dropping a temporary view. This allows users to free up memory and keep the desired caches valid at the same time.

  - In version 2.3 and earlier, Spark converts Parquet Hive tables by default but ignores table properties like `TBLPROPERTIES (parquet.compression 'NONE')`. This happens for ORC Hive table properties like `TBLPROPERTIES (orc.compress 'NONE')` in case of `spark.sql.hive.convertMetastoreOrc=true`, too. Since Spark 2.4, Spark respects Parquet/ORC specific table properties while converting Parquet/ORC Hive tables. As an example, `CREATE TABLE t(id int) STORED AS PARQUET TBLPROPERTIES (parquet.compression 'NONE')` would generate Snappy parquet files during insertion in Spark 2.3, and in Spark 2.4, the result would be uncompressed parquet files.

  - Since Spark 2.0, Spark converts Parquet Hive tables by default for better performance. Since Spark 2.4, Spark converts ORC Hive tables by default, too. It means Spark uses its own ORC support by default instead of Hive SerDe. As an example, `CREATE TABLE t(id int) STORED AS ORC` would be handled with Hive SerDe in Spark 2.3, and in Spark 2.4, it would be converted into Spark's ORC data source table and ORC vectorization would be applied. To set `false` to `spark.sql.hive.convertMetastoreOrc` restores the previous behavior.

  - In version 2.3 and earlier, CSV rows are considered as malformed if at least one column value in the row is malformed. CSV parser dropped such rows in the DROPMALFORMED mode or outputs an error in the FAILFAST mode. Since Spark 2.4, CSV row is considered as malformed only when it contains malformed column values requested from CSV datasource, other values can be ignored. As an example, CSV file contains the "id,name" header and one row "1234". In Spark 2.4, selection of the id column consists of a row with one column value 1234 but in Spark 2.3 and earlier it is empty in the DROPMALFORMED mode. To restore the previous behavior, set `spark.sql.csv.parser.columnPruning.enabled` to `false`.

  - Since Spark 2.4, File listing for compute statistics is done in parallel by default. This can be disabled by setting `spark.sql.statistics.parallelFileListingInStatsComputation.enabled` to `False`.

  - Since Spark 2.4, Metadata files (e.g. Parquet summary files) and temporary files are not counted as data files when calculating table size during Statistics computation.

  - Since Spark 2.4, empty strings are saved as quoted empty strings `""`. In version 2.3 and earlier, empty strings are equal to `null` values and do not reflect to any characters in saved CSV files. For example, the row of `"a", null, "", 1` was written as `a,,,1`. Since Spark 2.4, the same row is saved as `a,,"",1`. To restore the previous behavior, set the CSV option `emptyValue` to empty (not quoted) string.

  - Since Spark 2.4, The LOAD DATA command supports wildcard `?` and `*`, which match any one character, and zero or more characters, respectively. Example: `LOAD DATA INPATH '/tmp/folder*/'` or `LOAD DATA INPATH '/tmp/part-?'`. Special Characters like `space` also now work in paths. Example: `LOAD DATA INPATH '/tmp/folder name/'`.

  - In Spark version 2.3 and earlier, HAVING without GROUP BY is treated as WHERE. This means, `SELECT 1 FROM range(10) HAVING true` is executed as `SELECT 1 FROM range(10) WHERE true`  and returns 10 rows. This violates SQL standard, and has been fixed in Spark 2.4. Since Spark 2.4, HAVING without GROUP BY is treated as a global aggregate, which means `SELECT 1 FROM range(10) HAVING true` will return only one row. To restore the previous behavior, set `spark.sql.legacy.parser.havingWithoutGroupByAsWhere` to `true`.

  - In version 2.3 and earlier, when reading from a Parquet data source table, Spark always returns null for any column whose column names in Hive metastore schema and Parquet schema are in different letter cases, no matter whether `spark.sql.caseSensitive` is set to `true` or `false`. Since 2.4, when `spark.sql.caseSensitive` is set to `false`, Spark does case insensitive column name resolution between Hive metastore schema and Parquet schema, so even column names are in different letter cases, Spark returns corresponding column values. An exception is thrown if there is ambiguity, i.e. more than one Parquet column is matched. This change also applies to Parquet Hive tables when `spark.sql.hive.convertMetastoreParquet` is set to `true`.

## Upgrading from Spark SQL 2.2 to 2.3

  - Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the referenced columns only include the internal corrupt record column (named `_corrupt_record` by default). For example, `spark.read.schema(schema).json(file).filter($"_corrupt_record".isNotNull).count()` and `spark.read.schema(schema).json(file).select("_corrupt_record").show()`. Instead, you can cache or save the parsed results and then send the same query. For example, `val df = spark.read.schema(schema).json(file).cache()` and then `df.filter($"_corrupt_record".isNotNull).count()`.

  - The `percentile_approx` function previously accepted numeric type input and output double type results. Now it supports date type, timestamp type and numeric types as input types. The result type is also changed to be the same as the input type, which is more reasonable for percentiles.

  - Since Spark 2.3, the Join/Filter's deterministic predicates that are after the first non-deterministic predicates are also pushed down/through the child operators, if possible. In prior Spark versions, these filters are not eligible for predicate pushdown.

  - Partition column inference previously found incorrect common type for different inferred types, for example, previously it ended up with double type as the common type for double type and date type. Now it finds the correct common type for such conflicts. The conflict resolution follows the table below:
    <table class="table">
      <tr>
        <th>
          <b>InputA \ InputB</b>
        </th>
        <th>
          <b>NullType</b>
        </th>
        <th>
          <b>IntegerType</b>
        </th>
        <th>
          <b>LongType</b>
        </th>
        <th>
          <b>DecimalType(38,0)*</b>
        </th>
        <th>
          <b>DoubleType</b>
        </th>
        <th>
          <b>DateType</b>
        </th>
        <th>
          <b>TimestampType</b>
        </th>
        <th>
          <b>StringType</b>
        </th>
      </tr>
      <tr>
        <td>
          <b>NullType</b>
        </td>
        <td>NullType</td>
        <td>IntegerType</td>
        <td>LongType</td>
        <td>DecimalType(38,0)</td>
        <td>DoubleType</td>
        <td>DateType</td>
        <td>TimestampType</td>
        <td>StringType</td>
      </tr>
      <tr>
        <td>
          <b>IntegerType</b>
        </td>
        <td>IntegerType</td>
        <td>IntegerType</td>
        <td>LongType</td>
        <td>DecimalType(38,0)</td>
        <td>DoubleType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
      </tr>
      <tr>
        <td>
          <b>LongType</b>
        </td>
        <td>LongType</td>
        <td>LongType</td>
        <td>LongType</td>
        <td>DecimalType(38,0)</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
      </tr>
      <tr>
        <td>
          <b>DecimalType(38,0)*</b>
        </td>
        <td>DecimalType(38,0)</td>
        <td>DecimalType(38,0)</td>
        <td>DecimalType(38,0)</td>
        <td>DecimalType(38,0)</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
      </tr>
      <tr>
        <td>
          <b>DoubleType</b>
        </td>
        <td>DoubleType</td>
        <td>DoubleType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>DoubleType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
      </tr>
      <tr>
        <td>
          <b>DateType</b>
        </td>
        <td>DateType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>DateType</td>
        <td>TimestampType</td>
        <td>StringType</td>
      </tr>
      <tr>
        <td>
          <b>TimestampType</b>
        </td>
        <td>TimestampType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>TimestampType</td>
        <td>TimestampType</td>
        <td>StringType</td>
      </tr>
      <tr>
        <td>
          <b>StringType</b>
        </td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
        <td>StringType</td>
      </tr>
    </table>

    Note that, for <b>DecimalType(38,0)*</b>, the table above intentionally does not cover all other combinations of scales and precisions because currently we only infer decimal type like `BigInteger`/`BigInt`. For example, 1.1 is inferred as double type.

  - Since Spark 2.3, when either broadcast hash join or broadcast nested loop join is applicable, we prefer to broadcasting the table that is explicitly specified in a broadcast hint. For details, see the section [Join Strategy Hints for SQL Queries](sql-performance-tuning.html#join-strategy-hints-for-sql-queries) and [SPARK-22489](https://issues.apache.org/jira/browse/SPARK-22489).

  - Since Spark 2.3, when all inputs are binary, `functions.concat()` returns an output as binary. Otherwise, it returns as a string. Until Spark 2.3, it always returns as a string despite of input types. To keep the old behavior, set `spark.sql.function.concatBinaryAsString` to `true`.

  - Since Spark 2.3, when all inputs are binary, SQL `elt()` returns an output as binary. Otherwise, it returns as a string. Until Spark 2.3, it always returns as a string despite of input types. To keep the old behavior, set `spark.sql.function.eltOutputAsString` to `true`.

 - Since Spark 2.3, by default arithmetic operations between decimals return a rounded value if an exact representation is not possible (instead of returning NULL). This is compliant with SQL ANSI 2011 specification and Hive's new behavior introduced in Hive 2.2 (HIVE-15331). This involves the following changes

    - The rules to determine the result type of an arithmetic operation have been updated. In particular, if the precision / scale needed are out of the range of available values, the scale is reduced up to 6, in order to prevent the truncation of the integer part of the decimals. All the arithmetic operations are affected by the change, i.e. addition (`+`), subtraction (`-`), multiplication (`*`), division (`/`), remainder (`%`) and positive modulus (`pmod`).

    - Literal values used in SQL operations are converted to DECIMAL with the exact precision and scale needed by them.

    - The configuration `spark.sql.decimalOperations.allowPrecisionLoss` has been introduced. It defaults to `true`, which means the new behavior described here; if set to `false`, Spark uses previous rules, i.e. it doesn't adjust the needed scale to represent the values and it returns NULL if an exact representation of the value is not possible.

  - Un-aliased subquery's semantic has not been well defined with confusing behaviors. Since Spark 2.3, we invalidate such confusing cases, for example: `SELECT v.i from (SELECT i FROM v)`, Spark will throw an analysis exception in this case because users should not be able to use the qualifier inside a subquery. See [SPARK-20690](https://issues.apache.org/jira/browse/SPARK-20690) and [SPARK-21335](https://issues.apache.org/jira/browse/SPARK-21335) for more details.

  - When creating a `SparkSession` with `SparkSession.builder.getOrCreate()`, if there is an existing `SparkContext`, the builder was trying to update the `SparkConf` of the existing `SparkContext` with configurations specified to the builder, but the `SparkContext` is shared by all `SparkSession`s, so we should not update them. Since 2.3, the builder comes to not update the configurations. If you want to update them, you need to update them prior to creating a `SparkSession`.

## Upgrading from Spark SQL 2.1 to 2.2

  - Spark 2.1.1 introduced a new configuration key: `spark.sql.hive.caseSensitiveInferenceMode`. It had a default setting of `NEVER_INFER`, which kept behavior identical to 2.1.0. However, Spark 2.2.0 changes this setting's default value to `INFER_AND_SAVE` to restore compatibility with reading Hive metastore tables whose underlying file schema have mixed-case column names. With the `INFER_AND_SAVE` configuration value, on first access Spark will perform schema inference on any Hive metastore table for which it has not already saved an inferred schema. Note that schema inference can be a very time-consuming operation for tables with thousands of partitions. If compatibility with mixed-case column names is not a concern, you can safely set `spark.sql.hive.caseSensitiveInferenceMode` to `NEVER_INFER` to avoid the initial overhead of schema inference. Note that with the new default `INFER_AND_SAVE` setting, the results of the schema inference are saved as a metastore key for future use. Therefore, the initial schema inference occurs only at a table's first access.

  - Since Spark 2.2.1 and 2.3.0, the schema is always inferred at runtime when the data source tables have the columns that exist in both partition schema and data schema. The inferred schema does not have the partitioned columns. When reading the table, Spark respects the partition values of these overlapping columns instead of the values stored in the data source files. In 2.2.0 and 2.1.x release, the inferred schema is partitioned but the data of the table is invisible to users (i.e., the result set is empty).

  - Since Spark 2.2, view definitions are stored in a different way from prior versions. This may cause Spark unable to read views created by prior versions. In such cases, you need to recreate the views using `ALTER VIEW AS` or `CREATE OR REPLACE VIEW AS` with newer Spark versions.

## Upgrading from Spark SQL 2.0 to 2.1

 - Datasource tables now store partition metadata in the Hive metastore. This means that Hive DDLs such as `ALTER TABLE PARTITION ... SET LOCATION` are now available for tables created with the Datasource API.

    - Legacy datasource tables can be migrated to this format via the `MSCK REPAIR TABLE` command. Migrating legacy tables is recommended to take advantage of Hive DDL support and improved planning performance.

    - To determine if a table has been migrated, look for the `PartitionProvider: Catalog` attribute when issuing `DESCRIBE FORMATTED` on the table.
 - Changes to `INSERT OVERWRITE TABLE ... PARTITION ...` behavior for Datasource tables.

    - In prior Spark versions `INSERT OVERWRITE` overwrote the entire Datasource table, even when given a partition specification. Now only partitions matching the specification are overwritten.

    - Note that this still differs from the behavior of Hive tables, which is to overwrite only partitions overlapping with newly inserted data.

## Upgrading from Spark SQL 1.6 to 2.0

 - `SparkSession` is now the new entry point of Spark that replaces the old `SQLContext` and

   `HiveContext`. Note that the old SQLContext and HiveContext are kept for backward compatibility. A new `catalog` interface is accessible from `SparkSession` - existing API on databases and tables access such as `listTables`, `createExternalTable`, `dropTempView`, `cacheTable` are moved here.

 - Dataset API and DataFrame API are unified. In Scala, `DataFrame` becomes a type alias for
   `Dataset[Row]`, while Java API users must replace `DataFrame` with `Dataset<Row>`. Both the typed
   transformations (e.g., `map`, `filter`, and `groupByKey`) and untyped transformations (e.g.,
   `select` and `groupBy`) are available on the Dataset class. Since compile-time type-safety in
   Python and R is not a language feature, the concept of Dataset does not apply to these languages’
   APIs. Instead, `DataFrame` remains the primary programming abstraction, which is analogous to the
   single-node data frame notion in these languages.

 - Dataset and DataFrame API `unionAll` has been deprecated and replaced by `union`

 - Dataset and DataFrame API `explode` has been deprecated, alternatively, use `functions.explode()` with `select` or `flatMap`

 - Dataset and DataFrame API `registerTempTable` has been deprecated and replaced by `createOrReplaceTempView`

 - Changes to `CREATE TABLE ... LOCATION` behavior for Hive tables.

    - From Spark 2.0, `CREATE TABLE ... LOCATION` is equivalent to `CREATE EXTERNAL TABLE ... LOCATION`
      in order to prevent accidental dropping the existing data in the user-provided locations.
      That means, a Hive table created in Spark SQL with the user-specified location is always a Hive external table.
      Dropping external tables will not remove the data. Users are not allowed to specify the location for Hive managed tables.
      Note that this is different from the Hive behavior.

    - As a result, `DROP TABLE` statements on those tables will not remove the data.

 - `spark.sql.parquet.cacheMetadata` is no longer used.
   See [SPARK-13664](https://issues.apache.org/jira/browse/SPARK-13664) for details.

## Upgrading from Spark SQL 1.5 to 1.6

 - From Spark 1.6, by default, the Thrift server runs in multi-session mode. Which means each JDBC/ODBC
   connection owns a copy of their own SQL configuration and temporary function registry. Cached
   tables are still shared though. If you prefer to run the Thrift server in the old single-session
   mode, please set option `spark.sql.hive.thriftServer.singleSession` to `true`. You may either add
   this option to `spark-defaults.conf`, or pass it to `start-thriftserver.sh` via `--conf`:

   {% highlight bash %}
   ./sbin/start-thriftserver.sh \
     --conf spark.sql.hive.thriftServer.singleSession=true \
     ...
   {% endhighlight %}

 - From Spark 1.6, LongType casts to TimestampType expect seconds instead of microseconds. This
   change was made to match the behavior of Hive 1.2 for more consistent type casting to TimestampType
   from numeric types. See [SPARK-11724](https://issues.apache.org/jira/browse/SPARK-11724) for
   details.

## Upgrading from Spark SQL 1.4 to 1.5

 - Optimized execution using manually managed memory (Tungsten) is now enabled by default, along with
   code generation for expression evaluation. These features can both be disabled by setting
   `spark.sql.tungsten.enabled` to `false`.

 - Parquet schema merging is no longer enabled by default. It can be re-enabled by setting
   `spark.sql.parquet.mergeSchema` to `true`.

 - In-memory columnar storage partition pruning is on by default. It can be disabled by setting
   `spark.sql.inMemoryColumnarStorage.partitionPruning` to `false`.

 - Unlimited precision decimal columns are no longer supported, instead Spark SQL enforces a maximum
   precision of 38. When inferring schema from `BigDecimal` objects, a precision of (38, 18) is now
   used. When no precision is specified in DDL then the default remains `Decimal(10, 0)`.

 - Timestamps are now stored at a precision of 1us, rather than 1ns

 - In the `sql` dialect, floating point numbers are now parsed as decimal. HiveQL parsing remains
   unchanged.

 - The canonical name of SQL/DataFrame functions are now lower case (e.g., sum vs SUM).

 - JSON data source will not automatically load new files that are created by other applications
   (i.e. files that are not inserted to the dataset through Spark SQL).
   For a JSON persistent table (i.e. the metadata of the table is stored in Hive Metastore),
   users can use `REFRESH TABLE` SQL command or `HiveContext`'s `refreshTable` method
   to include those new files to the table. For a DataFrame representing a JSON dataset, users need to recreate
   the DataFrame and the new DataFrame will include new files.

## Upgrading from Spark SQL 1.3 to 1.4

#### DataFrame data reader/writer interface
{:.no_toc}

Based on user feedback, we created a new, more fluid API for reading data in (`SQLContext.read`)
and writing data out (`DataFrame.write`),
and deprecated the old APIs (e.g., `SQLContext.parquetFile`, `SQLContext.jsonFile`).

See the API docs for `SQLContext.read` (
  <a href="api/scala/org/apache/spark/sql/SQLContext.html#read:DataFrameReader">Scala</a>,
  <a href="api/java/org/apache/spark/sql/SQLContext.html#read()">Java</a>,
  <a href="api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.read.html#pyspark.sql.SparkSession.read">Python</a>
) and `DataFrame.write` (
  <a href="api/scala/org/apache/spark/sql/DataFrame.html#write:DataFrameWriter">Scala</a>,
  <a href="api/java/org/apache/spark/sql/Dataset.html#write()">Java</a>,
  <a href="api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.write.html#pyspark.sql.DataFrame.write">Python</a>
) more information.


#### DataFrame.groupBy retains grouping columns
{:.no_toc}

Based on user feedback, we changed the default behavior of `DataFrame.groupBy().agg()` to retain the
grouping columns in the resulting `DataFrame`. To keep the behavior in 1.3, set `spark.sql.retainGroupColumns` to `false`.

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% highlight scala %}

// In 1.3.x, in order for the grouping column "department" to show up,
// it must be included explicitly as part of the agg function call.
df.groupBy("department").agg($"department", max("age"), sum("expense"))

// In 1.4+, grouping column "department" is included automatically.
df.groupBy("department").agg(max("age"), sum("expense"))

// Revert to 1.3 behavior (not retaining grouping column) by:
sqlContext.setConf("spark.sql.retainGroupColumns", "false")

{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight java %}

// In 1.3.x, in order for the grouping column "department" to show up,
// it must be included explicitly as part of the agg function call.
df.groupBy("department").agg(col("department"), max("age"), sum("expense"));

// In 1.4+, grouping column "department" is included automatically.
df.groupBy("department").agg(max("age"), sum("expense"));

// Revert to 1.3 behavior (not retaining grouping column) by:
sqlContext.setConf("spark.sql.retainGroupColumns", "false");

{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}

import pyspark.sql.functions as func

# In 1.3.x, in order for the grouping column "department" to show up,
# it must be included explicitly as part of the agg function call.
df.groupBy("department").agg(df["department"], func.max("age"), func.sum("expense"))

# In 1.4+, grouping column "department" is included automatically.
df.groupBy("department").agg(func.max("age"), func.sum("expense"))

# Revert to 1.3.x behavior (not retaining grouping column) by:
sqlContext.setConf("spark.sql.retainGroupColumns", "false")

{% endhighlight %}
</div>

</div>


#### Behavior change on DataFrame.withColumn
{:.no_toc}

Prior to 1.4, DataFrame.withColumn() supports adding a column only. The column will always be added
as a new column with its specified name in the result DataFrame even if there may be any existing
columns of the same name. Since 1.4, DataFrame.withColumn() supports adding a column of a different
name from names of all existing columns or replacing existing columns of the same name.

Note that this change is only for Scala API, not for PySpark and SparkR.


## Upgrading from Spark SQL 1.0-1.2 to 1.3

In Spark 1.3 we removed the "Alpha" label from Spark SQL and as part of this did a cleanup of the
available APIs. From Spark 1.3 onwards, Spark SQL will provide binary compatibility with other
releases in the 1.X series. This compatibility guarantee excludes APIs that are explicitly marked
as unstable (i.e., DeveloperAPI or Experimental).

#### Rename of SchemaRDD to DataFrame
{:.no_toc}

The largest change that users will notice when upgrading to Spark SQL 1.3 is that `SchemaRDD` has
been renamed to `DataFrame`. This is primarily because DataFrames no longer inherit from RDD
directly, but instead provide most of the functionality that RDDs provide though their own
implementation. DataFrames can still be converted to RDDs by calling the `.rdd` method.

In Scala, there is a type alias from `SchemaRDD` to `DataFrame` to provide source compatibility for
some use cases. It is still recommended that users update their code to use `DataFrame` instead.
Java and Python users will need to update their code.

#### Unification of the Java and Scala APIs
{:.no_toc}

Prior to Spark 1.3 there were separate Java compatible classes (`JavaSQLContext` and `JavaSchemaRDD`)
that mirrored the Scala API. In Spark 1.3 the Java API and Scala API have been unified. Users
of either language should use `SQLContext` and `DataFrame`. In general these classes try to
use types that are usable from both languages (i.e. `Array` instead of language-specific collections).
In some cases where no common type exists (e.g., for passing in closures or Maps) function overloading
is used instead.

Additionally, the Java specific types API has been removed. Users of both Scala and Java should
use the classes present in `org.apache.spark.sql.types` to describe schema programmatically.


#### Isolation of Implicit Conversions and Removal of dsl Package (Scala-only)
{:.no_toc}

Many of the code examples prior to Spark 1.3 started with `import sqlContext._`, which brought
all of the functions from sqlContext into scope. In Spark 1.3 we have isolated the implicit
conversions for converting `RDD`s into `DataFrame`s into an object inside of the `SQLContext`.
Users should now write `import sqlContext.implicits._`.

Additionally, the implicit conversions now only augment RDDs that are composed of `Product`s (i.e.,
case classes or tuples) with a method `toDF`, instead of applying automatically.

When using function inside of the DSL (now replaced with the `DataFrame` API) users used to import
`org.apache.spark.sql.catalyst.dsl`. Instead the public dataframe functions API should be used:
`import org.apache.spark.sql.functions._`.

#### Removal of the type aliases in org.apache.spark.sql for DataType (Scala-only)
{:.no_toc}

Spark 1.3 removes the type aliases that were present in the base sql package for `DataType`. Users
should instead import the classes in `org.apache.spark.sql.types`

#### UDF Registration Moved to `sqlContext.udf` (Java & Scala)
{:.no_toc}

Functions that are used to register UDFs, either for use in the DataFrame DSL or SQL, have been
moved into the udf object in `SQLContext`.

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% highlight scala %}

sqlContext.udf.register("strLen", (s: String) => s.length())

{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight java %}

sqlContext.udf().register("strLen", (String s) -> s.length(), DataTypes.IntegerType);

{% endhighlight %}
</div>

</div>

Python UDF registration is unchanged.



## Compatibility with Apache Hive

Spark SQL is designed to be compatible with the Hive Metastore, SerDes and UDFs.
Currently, Hive SerDes and UDFs are based on built-in Hive,
and Spark SQL can be connected to different versions of Hive Metastore
(from 0.12.0 to 2.3.9 and 3.0.0 to 3.1.3. Also see [Interacting with Different Versions of Hive Metastore](sql-data-sources-hive-tables.html#interacting-with-different-versions-of-hive-metastore)).

#### Deploying in Existing Hive Warehouses
{:.no_toc}

The Spark SQL Thrift JDBC server is designed to be "out of the box" compatible with existing Hive
installations. You do not need to modify your existing Hive Metastore or change the data placement
or partitioning of your tables.

### Supported Hive Features
{:.no_toc}

Spark SQL supports the vast majority of Hive features, such as:

* Hive query statements, including:
  * `SELECT`
  * `GROUP BY`
  * `ORDER BY`
  * `DISTRIBUTE BY`
  * `CLUSTER BY`
  * `SORT BY`
* All Hive operators, including:
  * Relational operators (`=`, `<=>`, `==`, `<>`, `<`, `>`, `>=`, `<=`, etc)
  * Arithmetic operators (`+`, `-`, `*`, `/`, `%`, etc)
  * Logical operators (`AND`, `OR`, etc)
  * Complex type constructors
  * Mathematical functions (`sign`, `ln`, `cos`, etc)
  * String functions (`instr`, `length`, `printf`, etc)
* User defined functions (UDF)
* User defined aggregation functions (UDAF)
* User defined serialization formats (SerDes)
* Window functions
* Joins
  * `JOIN`
  * `{LEFT|RIGHT|FULL} OUTER JOIN`
  * `LEFT SEMI JOIN`
  * `LEFT ANTI JOIN`
  * `CROSS JOIN`
* Unions
* Sub-queries
  * Sub-queries in the FROM Clause

    ```SELECT col FROM (SELECT a + b AS col FROM t1) t2```
  * Sub-queries in WHERE Clause
    * Correlated or non-correlated IN and NOT IN statement in WHERE Clause

      ```
      SELECT col FROM t1 WHERE col IN (SELECT a FROM t2 WHERE t1.a = t2.a)
      SELECT col FROM t1 WHERE col IN (SELECT a FROM t2)
      ```
    * Correlated or non-correlated EXISTS and NOT EXISTS statement in WHERE Clause

      ```
      SELECT col FROM t1 WHERE EXISTS (SELECT t2.a FROM t2 WHERE t1.a = t2.a AND t2.a > 10)
      SELECT col FROM t1 WHERE EXISTS (SELECT t2.a FROM t2 WHERE t2.a > 10)
      ```
    * Non-correlated IN and NOT IN statement in JOIN Condition

      ```SELECT t1.col FROM t1 JOIN t2 ON t1.a = t2.a AND t1.a IN (SELECT a FROM t3)```

    * Non-correlated EXISTS and NOT EXISTS statement in JOIN Condition

      ```SELECT t1.col FROM t1 JOIN t2 ON t1.a = t2.a AND EXISTS (SELECT * FROM t3 WHERE t3.a > 10)```

* Sampling
* Explain
* Partitioned tables including dynamic partition insertion
* View
  * If column aliases are not specified in view definition queries, both Spark and Hive will
    generate alias names, but in different ways. In order for Spark to be able to read views created
    by Hive, users should explicitly specify column aliases in view definition queries. As an
    example, Spark cannot read `v1` created as below by Hive.

    ```
    CREATE VIEW v1 AS SELECT * FROM (SELECT c + 1 FROM (SELECT 1 c) t1) t2;
    ```

    Instead, you should create `v1` as below with column aliases explicitly specified.

    ```
    CREATE VIEW v1 AS SELECT * FROM (SELECT c + 1 AS inc_c FROM (SELECT 1 c) t1) t2;
    ```

* All Hive DDL Functions, including:
  * `CREATE TABLE`
  * `CREATE TABLE AS SELECT`
  * `CREATE TABLE LIKE`
  * `ALTER TABLE`
* Most Hive Data types, including:
  * `TINYINT`
  * `SMALLINT`
  * `INT`
  * `BIGINT`
  * `BOOLEAN`
  * `FLOAT`
  * `DOUBLE`
  * `STRING`
  * `BINARY`
  * `TIMESTAMP`
  * `DATE`
  * `ARRAY<>`
  * `MAP<>`
  * `STRUCT<>`

### Unsupported Hive Functionality
{:.no_toc}

Below is a list of Hive features that we don't support yet. Most of these features are rarely used
in Hive deployments.

**Esoteric Hive Features**

* `UNION` type
* Unique join
* Column statistics collecting: Spark SQL does not piggyback scans to collect column statistics at
  the moment and only supports populating the sizeInBytes field of the hive metastore.

**Hive Input/Output Formats**

* File format for CLI: For results showing back to the CLI, Spark SQL only supports TextOutputFormat.
* Hadoop archive

**Hive Optimizations**

A handful of Hive optimizations are not yet included in Spark. Some of these (such as indexes) are
less important due to Spark SQL's in-memory computational model. Others are slotted for future
releases of Spark SQL.

* Block-level bitmap indexes and virtual columns (used to build indexes)
* Automatically determine the number of reducers for joins and groupbys: Currently, in Spark SQL, you
  need to control the degree of parallelism post-shuffle using "`SET spark.sql.shuffle.partitions=[num_tasks];`".
* Meta-data only query: For queries that can be answered by using only metadata, Spark SQL still
  launches tasks to compute the result.
* Skew data flag: Spark SQL does not follow the skew data flags in Hive.
* `STREAMTABLE` hint in join: Spark SQL does not follow the `STREAMTABLE` hint.
* Merge multiple small files for query results: if the result output contains multiple small files,
  Hive can optionally merge the small files into fewer large files to avoid overflowing the HDFS
  metadata. Spark SQL does not support that.

**Hive UDF/UDTF/UDAF**

Not all the APIs of the Hive UDF/UDTF/UDAF are supported by Spark SQL. Below are the unsupported APIs:

* `getRequiredJars` and `getRequiredFiles` (`UDF` and `GenericUDF`) are functions to automatically
  include additional resources required by this UDF.
* `initialize(StructObjectInspector)` in `GenericUDTF` is not supported yet. Spark SQL currently uses
  a deprecated interface `initialize(ObjectInspector[])` only.
* `configure` (`GenericUDF`, `GenericUDTF`, and `GenericUDAFEvaluator`) is a function to initialize
  functions with `MapredContext`, which is inapplicable to Spark.
* `close` (`GenericUDF` and `GenericUDAFEvaluator`) is a function to release associated resources.
  Spark SQL does not call this function when tasks finish.
* `reset` (`GenericUDAFEvaluator`) is a function to re-initialize aggregation for reusing the same aggregation.
  Spark SQL currently does not support the reuse of aggregation.
* `getWindowingEvaluator` (`GenericUDAFEvaluator`) is a function to optimize aggregation by evaluating
  an aggregate over a fixed window.

### Incompatible Hive UDF
{:.no_toc}

Below are the scenarios in which Hive and Spark generate different results:

* `SQRT(n)` If n < 0, Hive returns null, Spark SQL returns NaN.
* `ACOS(n)` If n < -1 or n > 1, Hive returns null, Spark SQL returns NaN.
* `ASIN(n)` If n < -1 or n > 1, Hive returns null, Spark SQL returns NaN.
* `CAST(n AS TIMESTAMP)` If n is integral numbers, Hive treats n as milliseconds, Spark SQL treats n as seconds.
