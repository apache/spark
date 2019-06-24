---
layout: global
title: Spark SQL Upgrading Guide
displayTitle: Spark SQL Upgrading Guide
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

## Upgrading From Spark SQL 2.4 to 3.0
  - Since Spark 3.0, PySpark requires a Pandas version of 0.23.2 or higher to use Pandas related functionality, such as `toPandas`, `createDataFrame` from Pandas DataFrame, etc.

  - Since Spark 3.0, PySpark requires a PyArrow version of 0.12.1 or higher to use PyArrow related functionality, such as `pandas_udf`, `toPandas` and `createDataFrame` with "spark.sql.execution.arrow.enabled=true", etc.

  - In Spark version 2.4 and earlier, SQL queries such as `FROM <table>` or `FROM <table> UNION ALL FROM <table>` are supported by accident. In hive-style `FROM <table> SELECT <expr>`, the `SELECT` clause is not negligible. Neither Hive nor Presto support this syntax. Therefore we will treat these queries as invalid since Spark 3.0.

  - Since Spark 3.0, the Dataset and DataFrame API `unionAll` is not deprecated any more. It is an alias for `union`.

  - In PySpark, when creating a `SparkSession` with `SparkSession.builder.getOrCreate()`, if there is an existing `SparkContext`, the builder was trying to update the `SparkConf` of the existing `SparkContext` with configurations specified to the builder, but the `SparkContext` is shared by all `SparkSession`s, so we should not update them. Since 3.0, the builder comes to not update the configurations. This is the same behavior as Java/Scala API in 2.3 and above. If you want to update them, you need to update them prior to creating a `SparkSession`.

  - In Spark version 2.4 and earlier, the parser of JSON data source treats empty strings as null for some data types such as `IntegerType`. For `FloatType` and `DoubleType`, it fails on empty strings and throws exceptions. Since Spark 3.0, we disallow empty strings and will throw exceptions for data types except for `StringType` and `BinaryType`.

  - Since Spark 3.0, the `from_json` functions supports two modes - `PERMISSIVE` and `FAILFAST`. The modes can be set via the `mode` option. The default mode became `PERMISSIVE`. In previous versions, behavior of `from_json` did not conform to either `PERMISSIVE` nor `FAILFAST`, especially in processing of malformed JSON records. For example, the JSON string `{"a" 1}` with the schema `a INT` is converted to `null` by previous versions but Spark 3.0 converts it to `Row(null)`.

  - The `ADD JAR` command previously returned a result set with the single value 0. It now returns an empty result set.

  - In Spark version 2.4 and earlier, users can create map values with map type key via built-in function like `CreateMap`, `MapFromArrays`, etc. Since Spark 3.0, it's not allowed to create map values with map type key with these built-in functions. Users can still read map values with map type key from data source or Java/Scala collections, though they are not very useful.

  - In Spark version 2.4 and earlier, `Dataset.groupByKey` results to a grouped dataset with key attribute wrongly named as "value", if the key is non-struct type, e.g. int, string, array, etc. This is counterintuitive and makes the schema of aggregation queries weird. For example, the schema of `ds.groupByKey(...).count()` is `(value, count)`. Since Spark 3.0, we name the grouping attribute to "key". The old behaviour is preserved under a newly added configuration `spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue` with a default value of `false`.

  - In Spark version 2.4 and earlier, float/double -0.0 is semantically equal to 0.0, but -0.0 and 0.0 are considered as different values when used in aggregate grouping keys, window partition keys and join keys. Since Spark 3.0, this bug is fixed. For example, `Seq(-0.0, 0.0).toDF("d").groupBy("d").count()` returns `[(0.0, 2)]` in Spark 3.0, and `[(0.0, 1), (-0.0, 1)]` in Spark 2.4 and earlier.

  - In Spark version 2.4 and earlier, users can create a map with duplicated keys via built-in functions like `CreateMap`, `StringToMap`, etc. The behavior of map with duplicated keys is undefined, e.g. map look up respects the duplicated key appears first, `Dataset.collect` only keeps the duplicated key appears last, `MapKeys` returns duplicated keys, etc. Since Spark 3.0, these built-in functions will remove duplicated map keys with last wins policy. Users may still read map values with duplicated keys from data sources which do not enforce it (e.g. Parquet), the behavior will be undefined.

  - In Spark version 2.4 and earlier, partition column value is converted as null if it can't be casted to corresponding user provided schema. Since 3.0, partition column value is validated with user provided schema. An exception is thrown if the validation fails. You can disable such validation by setting `spark.sql.sources.validatePartitionColumns` to `false`.

  - In Spark version 2.4 and earlier, the `SET` command works without any warnings even if the specified key is for `SparkConf` entries and it has no effect because the command does not update `SparkConf`, but the behavior might confuse users. Since 3.0, the command fails if a `SparkConf` key is used. You can disable such a check by setting `spark.sql.legacy.setCommandRejectsSparkCoreConfs` to `false`.

  - In Spark version 2.4 and earlier, CSV datasource converts a malformed CSV string to a row with all `null`s in the PERMISSIVE mode. Since Spark 3.0, the returned row can contain non-`null` fields if some of CSV column values were parsed and converted to desired types successfully.

  - In Spark version 2.4 and earlier, JSON datasource and JSON functions like `from_json` convert a bad JSON record to a row with all `null`s in the PERMISSIVE mode when specified schema is `StructType`. Since Spark 3.0, the returned row can contain non-`null` fields if some of JSON column values were parsed and converted to desired types successfully.

  - Refreshing a cached table would trigger a table uncache operation and then a table cache (lazily) operation. In Spark version 2.4 and earlier, the cache name and storage level are not preserved before the uncache operation. Therefore, the cache name and storage level could be changed unexpectedly. Since Spark 3.0, cache name and storage level will be first preserved for cache recreation. It helps to maintain a consistent cache behavior upon table refreshing.

  - Since Spark 3.0, JSON datasource and JSON function `schema_of_json` infer TimestampType from string values if they match to the pattern defined by the JSON option `timestampFormat`. Set JSON option `inferTimestamp` to `false` to disable such type inferring.

  - In PySpark, when Arrow optimization is enabled, if Arrow version is higher than 0.11.0, Arrow can perform safe type conversion when converting Pandas.Series to Arrow array during serialization. Arrow will raise errors when detecting unsafe type conversion like overflow. Setting `spark.sql.execution.pandas.arrowSafeTypeConversion` to true can enable it. The default setting is false. PySpark's behavior for Arrow versions is illustrated in the table below:
    <table class="table">
        <tr>
          <th>
            <b>PyArrow version</b>
          </th>
          <th>
            <b>Integer Overflow</b>
          </th>
          <th>
            <b>Floating Point Truncation</b>
          </th>
        </tr>
        <tr>
          <td>
            version < 0.11.0
          </td>
          <td>
            Raise error
          </td>
          <td>
            Silently allows
          </td>
        </tr>
        <tr>
          <td>
            version > 0.11.0, arrowSafeTypeConversion=false
          </td>
          <td>
            Silent overflow
          </td>
          <td>
            Silently allows
          </td>
        </tr>
        <tr>
          <td>
            version > 0.11.0, arrowSafeTypeConversion=true
          </td>
          <td>
            Raise error
          </td>
          <td>
            Raise error
          </td>
        </tr>
    </table>

  - In Spark version 2.4 and earlier, if `org.apache.spark.sql.functions.udf(Any, DataType)` gets a Scala closure with primitive-type argument, the returned UDF will return null if the input values is null. Since Spark 3.0, the UDF will return the default value of the Java type if the input value is null. For example, `val f = udf((x: Int) => x, IntegerType)`, `f($"x")` will return null in Spark 2.4 and earlier if column `x` is null, and return 0 in Spark 3.0. This behavior change is introduced because Spark 3.0 is built with Scala 2.12 by default.

  - Since Spark 3.0, Proleptic Gregorian calendar is used in parsing, formatting, and converting dates and timestamps as well as in extracting sub-components like years, days and etc. Spark 3.0 uses Java 8 API classes from the java.time packages that based on ISO chronology (https://docs.oracle.com/javase/8/docs/api/java/time/chrono/IsoChronology.html). In Spark version 2.4 and earlier, those operations are performed by using the hybrid calendar (Julian + Gregorian, see https://docs.oracle.com/javase/7/docs/api/java/util/GregorianCalendar.html). The changes impact on the results for dates before October 15, 1582 (Gregorian) and affect on the following Spark 3.0 API:

    - CSV/JSON datasources use java.time API for parsing and generating CSV/JSON content. In Spark version 2.4 and earlier, java.text.SimpleDateFormat is used for the same purpose with fallbacks to the parsing mechanisms of Spark 2.0 and 1.x. For example, `2018-12-08 10:39:21.123` with the pattern `yyyy-MM-dd'T'HH:mm:ss.SSS` cannot be parsed since Spark 3.0 because the timestamp does not match to the pattern but it can be parsed by earlier Spark versions due to a fallback to `Timestamp.valueOf`. To parse the same timestamp since Spark 3.0, the pattern should be `yyyy-MM-dd HH:mm:ss.SSS`.

    - The `unix_timestamp`, `date_format`, `to_unix_timestamp`, `from_unixtime`, `to_date`, `to_timestamp` functions. New implementation supports pattern formats as described here https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html and performs strict checking of its input. For example, the `2015-07-22 10:00:00` timestamp cannot be parse if pattern is `yyyy-MM-dd` because the parser does not consume whole input. Another example is the `31/01/2015 00:00` input cannot be parsed by the `dd/MM/yyyy hh:mm` pattern because `hh` supposes hours in the range `1-12`.

    - The `weekofyear`, `weekday`, `dayofweek`, `date_trunc`, `from_utc_timestamp`, `to_utc_timestamp`, and `unix_timestamp` functions use java.time API for calculation week number of year, day number of week as well for conversion from/to TimestampType values in UTC time zone.

    - the JDBC options `lowerBound` and `upperBound` are converted to TimestampType/DateType values in the same way as casting strings to TimestampType/DateType values. The conversion is based on Proleptic Gregorian calendar, and time zone defined by the SQL config `spark.sql.session.timeZone`. In Spark version 2.4 and earlier, the conversion is based on the hybrid calendar (Julian + Gregorian) and on default system time zone.
    
    - Formatting of `TIMESTAMP` and `DATE` literals.

  - In Spark version 2.4 and earlier, invalid time zone ids are silently ignored and replaced by GMT time zone, for example, in the from_utc_timestamp function. Since Spark 3.0, such time zone ids are rejected, and Spark throws `java.time.DateTimeException`.

  - In Spark version 2.4 and earlier, the `current_timestamp` function returns a timestamp with millisecond resolution only. Since Spark 3.0, the function can return the result with microsecond resolution if the underlying clock available on the system offers such resolution.

  - In Spark version 2.4 and earlier, when reading a Hive Serde table with Spark native data sources(parquet/orc), Spark will infer the actual file schema and update the table schema in metastore. Since Spark 3.0, Spark doesn't infer the schema anymore. This should not cause any problems to end users, but if it does, please set `spark.sql.hive.caseSensitiveInferenceMode` to `INFER_AND_SAVE`.

  - Since Spark 3.0, `TIMESTAMP` literals are converted to strings using the SQL config `spark.sql.session.timeZone`. In Spark version 2.4 and earlier, the conversion uses the default time zone of the Java virtual machine.

  - In Spark version 2.4, when a spark session is created via `cloneSession()`, the newly created spark session inherits its configuration from its parent `SparkContext` even though the same configuration may exist with a different value in its parent spark session. Since Spark 3.0, the configurations of a parent `SparkSession` have a higher precedence over the parent `SparkContext`. The old behavior can be restored by setting `spark.sql.legacy.sessionInitWithConfigDefaults` to `true`.

  - Since Spark 3.0, parquet logical type `TIMESTAMP_MICROS` is used by default while saving `TIMESTAMP` columns. In Spark version 2.4 and earlier, `TIMESTAMP` columns are saved as `INT96` in parquet files. To set `INT96` to `spark.sql.parquet.outputTimestampType` restores the previous behavior.

  - Since Spark 3.0, if `hive.default.fileformat` is not found in `Spark SQL configuration` then it will fallback to hive-site.xml present in the `Hadoop configuration` of `SparkContext`.

  - Since Spark 3.0, Spark will cast `String` to `Date/TimeStamp` in binary comparisons with dates/timestamps. The previous behaviour of casting `Date/Timestamp` to `String` can be restored by setting `spark.sql.legacy.typeCoercion.datetimeToString` to `true`.

  - Since Spark 3.0, when Avro files are written with user provided schema, the fields will be matched by field names between catalyst schema and avro schema instead of positions.

  - Since Spark 3.0, when Avro files are written with user provided non-nullable schema, even the catalyst schema is nullable, Spark is still able to write the files. However, Spark will throw runtime NPE if any of the records contains null.

  - Since Spark 3.0, we use a new protocol for fetching shuffle blocks, for external shuffle service users, we need to upgrade the server correspondingly. Otherwise, we'll get the error message `UnsupportedOperationException: Unexpected message: FetchShuffleBlocks`. If it is hard to upgrade the shuffle service right now, you can still use the old protocol by setting `spark.shuffle.useOldFetchProtocol` to `true`. 

  - Since Spark 3.0, a higher-order function `exists` follows the three-valued boolean logic, i.e., if the `predicate` returns any `null`s and no `true` is obtained, then `exists` will return `null` instead of `false`. For example, `exists(array(1, null, 3), x -> x % 2 == 0)` will be `null`. The previous behaviour can be restored by setting `spark.sql.legacy.arrayExistsFollowsThreeValuedLogic` to `false`.

  - Since Spark 3.0, if files or subdirectories disappear during recursive directory listing (i.e. they appear in an intermediate listing but then cannot be read or listed during later phases of the recursive directory listing, due to either concurrent file deletions or object store consistency issues) then the listing will fail with an exception unless `spark.sql.files.ignoreMissingFiles` is `true` (default `false`). In previous versions, these missing files or subdirectories would be ignored. Note that this change of behavior only applies during initial table file listing (or during `REFRESH TABLE`), not during query execution: the net change is that `spark.sql.files.ignoreMissingFiles` is now obeyed during table file listing / query planning, not only at query execution time.

## Upgrading from Spark SQL 2.4 to 2.4.1

  - The value of `spark.executor.heartbeatInterval`, when specified without units like "30" rather than "30s", was
    inconsistently interpreted as both seconds and milliseconds in Spark 2.4.0 in different parts of the code.
    Unitless values are now consistently interpreted as milliseconds. Applications that set values like "30"
    need to specify a value with units like "30s" now, to avoid being interpreted as milliseconds; otherwise, 
    the extremely short interval that results will likely cause applications to fail.

  - When turning a Dataset to another Dataset, Spark will up cast the fields in the original Dataset to the type of corresponding fields in the target DataSet. In version 2.4 and earlier, this up cast is not very strict, e.g. `Seq("str").toDS.as[Int]` fails, but `Seq("str").toDS.as[Boolean]` works and throw NPE during execution. In Spark 3.0, the up cast is stricter and turning String into something else is not allowed, i.e. `Seq("str").toDS.as[Boolean]` will fail during analysis.

## Upgrading From Spark SQL 2.3 to 2.4

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

  - Since Spark 2.4, when there is a struct field in front of the IN operator before a subquery, the inner query must contain a struct field as well. In previous versions, instead, the fields of the struct were compared to the output of the inner query. Eg. if `a` is a `struct(a string, b int)`, in Spark 2.4 `a in (select (1 as a, 'a' as b) from range(1))` is a valid query, while `a in (select 1, 'a' from range(1))` is not. In previous version it was the opposite.

  - In versions 2.2.1+ and 2.3, if `spark.sql.caseSensitive` is set to true, then the `CURRENT_DATE` and `CURRENT_TIMESTAMP` functions incorrectly became case-sensitive and would resolve to columns (unless typed in lower case). In Spark 2.4 this has been fixed and the functions are no longer case-sensitive.

  - Since Spark 2.4, Spark will evaluate the set operations referenced in a query by following a precedence rule as per the SQL standard. If the order is not specified by parentheses, set operations are performed from left to right with the exception that all INTERSECT operations are performed before any UNION, EXCEPT or MINUS operations. The old behaviour of giving equal precedence to all the set operations are preserved under a newly added configuration `spark.sql.legacy.setopsPrecedence.enabled` with a default value of `false`. When this property is set to `true`, spark will evaluate the set operators from left to right as they appear in the query given no explicit ordering is enforced by usage of parenthesis.

  - Since Spark 2.4, Spark will display table description column Last Access value as UNKNOWN when the value was Jan 01 1970.

  - Since Spark 2.4, Spark maximizes the usage of a vectorized ORC reader for ORC files by default. To do that, `spark.sql.orc.impl` and `spark.sql.orc.filterPushdown` change their default values to `native` and `true` respectively. ORC files created by native ORC writer cannot be read by some old Apache Hive releases. Use `spark.sql.orc.impl=hive` to create the files shared with Hive 2.1.1 and older.

  - In PySpark, when Arrow optimization is enabled, previously `toPandas` just failed when Arrow optimization is unable to be used whereas `createDataFrame` from Pandas DataFrame allowed the fallback to non-optimization. Now, both `toPandas` and `createDataFrame` from Pandas DataFrame allow the fallback by default, which can be switched off by `spark.sql.execution.arrow.fallback.enabled`.

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

## Upgrading From Spark SQL 2.3.0 to 2.3.1 and above

  - As of version 2.3.1 Arrow functionality, including `pandas_udf` and `toPandas()`/`createDataFrame()` with `spark.sql.execution.arrow.enabled` set to `True`, has been marked as experimental. These are still evolving and not currently recommended for use in production.

## Upgrading From Spark SQL 2.2 to 2.3

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

  - In PySpark, now we need Pandas 0.19.2 or upper if you want to use Pandas related functionalities, such as `toPandas`, `createDataFrame` from Pandas DataFrame, etc.

  - In PySpark, the behavior of timestamp values for Pandas related functionalities was changed to respect session timezone. If you want to use the old behavior, you need to set a configuration `spark.sql.execution.pandas.respectSessionTimeZone` to `False`. See [SPARK-22395](https://issues.apache.org/jira/browse/SPARK-22395) for details.

  - In PySpark, `na.fill()` or `fillna` also accepts boolean and replaces nulls with booleans. In prior Spark versions, PySpark just ignores it and returns the original Dataset/DataFrame.

  - Since Spark 2.3, when either broadcast hash join or broadcast nested loop join is applicable, we prefer to broadcasting the table that is explicitly specified in a broadcast hint. For details, see the section [Broadcast Hint](sql-performance-tuning.html#broadcast-hint-for-sql-queries) and [SPARK-22489](https://issues.apache.org/jira/browse/SPARK-22489).

  - Since Spark 2.3, when all inputs are binary, `functions.concat()` returns an output as binary. Otherwise, it returns as a string. Until Spark 2.3, it always returns as a string despite of input types. To keep the old behavior, set `spark.sql.function.concatBinaryAsString` to `true`.

  - Since Spark 2.3, when all inputs are binary, SQL `elt()` returns an output as binary. Otherwise, it returns as a string. Until Spark 2.3, it always returns as a string despite of input types. To keep the old behavior, set `spark.sql.function.eltOutputAsString` to `true`.

 - Since Spark 2.3, by default arithmetic operations between decimals return a rounded value if an exact representation is not possible (instead of returning NULL). This is compliant with SQL ANSI 2011 specification and Hive's new behavior introduced in Hive 2.2 (HIVE-15331). This involves the following changes

    - The rules to determine the result type of an arithmetic operation have been updated. In particular, if the precision / scale needed are out of the range of available values, the scale is reduced up to 6, in order to prevent the truncation of the integer part of the decimals. All the arithmetic operations are affected by the change, ie. addition (`+`), subtraction (`-`), multiplication (`*`), division (`/`), remainder (`%`) and positive module (`pmod`).

    - Literal values used in SQL operations are converted to DECIMAL with the exact precision and scale needed by them.

    - The configuration `spark.sql.decimalOperations.allowPrecisionLoss` has been introduced. It defaults to `true`, which means the new behavior described here; if set to `false`, Spark uses previous rules, ie. it doesn't adjust the needed scale to represent the values and it returns NULL if an exact representation of the value is not possible.

  - In PySpark, `df.replace` does not allow to omit `value` when `to_replace` is not a dictionary. Previously, `value` could be omitted in the other cases and had `None` by default, which is counterintuitive and error-prone.

  - Un-aliased subquery's semantic has not been well defined with confusing behaviors. Since Spark 2.3, we invalidate such confusing cases, for example: `SELECT v.i from (SELECT i FROM v)`, Spark will throw an analysis exception in this case because users should not be able to use the qualifier inside a subquery. See [SPARK-20690](https://issues.apache.org/jira/browse/SPARK-20690) and [SPARK-21335](https://issues.apache.org/jira/browse/SPARK-21335) for more details.

  - When creating a `SparkSession` with `SparkSession.builder.getOrCreate()`, if there is an existing `SparkContext`, the builder was trying to update the `SparkConf` of the existing `SparkContext` with configurations specified to the builder, but the `SparkContext` is shared by all `SparkSession`s, so we should not update them. Since 2.3, the builder comes to not update the configurations. If you want to update them, you need to update them prior to creating a `SparkSession`.

## Upgrading From Spark SQL 2.1 to 2.2

  - Spark 2.1.1 introduced a new configuration key: `spark.sql.hive.caseSensitiveInferenceMode`. It had a default setting of `NEVER_INFER`, which kept behavior identical to 2.1.0. However, Spark 2.2.0 changes this setting's default value to `INFER_AND_SAVE` to restore compatibility with reading Hive metastore tables whose underlying file schema have mixed-case column names. With the `INFER_AND_SAVE` configuration value, on first access Spark will perform schema inference on any Hive metastore table for which it has not already saved an inferred schema. Note that schema inference can be a very time-consuming operation for tables with thousands of partitions. If compatibility with mixed-case column names is not a concern, you can safely set `spark.sql.hive.caseSensitiveInferenceMode` to `NEVER_INFER` to avoid the initial overhead of schema inference. Note that with the new default `INFER_AND_SAVE` setting, the results of the schema inference are saved as a metastore key for future use. Therefore, the initial schema inference occurs only at a table's first access.

  - Since Spark 2.2.1 and 2.3.0, the schema is always inferred at runtime when the data source tables have the columns that exist in both partition schema and data schema. The inferred schema does not have the partitioned columns. When reading the table, Spark respects the partition values of these overlapping columns instead of the values stored in the data source files. In 2.2.0 and 2.1.x release, the inferred schema is partitioned but the data of the table is invisible to users (i.e., the result set is empty).

  - Since Spark 2.2, view definitions are stored in a different way from prior versions. This may cause Spark unable to read views created by prior versions. In such cases, you need to recreate the views using `ALTER VIEW AS` or `CREATE OR REPLACE VIEW AS` with newer Spark versions.

## Upgrading From Spark SQL 2.0 to 2.1

 - Datasource tables now store partition metadata in the Hive metastore. This means that Hive DDLs such as `ALTER TABLE PARTITION ... SET LOCATION` are now available for tables created with the Datasource API.

    - Legacy datasource tables can be migrated to this format via the `MSCK REPAIR TABLE` command. Migrating legacy tables is recommended to take advantage of Hive DDL support and improved planning performance.

    - To determine if a table has been migrated, look for the `PartitionProvider: Catalog` attribute when issuing `DESCRIBE FORMATTED` on the table.
 - Changes to `INSERT OVERWRITE TABLE ... PARTITION ...` behavior for Datasource tables.

    - In prior Spark versions `INSERT OVERWRITE` overwrote the entire Datasource table, even when given a partition specification. Now only partitions matching the specification are overwritten.

    - Note that this still differs from the behavior of Hive tables, which is to overwrite only partitions overlapping with newly inserted data.

## Upgrading From Spark SQL 1.6 to 2.0

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

## Upgrading From Spark SQL 1.5 to 1.6

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

 - Since 1.6.1, withColumn method in sparkR supports adding a new column to or replacing existing columns
   of the same name of a DataFrame.

 - From Spark 1.6, LongType casts to TimestampType expect seconds instead of microseconds. This
   change was made to match the behavior of Hive 1.2 for more consistent type casting to TimestampType
   from numeric types. See [SPARK-11724](https://issues.apache.org/jira/browse/SPARK-11724) for
   details.

## Upgrading From Spark SQL 1.4 to 1.5

 - Optimized execution using manually managed memory (Tungsten) is now enabled by default, along with
   code generation for expression evaluation. These features can both be disabled by setting
   `spark.sql.tungsten.enabled` to `false`.

 - Parquet schema merging is no longer enabled by default. It can be re-enabled by setting
   `spark.sql.parquet.mergeSchema` to `true`.

 - Resolution of strings to columns in python now supports using dots (`.`) to qualify the column or
   access nested values. For example `df['table.column.nestedField']`. However, this means that if
   your column name contains any dots you must now escape them using backticks (e.g., ``table.`column.with.dots`.nested``).

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

 - DataFrame.withColumn method in pySpark supports adding a new column or replacing existing columns of the same name.

## Upgrading from Spark SQL 1.3 to 1.4

#### DataFrame data reader/writer interface

Based on user feedback, we created a new, more fluid API for reading data in (`SQLContext.read`)
and writing data out (`DataFrame.write`),
and deprecated the old APIs (e.g., `SQLContext.parquetFile`, `SQLContext.jsonFile`).

See the API docs for `SQLContext.read` (
  <a href="api/scala/index.html#org.apache.spark.sql.SQLContext@read:DataFrameReader">Scala</a>,
  <a href="api/java/org/apache/spark/sql/SQLContext.html#read()">Java</a>,
  <a href="api/python/pyspark.sql.html#pyspark.sql.SQLContext.read">Python</a>
) and `DataFrame.write` (
  <a href="api/scala/index.html#org.apache.spark.sql.DataFrame@write:DataFrameWriter">Scala</a>,
  <a href="api/java/org/apache/spark/sql/Dataset.html#write()">Java</a>,
  <a href="api/python/pyspark.sql.html#pyspark.sql.DataFrame.write">Python</a>
) more information.


#### DataFrame.groupBy retains grouping columns

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

The largest change that users will notice when upgrading to Spark SQL 1.3 is that `SchemaRDD` has
been renamed to `DataFrame`. This is primarily because DataFrames no longer inherit from RDD
directly, but instead provide most of the functionality that RDDs provide though their own
implementation. DataFrames can still be converted to RDDs by calling the `.rdd` method.

In Scala, there is a type alias from `SchemaRDD` to `DataFrame` to provide source compatibility for
some use cases. It is still recommended that users update their code to use `DataFrame` instead.
Java and Python users will need to update their code.

#### Unification of the Java and Scala APIs

Prior to Spark 1.3 there were separate Java compatible classes (`JavaSQLContext` and `JavaSchemaRDD`)
that mirrored the Scala API. In Spark 1.3 the Java API and Scala API have been unified. Users
of either language should use `SQLContext` and `DataFrame`. In general these classes try to
use types that are usable from both languages (i.e. `Array` instead of language-specific collections).
In some cases where no common type exists (e.g., for passing in closures or Maps) function overloading
is used instead.

Additionally, the Java specific types API has been removed. Users of both Scala and Java should
use the classes present in `org.apache.spark.sql.types` to describe schema programmatically.


#### Isolation of Implicit Conversions and Removal of dsl Package (Scala-only)

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

Spark 1.3 removes the type aliases that were present in the base sql package for `DataType`. Users
should instead import the classes in `org.apache.spark.sql.types`

#### UDF Registration Moved to `sqlContext.udf` (Java & Scala)

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

#### Python DataTypes No Longer Singletons

When using DataTypes in Python you will need to construct them (i.e. `StringType()`) instead of
referencing a singleton.
