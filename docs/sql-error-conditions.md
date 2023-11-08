---
layout: global
title: Error Conditions
displayTitle: Error Conditions
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

This is a list of common, named error conditions returned by Spark SQL.

Also see [SQLSTATE Codes](sql-error-conditions-sqlstates.html).

### AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION

SQLSTATE: none assigned

Non-deterministic expression `<sqlExpr>` should not appear in the arguments of an aggregate function.

### ALL_PARTITION_COLUMNS_NOT_ALLOWED

SQLSTATE: none assigned

Cannot use all columns for partition columns.

### ALTER_TABLE_COLUMN_DESCRIPTOR_DUPLICATE

[SQLSTATE: 42710](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

ALTER TABLE `<type>` column `<columnName>` specifies descriptor "`<optionName>`" more than once, which is invalid.

### AMBIGUOUS_ALIAS_IN_NESTED_CTE

SQLSTATE: none assigned

Name `<name>` is ambiguous in nested CTE.
Please set `<config>` to "CORRECTED" so that name defined in inner CTE takes precedence. If set it to "LEGACY", outer CTE definitions will take precedence.
See '`<docroot>`/sql-migration-guide.html#query-engine'.

### AMBIGUOUS_COLUMN_OR_FIELD

[SQLSTATE: 42702](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Column or field `<name>` is ambiguous and has `<n>` matches.

### AMBIGUOUS_COLUMN_REFERENCE

[SQLSTATE: 42702](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Column `<name>` is ambiguous. It's because you joined several DataFrame together, and some of these DataFrames are the same.
This column points to one of the DataFrame but Spark is unable to figure out which one.
Please alias the DataFrames with different names via `DataFrame.alias` before joining them,
and specify the column using qualified name, e.g. `df.alias("a").join(df.alias("b"), col("a.id") > col("b.id"))`.

### AMBIGUOUS_LATERAL_COLUMN_ALIAS

[SQLSTATE: 42702](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Lateral column alias `<name>` is ambiguous and has `<n>` matches.

### AMBIGUOUS_REFERENCE

[SQLSTATE: 42704](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Reference `<name>` is ambiguous, could be: `<referenceNames>`.

### AMBIGUOUS_REFERENCE_TO_FIELDS

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Ambiguous reference to the field `<field>`. It appears `<count>` times in the schema.

### ARITHMETIC_OVERFLOW

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

`<message>`.`<alternative>` If necessary set `<config>` to "false" to bypass this error.

### [AS_OF_JOIN](sql-error-conditions-as-of-join-error-class.html)

SQLSTATE: none assigned

Invalid as-of join.

For more details see [AS_OF_JOIN](sql-error-conditions-as-of-join-error-class.html)

### AVRO_INCOMPATIBLE_READ_TYPE

SQLSTATE: none assigned

Cannot convert Avro `<avroPath>` to SQL `<sqlPath>` because the original encoded data type is `<avroType>`, however you're trying to read the field as `<sqlType>`, which would lead to an incorrect answer. To allow reading this field, enable the SQL configuration: "spark.sql.legacy.avro.allowIncompatibleSchema".

### BATCH_METADATA_NOT_FOUND

[SQLSTATE: 42K03](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Unable to find batch `<batchMetadataFile>`.

### BINARY_ARITHMETIC_OVERFLOW

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

`<value1>` `<symbol>` `<value2>` caused overflow.

### CALL_ON_STREAMING_DATASET_UNSUPPORTED

SQLSTATE: none assigned

The method `<methodName>` can not be called on streaming Dataset/DataFrame.

### CANNOT_CAST_DATATYPE

[SQLSTATE: 42846](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot cast `<sourceType>` to `<targetType>`.

### CANNOT_CONVERT_PROTOBUF_FIELD_TYPE_TO_SQL_TYPE

SQLSTATE: none assigned

Cannot convert Protobuf `<protobufColumn>` to SQL `<sqlColumn>` because schema is incompatible (protobufType = `<protobufType>`, sqlType = `<sqlType>`).

### CANNOT_CONVERT_PROTOBUF_MESSAGE_TYPE_TO_SQL_TYPE

SQLSTATE: none assigned

Unable to convert `<protobufType>` of Protobuf to SQL type `<toType>`.

### CANNOT_CONVERT_SQL_TYPE_TO_PROTOBUF_FIELD_TYPE

SQLSTATE: none assigned

Cannot convert SQL `<sqlColumn>` to Protobuf `<protobufColumn>` because schema is incompatible (protobufType = `<protobufType>`, sqlType = `<sqlType>`).

### CANNOT_CONVERT_SQL_VALUE_TO_PROTOBUF_ENUM_TYPE

SQLSTATE: none assigned

Cannot convert SQL `<sqlColumn>` to Protobuf `<protobufColumn>` because `<data>` is not in defined values for enum: `<enumString>`.

### CANNOT_DECODE_URL

[SQLSTATE: 22546](sql-error-conditions-sqlstates.html#class-22-data-exception)

The provided URL cannot be decoded: `<url>`. Please ensure that the URL is properly formatted and try again.

### CANNOT_INVOKE_IN_TRANSFORMATIONS

SQLSTATE: none assigned

Dataset transformations and actions can only be invoked by the driver, not inside of other Dataset transformations; for example, dataset1.map(x => dataset2.values.count() * x) is invalid because the values transformation and count action cannot be performed inside of the dataset1.map transformation. For more information, see SPARK-28702.

### CANNOT_LOAD_FUNCTION_CLASS

SQLSTATE: none assigned

Cannot load class `<className>` when registering the function `<functionName>`, please make sure it is on the classpath.

### CANNOT_LOAD_PROTOBUF_CLASS

SQLSTATE: none assigned

Could not load Protobuf class with name `<protobufClassName>`. `<explanation>`.

### CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE

[SQLSTATE: 42825](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Failed to merge incompatible data types `<left>` and `<right>`. Please check the data types of the columns being merged and ensure that they are compatible. If necessary, consider casting the columns to compatible data types before attempting the merge.

### CANNOT_MERGE_SCHEMAS

[SQLSTATE: 42KD9](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Failed merging schemas:
Initial schema:
`<left>`
Schema that cannot be merged with the initial schema:
`<right>`.

### CANNOT_MODIFY_CONFIG

[SQLSTATE: 46110](sql-error-conditions-sqlstates.html#class-46-java-ddl-1)

Cannot modify the value of the Spark config: `<key>`.
See also '`<docroot>`/sql-migration-guide.html#ddl-statements'.

### CANNOT_PARSE_DECIMAL

[SQLSTATE: 22018](sql-error-conditions-sqlstates.html#class-22-data-exception)

Cannot parse decimal. Please ensure that the input is a valid number with optional decimal point or comma separators.

### CANNOT_PARSE_INTERVAL

SQLSTATE: none assigned

Unable to parse `<intervalString>`. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. If the issue persists, please double check that the input value is not null or empty and try again.

### CANNOT_PARSE_JSON_FIELD

[SQLSTATE: 2203G](sql-error-conditions-sqlstates.html#class-22-data-exception)

Cannot parse the field name `<fieldName>` and the value `<fieldValue>` of the JSON token type `<jsonType>` to target Spark data type `<dataType>`.

### CANNOT_PARSE_PROTOBUF_DESCRIPTOR

SQLSTATE: none assigned

Error parsing descriptor bytes into Protobuf FileDescriptorSet.

### CANNOT_PARSE_TIMESTAMP

[SQLSTATE: 22007](sql-error-conditions-sqlstates.html#class-22-data-exception)

`<message>`. If necessary set `<ansiConfig>` to "false" to bypass this error.

### CANNOT_READ_FILE_FOOTER

SQLSTATE: none assigned

Could not read footer for file: `<file>`. Please ensure that the file is in either ORC or Parquet format. If not, please convert it to a valid format. If the file is in the valid format, please check if it is corrupt. If it is, you can choose to either ignore it or fix the corruption.

### CANNOT_RECOGNIZE_HIVE_TYPE

[SQLSTATE: 429BB](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot recognize hive type string: `<fieldType>`, column: `<fieldName>`. The specified data type for the field cannot be recognized by Spark SQL. Please check the data type of the specified field and ensure that it is a valid Spark SQL data type. Refer to the Spark SQL documentation for a list of valid data types and their format. If the data type is correct, please ensure that you are using a supported version of Spark SQL.

### CANNOT_RENAME_ACROSS_SCHEMA

[SQLSTATE: 0AKD0](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

Renaming a `<type>` across schemas is not allowed.

### CANNOT_RESOLVE_STAR_EXPAND

SQLSTATE: none assigned

Cannot resolve `<targetString>`.* given input columns `<columns>`. Please check that the specified table or struct exists and is accessible in the input columns.

### CANNOT_RESTORE_PERMISSIONS_FOR_PATH

SQLSTATE: none assigned

Failed to set permissions on created path `<path>` back to `<permission>`.

### [CANNOT_UPDATE_FIELD](sql-error-conditions-cannot-update-field-error-class.html)

SQLSTATE: none assigned

Cannot update `<table>` field `<fieldName>` type:

For more details see [CANNOT_UPDATE_FIELD](sql-error-conditions-cannot-update-field-error-class.html)

### CANNOT_UP_CAST_DATATYPE

SQLSTATE: none assigned

Cannot up cast `<expression>` from `<sourceType>` to `<targetType>`.
`<details>`

### CAST_INVALID_INPUT

[SQLSTATE: 22018](sql-error-conditions-sqlstates.html#class-22-data-exception)

The value `<expression>` of the type `<sourceType>` cannot be cast to `<targetType>` because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. If necessary set `<ansiConfig>` to "false" to bypass this error.

### CAST_OVERFLOW

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

The value `<value>` of the type `<sourceType>` cannot be cast to `<targetType>` due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead. If necessary set `<ansiConfig>` to "false" to bypass this error.

### CAST_OVERFLOW_IN_TABLE_INSERT

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

Fail to insert a value of `<sourceType>` type into the `<targetType>` type column `<columnName>` due to an overflow. Use `try_cast` on the input value to tolerate overflow and return NULL instead.

### CODEC_NOT_AVAILABLE

SQLSTATE: none assigned

The codec `<codecName>` is not available. Consider to set the config `<configKey>` to `<configVal>`.

### CODEC_SHORT_NAME_NOT_FOUND

SQLSTATE: none assigned

Cannot find a short name for the codec `<codecName>`.

### COLUMN_ALIASES_IS_NOT_ALLOWED

SQLSTATE: none assigned

Columns aliases are not allowed in `<op>`.

### COLUMN_ALREADY_EXISTS

[SQLSTATE: 42711](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The column `<columnName>` already exists. Consider to choose another name or rename the existing column.

### COLUMN_NOT_DEFINED_IN_TABLE

SQLSTATE: none assigned

`<colType>` column `<colName>` is not defined in table `<tableName>`, defined table columns are: `<tableCols>`.

### COLUMN_NOT_FOUND

[SQLSTATE: 42703](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The column `<colName>` cannot be found. Verify the spelling and correctness of the column name according to the SQL config `<caseSensitiveConfig>`.

### COMPARATOR_RETURNS_NULL

SQLSTATE: none assigned

The comparator has returned a NULL for a comparison between `<firstValue>` and `<secondValue>`. It should return a positive integer for "greater than", 0 for "equal" and a negative integer for "less than". To revert to deprecated behavior where NULL is treated as 0 (equal), you must set "spark.sql.legacy.allowNullComparisonResultInArraySort" to "true".

### CONCURRENT_QUERY

SQLSTATE: none assigned

Another instance of this query was just started by a concurrent session.

### CONCURRENT_STREAM_LOG_UPDATE

SQLSTATE: 40000

Concurrent update to the log. Multiple streaming jobs detected for `<batchId>`.
Please make sure only one streaming job runs on a specific checkpoint location at a time.

### [CONNECT](sql-error-conditions-connect-error-class.html)

SQLSTATE: none assigned

Generic Spark Connect error.

For more details see [CONNECT](sql-error-conditions-connect-error-class.html)

### CONVERSION_INVALID_INPUT

[SQLSTATE: 22018](sql-error-conditions-sqlstates.html#class-22-data-exception)

The value `<str>` (`<fmt>`) cannot be converted to `<targetType>` because it is malformed. Correct the value as per the syntax, or change its format. Use `<suggestion>` to tolerate malformed input and return NULL instead.

### CREATE_PERMANENT_VIEW_WITHOUT_ALIAS

SQLSTATE: none assigned

Not allowed to create the permanent view `<name>` without explicitly assigning an alias for the expression `<attr>`.

### CREATE_TABLE_COLUMN_DESCRIPTOR_DUPLICATE

[SQLSTATE: 42710](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

CREATE TABLE column `<columnName>` specifies descriptor "`<optionName>`" more than once, which is invalid.

### [CREATE_VIEW_COLUMN_ARITY_MISMATCH](sql-error-conditions-create-view-column-arity-mismatch-error-class.html)

[SQLSTATE: 21S01](sql-error-conditions-sqlstates.html#class-21-cardinality-violation)

Cannot create view `<viewName>`, the reason is

For more details see [CREATE_VIEW_COLUMN_ARITY_MISMATCH](sql-error-conditions-create-view-column-arity-mismatch-error-class.html)

### [DATATYPE_MISMATCH](sql-error-conditions-datatype-mismatch-error-class.html)

[SQLSTATE: 42K09](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot resolve `<sqlExpr>` due to data type mismatch:

For more details see [DATATYPE_MISMATCH](sql-error-conditions-datatype-mismatch-error-class.html)

### DATATYPE_MISSING_SIZE

[SQLSTATE: 42K01](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

DataType `<type>` requires a length parameter, for example `<type>`(10). Please specify the length.

### DATA_SOURCE_NOT_FOUND

[SQLSTATE: 42K02](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Failed to find the data source: `<provider>`. Please find packages at `https://spark.apache.org/third-party-projects.html`.

### DATETIME_OVERFLOW

[SQLSTATE: 22008](sql-error-conditions-sqlstates.html#class-22-data-exception)

Datetime operation overflow: `<operation>`.

### DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

Decimal precision `<precision>` exceeds max precision `<maxPrecision>`.

### DEFAULT_DATABASE_NOT_EXISTS

[SQLSTATE: 42704](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Default database `<defaultDatabase>` does not exist, please create it first or change default database to ``<defaultDatabase>``.

### DISTINCT_WINDOW_FUNCTION_UNSUPPORTED

SQLSTATE: none assigned

Distinct window functions are not supported: `<windowExpr>`.

### DIVIDE_BY_ZERO

[SQLSTATE: 22012](sql-error-conditions-sqlstates.html#class-22-data-exception)

Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set `<config>` to "false" to bypass this error.

### DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT

SQLSTATE: none assigned

Duplicated field names in Arrow Struct are not allowed, got `<fieldNames>`.

### DUPLICATED_MAP_KEY

[SQLSTATE: 23505](sql-error-conditions-sqlstates.html#class-23-integrity-constraint-violation)

Duplicate map key `<key>` was found, please check the input data. If you want to remove the duplicated keys, you can set `<mapKeyDedupPolicy>` to "LAST_WIN" so that the key inserted at last takes precedence.

### DUPLICATED_METRICS_NAME

SQLSTATE: none assigned

The metric name is not unique: `<metricName>`. The same name cannot be used for metrics with different results. However multiple instances of metrics with with same result and name are allowed (e.g. self-joins).

### DUPLICATE_CLAUSES

SQLSTATE: none assigned

Found duplicate clauses: `<clauseName>`. Please, remove one of them.

### DUPLICATE_KEY

[SQLSTATE: 23505](sql-error-conditions-sqlstates.html#class-23-integrity-constraint-violation)

Found duplicate keys `<keyColumn>`.

### [DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT](sql-error-conditions-duplicate-routine-parameter-assignment-error-class.html)

[SQLSTATE: 4274K](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Call to function `<functionName>` is invalid because it includes multiple argument assignments to the same parameter name `<parameterName>`.

For more details see [DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT](sql-error-conditions-duplicate-routine-parameter-assignment-error-class.html)

### EMPTY_JSON_FIELD_VALUE

[SQLSTATE: 42604](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Failed to parse an empty string for data type `<dataType>`.

### ENCODER_NOT_FOUND

SQLSTATE: none assigned

Not found an encoder of the type `<typeName>` to Spark SQL internal representation. Consider to change the input type to one of supported at '`<docroot>`/sql-ref-datatypes.html'.

### EVENT_TIME_IS_NOT_ON_TIMESTAMP_TYPE

SQLSTATE: none assigned

The event time `<eventName>` has the invalid type `<eventType>`, but expected "TIMESTAMP".

### EXCEED_LIMIT_LENGTH

SQLSTATE: none assigned

Exceeds char/varchar type length limitation: `<limit>`.

### EXPRESSION_TYPE_IS_NOT_ORDERABLE

SQLSTATE: none assigned

Column expression `<expr>` cannot be sorted because its type `<exprType>` is not orderable.

### FAILED_EXECUTE_UDF

[SQLSTATE: 39000](sql-error-conditions-sqlstates.html#class-39-external-routine-invocation-exception)

Failed to execute user defined function (`<functionName>`: (`<signature>`) => `<result>`).

### FAILED_FUNCTION_CALL

[SQLSTATE: 38000](sql-error-conditions-sqlstates.html#class-38-external-routine-exception)

Failed preparing of the function `<funcName>` for call. Please, double check function's arguments.

### FAILED_PARSE_STRUCT_TYPE

[SQLSTATE: 22018](sql-error-conditions-sqlstates.html#class-22-data-exception)

Failed parsing struct: `<raw>`.

### FAILED_RENAME_PATH

[SQLSTATE: 42K04](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Failed to rename `<sourcePath>` to `<targetPath>` as destination already exists.

### FAILED_RENAME_TEMP_FILE

SQLSTATE: none assigned

Failed to rename temp file `<srcPath>` to `<dstPath>` as FileSystem.rename returned false.

### FIELDS_ALREADY_EXISTS

SQLSTATE: none assigned

Cannot `<op>` column, because `<fieldNames>` already exists in `<struct>`.

### FIELD_NOT_FOUND

[SQLSTATE: 42704](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

No such struct field `<fieldName>` in `<fields>`.

### FORBIDDEN_OPERATION

[SQLSTATE: 42809](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The operation `<statement>` is not allowed on the `<objectType>`: `<objectName>`.

### GENERATED_COLUMN_WITH_DEFAULT_VALUE

SQLSTATE: none assigned

A column cannot have both a default value and a generation expression but column `<colName>` has default value: (`<defaultValue>`) and generation expression: (`<genExpr>`).

### GRAPHITE_SINK_INVALID_PROTOCOL

SQLSTATE: none assigned

Invalid Graphite protocol: `<protocol>`.

### GRAPHITE_SINK_PROPERTY_MISSING

SQLSTATE: none assigned

Graphite sink requires '`<property>`' property.

### GROUPING_COLUMN_MISMATCH

[SQLSTATE: 42803](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Column of grouping (`<grouping>`) can't be found in grouping columns `<groupingColumns>`.

### GROUPING_ID_COLUMN_MISMATCH

[SQLSTATE: 42803](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Columns of grouping_id (`<groupingIdColumn>`) does not match grouping columns (`<groupByColumns>`).

### GROUPING_SIZE_LIMIT_EXCEEDED

[SQLSTATE: 54000](sql-error-conditions-sqlstates.html#class-54-program-limit-exceeded)

Grouping sets size cannot be greater than `<maxSize>`.

### GROUP_BY_AGGREGATE

[SQLSTATE: 42903](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Aggregate functions are not allowed in GROUP BY, but found `<sqlExpr>`.

### GROUP_BY_POS_AGGREGATE

[SQLSTATE: 42903](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

GROUP BY `<index>` refers to an expression `<aggExpr>` that contains an aggregate function. Aggregate functions are not allowed in GROUP BY.

### GROUP_BY_POS_OUT_OF_RANGE

[SQLSTATE: 42805](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

GROUP BY position `<index>` is not in select list (valid range is [1, `<size>`]).

### GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE

SQLSTATE: none assigned

The expression `<sqlExpr>` cannot be used as a grouping expression because its data type `<dataType>` is not an orderable data type.

### HLL_INVALID_INPUT_SKETCH_BUFFER

SQLSTATE: none assigned

Invalid call to `<function>`; only valid HLL sketch buffers are supported as inputs (such as those produced by the `hll_sketch_agg` function).

### HLL_INVALID_LG_K

SQLSTATE: none assigned

Invalid call to `<function>`; the `lgConfigK` value must be between `<min>` and `<max>`, inclusive: `<value>`.

### HLL_UNION_DIFFERENT_LG_K

SQLSTATE: none assigned

Sketches have different `lgConfigK` values: `<left>` and `<right>`. Set the `allowDifferentLgConfigK` parameter to true to call `<function>` with different `lgConfigK` values.

### IDENTIFIER_TOO_MANY_NAME_PARTS

[SQLSTATE: 42601](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

`<identifier>` is not a valid identifier as it has more than 2 name parts.

### INCOMPARABLE_PIVOT_COLUMN

[SQLSTATE: 42818](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Invalid pivot column `<columnName>`. Pivot columns must be comparable.

### INCOMPATIBLE_COLUMN_TYPE

[SQLSTATE: 42825](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

`<operator>` can only be performed on tables with compatible column types. The `<columnOrdinalNumber>` column of the `<tableOrdinalNumber>` table is `<dataType1>` type which is not compatible with `<dataType2>` at the same column of the first table.`<hint>`.

### INCOMPATIBLE_DATASOURCE_REGISTER

SQLSTATE: none assigned

Detected an incompatible DataSourceRegister. Please remove the incompatible library from classpath or upgrade it. Error: `<message>`

### [INCOMPATIBLE_DATA_FOR_TABLE](sql-error-conditions-incompatible-data-for-table-error-class.html)

SQLSTATE: KD000

Cannot write incompatible data for the table `<tableName>`:

For more details see [INCOMPATIBLE_DATA_FOR_TABLE](sql-error-conditions-incompatible-data-for-table-error-class.html)

### INCOMPATIBLE_JOIN_TYPES

[SQLSTATE: 42613](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The join types `<joinType1>` and `<joinType2>` are incompatible.

### INCOMPATIBLE_VIEW_SCHEMA_CHANGE

SQLSTATE: none assigned

The SQL query of view `<viewName>` has an incompatible schema change and column `<colName>` cannot be resolved. Expected `<expectedNum>` columns named `<colName>` but got `<actualCols>`.
Please try to re-create the view by running: `<suggestion>`.

### [INCOMPLETE_TYPE_DEFINITION](sql-error-conditions-incomplete-type-definition-error-class.html)

[SQLSTATE: 42K01](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Incomplete complex type:

For more details see [INCOMPLETE_TYPE_DEFINITION](sql-error-conditions-incomplete-type-definition-error-class.html)

### [INCONSISTENT_BEHAVIOR_CROSS_VERSION](sql-error-conditions-inconsistent-behavior-cross-version-error-class.html)

[SQLSTATE: 42K0B](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

You may get a different result due to the upgrading to

For more details see [INCONSISTENT_BEHAVIOR_CROSS_VERSION](sql-error-conditions-inconsistent-behavior-cross-version-error-class.html)

### INCORRECT_END_OFFSET

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

Max offset with `<rowsPerSecond>` rowsPerSecond is `<maxSeconds>`, but it's `<endSeconds>` now.

### INCORRECT_RAMP_UP_RATE

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

Max offset with `<rowsPerSecond>` rowsPerSecond is `<maxSeconds>`, but 'rampUpTimeSeconds' is `<rampUpTimeSeconds>`.

### INDEX_ALREADY_EXISTS

[SQLSTATE: 42710](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot create the index `<indexName>` on table `<tableName>` because it already exists.

### INDEX_NOT_FOUND

[SQLSTATE: 42704](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot find the index `<indexName>` on table `<tableName>`.

### [INSERT_COLUMN_ARITY_MISMATCH](sql-error-conditions-insert-column-arity-mismatch-error-class.html)

[SQLSTATE: 21S01](sql-error-conditions-sqlstates.html#class-21-cardinality-violation)

Cannot write to `<tableName>`, the reason is

For more details see [INSERT_COLUMN_ARITY_MISMATCH](sql-error-conditions-insert-column-arity-mismatch-error-class.html)

### INSERT_PARTITION_COLUMN_ARITY_MISMATCH

[SQLSTATE: 21S01](sql-error-conditions-sqlstates.html#class-21-cardinality-violation)

Cannot write to '`<tableName>`', `<reason>`:
Table columns: `<tableColumns>`.
Partition columns with static values: `<staticPartCols>`.
Data columns: `<dataColumns>`.

### [INSUFFICIENT_TABLE_PROPERTY](sql-error-conditions-insufficient-table-property-error-class.html)

SQLSTATE: none assigned

Can't find table property:

For more details see [INSUFFICIENT_TABLE_PROPERTY](sql-error-conditions-insufficient-table-property-error-class.html)

### INTERNAL_ERROR

[SQLSTATE: XX000](sql-error-conditions-sqlstates.html#class-XX-internal-error)

`<message>`

### INTERNAL_ERROR_BROADCAST

[SQLSTATE: XX000](sql-error-conditions-sqlstates.html#class-XX-internal-error)

`<message>`

### INTERNAL_ERROR_EXECUTOR

[SQLSTATE: XX000](sql-error-conditions-sqlstates.html#class-XX-internal-error)

`<message>`

### INTERNAL_ERROR_MEMORY

[SQLSTATE: XX000](sql-error-conditions-sqlstates.html#class-XX-internal-error)

`<message>`

### INTERNAL_ERROR_NETWORK

[SQLSTATE: XX000](sql-error-conditions-sqlstates.html#class-XX-internal-error)

`<message>`

### INTERNAL_ERROR_SHUFFLE

[SQLSTATE: XX000](sql-error-conditions-sqlstates.html#class-XX-internal-error)

`<message>`

### INTERNAL_ERROR_STORAGE

[SQLSTATE: XX000](sql-error-conditions-sqlstates.html#class-XX-internal-error)

`<message>`

### INTERVAL_ARITHMETIC_OVERFLOW

[SQLSTATE: 22015](sql-error-conditions-sqlstates.html#class-22-data-exception)

`<message>`.`<alternative>`

### INTERVAL_DIVIDED_BY_ZERO

[SQLSTATE: 22012](sql-error-conditions-sqlstates.html#class-22-data-exception)

Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead.

### INVALID_ARRAY_INDEX

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

The index `<indexValue>` is out of bounds. The array has `<arraySize>` elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead. If necessary set `<ansiConfig>` to "false" to bypass this error.

### INVALID_ARRAY_INDEX_IN_ELEMENT_AT

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

The index `<indexValue>` is out of bounds. The array has `<arraySize>` elements. Use `try_element_at` to tolerate accessing element at invalid index and return NULL instead. If necessary set `<ansiConfig>` to "false" to bypass this error.

### INVALID_BITMAP_POSITION

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

The 0-indexed bitmap position `<bitPosition>` is out of bounds. The bitmap has `<bitmapNumBits>` bits (`<bitmapNumBytes>` bytes).

### [INVALID_BOUNDARY](sql-error-conditions-invalid-boundary-error-class.html)

SQLSTATE: none assigned

The boundary `<boundary>` is invalid: `<invalidValue>`.

For more details see [INVALID_BOUNDARY](sql-error-conditions-invalid-boundary-error-class.html)

### INVALID_BUCKET_FILE

SQLSTATE: none assigned

Invalid bucket file: `<path>`.

### INVALID_BYTE_STRING

SQLSTATE: none assigned

The expected format is ByteString, but was `<unsupported>` (`<class>`).

### INVALID_COLUMN_NAME_AS_PATH

[SQLSTATE: 46121](sql-error-conditions-sqlstates.html#class-46-java-ddl-1)

The datasource `<datasource>` cannot save the column `<columnName>` because its name contains some characters that are not allowed in file paths. Please, use an alias to rename it.

### INVALID_COLUMN_OR_FIELD_DATA_TYPE

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Column or field `<name>` is of type `<type>` while it's required to be `<expectedType>`.

### [INVALID_CURSOR](sql-error-conditions-invalid-cursor-error-class.html)

[SQLSTATE: HY109](sql-error-conditions-sqlstates.html#class-HY-cli-specific-condition)

The cursor is invalid.

For more details see [INVALID_CURSOR](sql-error-conditions-invalid-cursor-error-class.html)

### [INVALID_DEFAULT_VALUE](sql-error-conditions-invalid-default-value-error-class.html)

SQLSTATE: none assigned

Failed to execute `<statement>` command because the destination table column `<colName>` has a DEFAULT value `<defaultValue>`,

For more details see [INVALID_DEFAULT_VALUE](sql-error-conditions-invalid-default-value-error-class.html)

### INVALID_DRIVER_MEMORY

SQLSTATE: F0000

System memory `<systemMemory>` must be at least `<minSystemMemory>`. Please increase heap size using the --driver-memory option or "`<config>`" in Spark configuration.

### INVALID_EMPTY_LOCATION

[SQLSTATE: 42K05](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The location name cannot be empty string, but ``<location>`` was given.

### INVALID_ESC

SQLSTATE: none assigned

Found an invalid escape string: `<invalidEscape>`. The escape string must contain only one character.

### INVALID_ESCAPE_CHAR

SQLSTATE: none assigned

`EscapeChar` should be a string literal of length one, but got `<sqlExpr>`.

### INVALID_EXECUTOR_MEMORY

SQLSTATE: F0000

Executor memory `<executorMemory>` must be at least `<minSystemMemory>`. Please increase executor memory using the --executor-memory option or "`<config>`" in Spark configuration.

### INVALID_EXTRACT_BASE_FIELD_TYPE

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Can't extract a value from `<base>`. Need a complex type [STRUCT, ARRAY, MAP] but got `<other>`.

### INVALID_EXTRACT_FIELD

[SQLSTATE: 42601](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot extract `<field>` from `<expr>`.

### INVALID_EXTRACT_FIELD_TYPE

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Field name should be a non-null string literal, but it's `<extraction>`.

### INVALID_FIELD_NAME

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Field name `<fieldName>` is invalid: `<path>` is not a struct.

### [INVALID_FORMAT](sql-error-conditions-invalid-format-error-class.html)

[SQLSTATE: 42601](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The format is invalid: `<format>`.

For more details see [INVALID_FORMAT](sql-error-conditions-invalid-format-error-class.html)

### INVALID_FRACTION_OF_SECOND

[SQLSTATE: 22023](sql-error-conditions-sqlstates.html#class-22-data-exception)

The fraction of sec must be zero. Valid range is [0, 60]. If necessary set `<ansiConfig>` to "false" to bypass this error.

### [INVALID_HANDLE](sql-error-conditions-invalid-handle-error-class.html)

[SQLSTATE: HY000](sql-error-conditions-sqlstates.html#class-HY-cli-specific-condition)

The handle `<handle>` is invalid.

For more details see [INVALID_HANDLE](sql-error-conditions-invalid-handle-error-class.html)

### INVALID_HIVE_COLUMN_NAME

SQLSTATE: none assigned

Cannot create the table `<tableName>` having the nested column `<columnName>` whose name contains invalid characters `<invalidChars>` in Hive metastore.

### INVALID_IDENTIFIER

[SQLSTATE: 42602](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The identifier `<ident>` is invalid. Please, consider quoting it with back-quotes as ``<ident>``.

### INVALID_INDEX_OF_ZERO

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

The index 0 is invalid. An index shall be either `< 0 or >` 0 (the first element has index 1).

### [INVALID_INLINE_TABLE](sql-error-conditions-invalid-inline-table-error-class.html)

SQLSTATE: none assigned

Invalid inline table.

For more details see [INVALID_INLINE_TABLE](sql-error-conditions-invalid-inline-table-error-class.html)

### INVALID_JSON_ROOT_FIELD

[SQLSTATE: 22032](sql-error-conditions-sqlstates.html#class-22-data-exception)

Cannot convert JSON root field to target Spark type.

### INVALID_JSON_SCHEMA_MAP_TYPE

[SQLSTATE: 22032](sql-error-conditions-sqlstates.html#class-22-data-exception)

Input schema `<jsonSchema>` can only contain STRING as a key type for a MAP.

### [INVALID_LAMBDA_FUNCTION_CALL](sql-error-conditions-invalid-lambda-function-call-error-class.html)

SQLSTATE: none assigned

Invalid lambda function call.

For more details see [INVALID_LAMBDA_FUNCTION_CALL](sql-error-conditions-invalid-lambda-function-call-error-class.html)

### INVALID_LATERAL_JOIN_TYPE

[SQLSTATE: 42613](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The `<joinType>` JOIN with LATERAL correlation is not allowed because an OUTER subquery cannot correlate to its join partner. Remove the LATERAL correlation or use an INNER JOIN, or LEFT OUTER JOIN instead.

### [INVALID_LIMIT_LIKE_EXPRESSION](sql-error-conditions-invalid-limit-like-expression-error-class.html)

SQLSTATE: none assigned

The limit like expression `<expr>` is invalid.

For more details see [INVALID_LIMIT_LIKE_EXPRESSION](sql-error-conditions-invalid-limit-like-expression-error-class.html)

### INVALID_NON_DETERMINISTIC_EXPRESSIONS

SQLSTATE: none assigned

The operator expects a deterministic expression, but the actual expression is `<sqlExprs>`.

### INVALID_NUMERIC_LITERAL_RANGE

SQLSTATE: none assigned

Numeric literal `<rawStrippedQualifier>` is outside the valid range for `<typeName>` with minimum value of `<minValue>` and maximum value of `<maxValue>`. Please adjust the value accordingly.

### [INVALID_OBSERVED_METRICS](sql-error-conditions-invalid-observed-metrics-error-class.html)

SQLSTATE: none assigned

Invalid observed metrics.

For more details see [INVALID_OBSERVED_METRICS](sql-error-conditions-invalid-observed-metrics-error-class.html)

### [INVALID_OPTIONS](sql-error-conditions-invalid-options-error-class.html)

[SQLSTATE: 42K06](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Invalid options:

For more details see [INVALID_OPTIONS](sql-error-conditions-invalid-options-error-class.html)

### INVALID_PANDAS_UDF_PLACEMENT

[SQLSTATE: 0A000](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

The group aggregate pandas UDF `<functionList>` cannot be invoked together with as other, non-pandas aggregate functions.

### [INVALID_PARAMETER_VALUE](sql-error-conditions-invalid-parameter-value-error-class.html)

[SQLSTATE: 22023](sql-error-conditions-sqlstates.html#class-22-data-exception)

The value of parameter(s) `<parameter>` in `<functionName>` is invalid:

For more details see [INVALID_PARAMETER_VALUE](sql-error-conditions-invalid-parameter-value-error-class.html)

### [INVALID_PARTITION_OPERATION](sql-error-conditions-invalid-partition-operation-error-class.html)

SQLSTATE: none assigned

The partition command is invalid.

For more details see [INVALID_PARTITION_OPERATION](sql-error-conditions-invalid-partition-operation-error-class.html)

### INVALID_PROPERTY_KEY

[SQLSTATE: 42602](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

`<key>` is an invalid property key, please use quotes, e.g. SET `<key>`=`<value>`.

### INVALID_PROPERTY_VALUE

[SQLSTATE: 42602](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

`<value>` is an invalid property value, please use quotes, e.g. SET `<key>`=`<value>`

### [INVALID_SCHEMA](sql-error-conditions-invalid-schema-error-class.html)

[SQLSTATE: 42K07](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The input schema `<inputSchema>` is not a valid schema string.

For more details see [INVALID_SCHEMA](sql-error-conditions-invalid-schema-error-class.html)

### INVALID_SET_SYNTAX

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Expected format is 'SET', 'SET key', or 'SET key=value'. If you want to include special characters in key, or include semicolon in value, please use backquotes, e.g., SET `key`=`value`.

### INVALID_SQL_ARG

SQLSTATE: none assigned

The argument `<name>` of `sql()` is invalid. Consider to replace it by a SQL literal.

### [INVALID_SQL_SYNTAX](sql-error-conditions-invalid-sql-syntax-error-class.html)

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Invalid SQL syntax:

For more details see [INVALID_SQL_SYNTAX](sql-error-conditions-invalid-sql-syntax-error-class.html)

### [INVALID_SUBQUERY_EXPRESSION](sql-error-conditions-invalid-subquery-expression-error-class.html)

[SQLSTATE: 42823](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Invalid subquery:

For more details see [INVALID_SUBQUERY_EXPRESSION](sql-error-conditions-invalid-subquery-expression-error-class.html)

### INVALID_TEMP_OBJ_REFERENCE

SQLSTATE: none assigned

Cannot create the persistent object `<objName>` of the type `<obj>` because it references to the temporary object `<tempObjName>` of the type `<tempObj>`. Please make the temporary object `<tempObjName>` persistent, or make the persistent object `<objName>` temporary.

### [INVALID_TIME_TRAVEL_TIMESTAMP_EXPR](sql-error-conditions-invalid-time-travel-timestamp-expr-error-class.html)

SQLSTATE: none assigned

The time travel timestamp expression `<expr>` is invalid.

For more details see [INVALID_TIME_TRAVEL_TIMESTAMP_EXPR](sql-error-conditions-invalid-time-travel-timestamp-expr-error-class.html)

### INVALID_TYPED_LITERAL

[SQLSTATE: 42604](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The value of the typed literal `<valueType>` is invalid: `<value>`.

### INVALID_UDF_IMPLEMENTATION

SQLSTATE: none assigned

Function `<funcName>` does not implement ScalarFunction or AggregateFunction.

### INVALID_URL

SQLSTATE: none assigned

The url is invalid: `<url>`. If necessary set `<ansiConfig>` to "false" to bypass this error.

### INVALID_USAGE_OF_STAR_OR_REGEX

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Invalid usage of `<elem>` in `<prettyName>`.

### INVALID_VIEW_TEXT

SQLSTATE: none assigned

The view `<viewName>` cannot be displayed due to invalid view text: `<viewText>`. This may be caused by an unauthorized modification of the view or an incorrect query syntax. Please check your query syntax and verify that the view has not been tampered with.

### INVALID_WHERE_CONDITION

[SQLSTATE: 42903](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The WHERE condition `<condition>` contains invalid expressions: `<expressionList>`.
Rewrite the query to avoid window functions, aggregate functions, and generator functions in the WHERE clause.

### INVALID_WINDOW_SPEC_FOR_AGGREGATION_FUNC

SQLSTATE: none assigned

Cannot specify ORDER BY or a window frame for `<aggFunc>`.

### [INVALID_WRITE_DISTRIBUTION](sql-error-conditions-invalid-write-distribution-error-class.html)

SQLSTATE: none assigned

The requested write distribution is invalid.

For more details see [INVALID_WRITE_DISTRIBUTION](sql-error-conditions-invalid-write-distribution-error-class.html)

### JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE

SQLSTATE: none assigned

The join condition `<joinCondition>` has the invalid type `<conditionType>`, expected "BOOLEAN".

### LOAD_DATA_PATH_NOT_EXISTS

SQLSTATE: none assigned

LOAD DATA input path does not exist: `<path>`.

### LOCAL_MUST_WITH_SCHEMA_FILE

SQLSTATE: none assigned

LOCAL must be used together with the schema of `file`, but got: ``<actualSchema>``.

### LOCATION_ALREADY_EXISTS

[SQLSTATE: 42710](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot name the managed table as `<identifier>`, as its associated location `<location>` already exists. Please pick a different table name, or remove the existing location first.

### MALFORMED_CSV_RECORD

SQLSTATE: none assigned

Malformed CSV record: `<badRecord>`

### MALFORMED_PROTOBUF_MESSAGE

SQLSTATE: none assigned

Malformed Protobuf messages are detected in message deserialization. Parse Mode: `<failFastMode>`. To process malformed protobuf message as null result, try setting the option 'mode' as 'PERMISSIVE'.

### [MALFORMED_RECORD_IN_PARSING](sql-error-conditions-malformed-record-in-parsing-error-class.html)

[SQLSTATE: 22023](sql-error-conditions-sqlstates.html#class-22-data-exception)

Malformed records are detected in record parsing: `<badRecord>`.
Parse Mode: `<failFastMode>`. To process malformed records as null result, try setting the option 'mode' as 'PERMISSIVE'.

For more details see [MALFORMED_RECORD_IN_PARSING](sql-error-conditions-malformed-record-in-parsing-error-class.html)

### MERGE_CARDINALITY_VIOLATION

[SQLSTATE: 23K01](sql-error-conditions-sqlstates.html#class-23-integrity-constraint-violation)

The ON search condition of the MERGE statement matched a single row from the target table with multiple rows of the source table.
This could result in the target row being operated on more than once with an update or delete operation and is not allowed.

### MISSING_AGGREGATION

[SQLSTATE: 42803](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The non-aggregating expression `<expression>` is based on columns which are not participating in the GROUP BY clause.
Add the columns or the expression to the GROUP BY, aggregate the expression, or use `<expressionAnyValue>` if you do not care which of the values within a group is returned.

### [MISSING_ATTRIBUTES](sql-error-conditions-missing-attributes-error-class.html)

SQLSTATE: none assigned

Resolved attribute(s) `<missingAttributes>` missing from `<input>` in operator `<operator>`.

For more details see [MISSING_ATTRIBUTES](sql-error-conditions-missing-attributes-error-class.html)

### MISSING_GROUP_BY

[SQLSTATE: 42803](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The query does not include a GROUP BY clause. Add GROUP BY or turn it into the window functions using OVER clauses.

### MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION

SQLSTATE: none assigned

The expression `<expr>` does not support more than one source.

### MULTI_UDF_INTERFACE_ERROR

SQLSTATE: none assigned

Not allowed to implement multiple UDF interfaces, UDF class `<className>`.

### NAMED_PARAMETERS_NOT_SUPPORTED

[SQLSTATE: 4274K](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Named parameters are not supported for function `<functionName>`; please retry the query with positional arguments to the function call instead.

### NAMED_PARAMETER_SUPPORT_DISABLED

SQLSTATE: none assigned

Cannot call function `<functionName>` because named argument references are not enabled here. In this case, the named argument reference was `<argument>`. Set "spark.sql.allowNamedFunctionArguments" to "true" to turn on feature.

### NESTED_AGGREGATE_FUNCTION

[SQLSTATE: 42607](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

It is not allowed to use an aggregate function in the argument of another aggregate function. Please use the inner aggregate function in a sub-query.

### NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION

[SQLSTATE: 42613](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

When there are more than one MATCHED clauses in a MERGE statement, only the last MATCHED clause can omit the condition.

### NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION

[SQLSTATE: 42613](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

When there are more than one NOT MATCHED BY SOURCE clauses in a MERGE statement, only the last NOT MATCHED BY SOURCE clause can omit the condition.

### NON_LAST_NOT_MATCHED_BY_TARGET_CLAUSE_OMIT_CONDITION

[SQLSTATE: 42613](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

When there are more than one NOT MATCHED [BY TARGET] clauses in a MERGE statement, only the last NOT MATCHED [BY TARGET] clause can omit the condition.

### NON_LITERAL_PIVOT_VALUES

[SQLSTATE: 42K08](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Literal expressions required for pivot values, found `<expression>`.

### NON_PARTITION_COLUMN

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

PARTITION clause cannot contain the non-partition column: `<columnName>`.

### NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING

SQLSTATE: none assigned

Window function is not supported in `<windowFunc>` (as column `<columnName>`) on streaming DataFrames/Datasets. Structured Streaming only supports time-window aggregation using the WINDOW function. (window specification: `<windowSpec>`)

### [NOT_ALLOWED_IN_FROM](sql-error-conditions-not-allowed-in-from-error-class.html)

SQLSTATE: none assigned

Not allowed in the FROM clause:

For more details see [NOT_ALLOWED_IN_FROM](sql-error-conditions-not-allowed-in-from-error-class.html)

### [NOT_A_CONSTANT_STRING](sql-error-conditions-not-a-constant-string-error-class.html)

[SQLSTATE: 42601](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The expression `<expr>` used for the routine or clause `<name>` must be a constant STRING which is NOT NULL.

For more details see [NOT_A_CONSTANT_STRING](sql-error-conditions-not-a-constant-string-error-class.html)

### NOT_A_PARTITIONED_TABLE

SQLSTATE: none assigned

Operation `<operation>` is not allowed for `<tableIdentWithDB>` because it is not a partitioned table.

### [NOT_NULL_CONSTRAINT_VIOLATION](sql-error-conditions-not-null-constraint-violation-error-class.html)

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Assigning a NULL is not allowed here.

For more details see [NOT_NULL_CONSTRAINT_VIOLATION](sql-error-conditions-not-null-constraint-violation-error-class.html)

### NOT_SUPPORTED_CHANGE_COLUMN

SQLSTATE: none assigned

ALTER TABLE ALTER/CHANGE COLUMN is not supported for changing `<table>`'s column `<originName>` with type `<originType>` to `<newName>` with type `<newType>`.

### NOT_SUPPORTED_COMMAND_FOR_V2_TABLE

[SQLSTATE: 46110](sql-error-conditions-sqlstates.html#class-46-java-ddl-1)

`<cmd>` is not supported for v2 tables.

### NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT

SQLSTATE: none assigned

`<cmd>` is not supported, if you want to enable it, please set "spark.sql.catalogImplementation" to "hive".

### [NOT_SUPPORTED_IN_JDBC_CATALOG](sql-error-conditions-not-supported-in-jdbc-catalog-error-class.html)

[SQLSTATE: 46110](sql-error-conditions-sqlstates.html#class-46-java-ddl-1)

Not supported command in JDBC catalog:

For more details see [NOT_SUPPORTED_IN_JDBC_CATALOG](sql-error-conditions-not-supported-in-jdbc-catalog-error-class.html)

### NO_DEFAULT_COLUMN_VALUE_AVAILABLE

[SQLSTATE: 42608](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Can't determine the default value for `<colName>` since it is not nullable and it has no default value.

### NO_HANDLER_FOR_UDAF

SQLSTATE: none assigned

No handler for UDAF '`<functionName>`'. Use sparkSession.udf.register(...) instead.

### NO_SQL_TYPE_IN_PROTOBUF_SCHEMA

SQLSTATE: none assigned

Cannot find `<catalystFieldPath>` in Protobuf schema.

### NO_UDF_INTERFACE

SQLSTATE: none assigned

UDF class `<className>` doesn't implement any UDF interface.

### NULLABLE_COLUMN_OR_FIELD

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Column or field `<name>` is nullable while it's required to be non-nullable.

### NULLABLE_ROW_ID_ATTRIBUTES

[SQLSTATE: 42000](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Row ID attributes cannot be nullable: `<nullableRowIdAttrs>`.

### NULL_MAP_KEY

[SQLSTATE: 2200E](sql-error-conditions-sqlstates.html#class-22-data-exception)

Cannot use null as map key.

### NUMERIC_OUT_OF_SUPPORTED_RANGE

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

The value `<value>` cannot be interpreted as a numeric since it has more than 38 digits.

### NUMERIC_VALUE_OUT_OF_RANGE

[SQLSTATE: 22003](sql-error-conditions-sqlstates.html#class-22-data-exception)

`<value>` cannot be represented as Decimal(`<precision>`, `<scale>`). If necessary set `<config>` to "false" to bypass this error, and return NULL instead.

### NUM_COLUMNS_MISMATCH

[SQLSTATE: 42826](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

`<operator>` can only be performed on inputs with the same number of columns, but the first input has `<firstNumColumns>` columns and the `<invalidOrdinalNum>` input has `<invalidNumColumns>` columns.

### NUM_TABLE_VALUE_ALIASES_MISMATCH

SQLSTATE: none assigned

Number of given aliases does not match number of output columns. Function name: `<funcName>`; number of aliases: `<aliasesNum>`; number of output columns: `<outColsNum>`.

### OPERATION_CANCELED

[SQLSTATE: HY008](sql-error-conditions-sqlstates.html#class-HY-cli-specific-condition)

Operation has been canceled.

### ORDER_BY_POS_OUT_OF_RANGE

[SQLSTATE: 42805](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

ORDER BY position `<index>` is not in select list (valid range is [1, `<size>`]).

### PARSE_EMPTY_STATEMENT

[SQLSTATE: 42617](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Syntax error, unexpected empty statement.

### PARSE_SYNTAX_ERROR

[SQLSTATE: 42601](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Syntax error at or near `<error>``<hint>`.

### PARTITIONS_ALREADY_EXIST

[SQLSTATE: 428FT](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot ADD or RENAME TO partition(s) `<partitionList>` in table `<tableName>` because they already exist.
Choose a different name, drop the existing partition, or add the IF NOT EXISTS clause to tolerate a pre-existing partition.

### PARTITIONS_NOT_FOUND

[SQLSTATE: 428FT](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The partition(s) `<partitionList>` cannot be found in table `<tableName>`.
Verify the partition specification and table name.
To tolerate the error on drop use ALTER TABLE … DROP IF EXISTS PARTITION.

### PATH_ALREADY_EXISTS

[SQLSTATE: 42K04](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Path `<outputPath>` already exists. Set mode as "overwrite" to overwrite the existing path.

### PATH_NOT_FOUND

[SQLSTATE: 42K03](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Path does not exist: `<path>`.

### PIVOT_VALUE_DATA_TYPE_MISMATCH

[SQLSTATE: 42K09](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Invalid pivot value '`<value>`': value data type `<valueType>` does not match pivot column data type `<pivotType>`.

### PLAN_VALIDATION_FAILED_RULE_EXECUTOR

SQLSTATE: none assigned

The input plan of `<ruleExecutor>` is invalid: `<reason>`

### PLAN_VALIDATION_FAILED_RULE_IN_BATCH

SQLSTATE: none assigned

Rule `<rule>` in batch `<batch>` generated an invalid plan: `<reason>`

### PROTOBUF_DEPENDENCY_NOT_FOUND

SQLSTATE: none assigned

Could not find dependency: `<dependencyName>`.

### PROTOBUF_DESCRIPTOR_FILE_NOT_FOUND

SQLSTATE: none assigned

Error reading Protobuf descriptor file at path: `<filePath>`.

### PROTOBUF_FIELD_MISSING

SQLSTATE: none assigned

Searching for `<field>` in Protobuf schema at `<protobufSchema>` gave `<matchSize>` matches. Candidates: `<matches>`.

### PROTOBUF_FIELD_MISSING_IN_SQL_SCHEMA

SQLSTATE: none assigned

Found `<field>` in Protobuf schema but there is no match in the SQL schema.

### PROTOBUF_FIELD_TYPE_MISMATCH

SQLSTATE: none assigned

Type mismatch encountered for field: `<field>`.

### PROTOBUF_MESSAGE_NOT_FOUND

SQLSTATE: none assigned

Unable to locate Message `<messageName>` in Descriptor.

### PROTOBUF_TYPE_NOT_SUPPORT

SQLSTATE: none assigned

Protobuf type not yet supported: `<protobufType>`.

### RECURSIVE_PROTOBUF_SCHEMA

SQLSTATE: none assigned

Found recursive reference in Protobuf schema, which can not be processed by Spark by default: `<fieldDescriptor>`. try setting the option `recursive.fields.max.depth` 0 to 10. Going beyond 10 levels of recursion is not allowed.

### RECURSIVE_VIEW

SQLSTATE: none assigned

Recursive view `<viewIdent>` detected (cycle: `<newPath>`).

### REF_DEFAULT_VALUE_IS_NOT_ALLOWED_IN_PARTITION

SQLSTATE: none assigned

References to DEFAULT column values are not allowed within the PARTITION clause.

### RENAME_SRC_PATH_NOT_FOUND

[SQLSTATE: 42K03](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Failed to rename as `<sourcePath>` was not found.

### REPEATED_CLAUSE

[SQLSTATE: 42614](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The `<clause>` clause may be used at most once per `<operation>` operation.

### REQUIRED_PARAMETER_NOT_FOUND

[SQLSTATE: 4274K](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot invoke function `<functionName>` because the parameter named `<parameterName>` is required, but the function call did not supply a value. Please update the function call to supply an argument value (either positionally at index `<index>` or by name) and retry the query again.

### REQUIRES_SINGLE_PART_NAMESPACE

[SQLSTATE: 42K05](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

`<sessionCatalog>` requires a single-part namespace, but got `<namespace>`.

### ROUTINE_ALREADY_EXISTS

[SQLSTATE: 42723](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot create the function `<routineName>` because it already exists.
Choose a different name, drop or replace the existing function, or add the IF NOT EXISTS clause to tolerate a pre-existing function.

### ROUTINE_NOT_FOUND

[SQLSTATE: 42883](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The function `<routineName>` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a schema and catalog, verify the current_schema() output, or qualify the name with the correct schema and catalog.
To tolerate the error on drop use DROP FUNCTION IF EXISTS.

### RULE_ID_NOT_FOUND

[SQLSTATE: 22023](sql-error-conditions-sqlstates.html#class-22-data-exception)

Not found an id for the rule name "`<ruleName>`". Please modify RuleIdCollection.scala if you are adding a new rule.

### SCALAR_SUBQUERY_IS_IN_GROUP_BY_OR_AGGREGATE_FUNCTION

SQLSTATE: none assigned

The correlated scalar subquery '`<sqlExpr>`' is neither present in GROUP BY, nor in an aggregate function. Add it to GROUP BY using ordinal position or wrap it in `first()` (or `first_value`) if you don't care which value you get.

### SCALAR_SUBQUERY_TOO_MANY_ROWS

[SQLSTATE: 21000](sql-error-conditions-sqlstates.html#class-21-cardinality-violation)

More than one row returned by a subquery used as an expression.

### SCHEMA_ALREADY_EXISTS

[SQLSTATE: 42P06](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot create schema `<schemaName>` because it already exists.
Choose a different name, drop the existing schema, or add the IF NOT EXISTS clause to tolerate pre-existing schema.

### SCHEMA_NOT_EMPTY

[SQLSTATE: 2BP01](sql-error-conditions-sqlstates.html#class-2B-dependent-privilege-descriptors-still-exist)

Cannot drop a schema `<schemaName>` because it contains objects.
Use DROP SCHEMA ... CASCADE to drop the schema and all its objects.

### SCHEMA_NOT_FOUND

[SQLSTATE: 42704](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The schema `<schemaName>` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a catalog, verify the current_schema() output, or qualify the name with the correct catalog.
To tolerate the error on drop use DROP SCHEMA IF EXISTS.

### SECOND_FUNCTION_ARGUMENT_NOT_INTEGER

[SQLSTATE: 22023](sql-error-conditions-sqlstates.html#class-22-data-exception)

The second argument of `<functionName>` function needs to be an integer.

### SEED_EXPRESSION_IS_UNFOLDABLE

SQLSTATE: none assigned

The seed expression `<seedExpr>` of the expression `<exprWithSeed>` must be foldable.

### SORT_BY_WITHOUT_BUCKETING

SQLSTATE: none assigned

sortBy must be used together with bucketBy.

### SPECIFY_BUCKETING_IS_NOT_ALLOWED

SQLSTATE: none assigned

Cannot specify bucketing information if the table schema is not specified when creating and will be inferred at runtime.

### SPECIFY_PARTITION_IS_NOT_ALLOWED

SQLSTATE: none assigned

It is not allowed to specify partition columns when the table schema is not defined. When the table schema is not provided, schema and partition columns will be inferred.

### SQL_CONF_NOT_FOUND

SQLSTATE: none assigned

The SQL config `<sqlConf>` cannot be found. Please verify that the config exists.

### STAR_GROUP_BY_POS

[SQLSTATE: 0A000](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

Star (*) is not allowed in a select list when GROUP BY an ordinal position is used.

### STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST

SQLSTATE: none assigned

Static partition column `<staticName>` is also specified in the column list.

### STREAM_FAILED

SQLSTATE: none assigned

Query [id = `<id>`, runId = `<runId>`] terminated with exception: `<message>`

### SUM_OF_LIMIT_AND_OFFSET_EXCEEDS_MAX_INT

SQLSTATE: none assigned

The sum of the LIMIT clause and the OFFSET clause must not be greater than the maximum 32-bit integer value (2,147,483,647) but found limit = `<limit>`, offset = `<offset>`.

### TABLE_OR_VIEW_ALREADY_EXISTS

[SQLSTATE: 42P07](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot create table or view `<relationName>` because it already exists.
Choose a different name, drop or replace the existing object, or add the IF NOT EXISTS clause to tolerate pre-existing objects.

### TABLE_OR_VIEW_NOT_FOUND

[SQLSTATE: 42P01](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The table or view `<relationName>` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.
To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS.

### TABLE_VALUED_FUNCTION_TOO_MANY_TABLE_ARGUMENTS

SQLSTATE: none assigned

There are too many table arguments for table-valued function. It allows one table argument, but got: `<num>`. If you want to allow it, please set "spark.sql.allowMultipleTableArguments.enabled" to "true"

### TASK_WRITE_FAILED

SQLSTATE: none assigned

Task failed while writing rows to `<path>`.

### TEMP_TABLE_OR_VIEW_ALREADY_EXISTS

[SQLSTATE: 42P07](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot create the temporary view `<relationName>` because it already exists.
Choose a different name, drop or replace the existing view,  or add the IF NOT EXISTS clause to tolerate pre-existing views.

### TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS

[SQLSTATE: 428EK](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

CREATE TEMPORARY VIEW or the corresponding Dataset APIs only accept single-part view names, but got: `<actualName>`.

### TOO_MANY_ARRAY_ELEMENTS

[SQLSTATE: 54000](sql-error-conditions-sqlstates.html#class-54-program-limit-exceeded)

Cannot initialize array with `<numElements>` elements of size `<size>`.

### UDTF_ALIAS_NUMBER_MISMATCH

SQLSTATE: none assigned

The number of aliases supplied in the AS clause does not match the number of columns output by the UDTF. Expected `<aliasesSize>` aliases, but got `<aliasesNames>`. Please ensure that the number of aliases provided matches the number of columns output by the UDTF.

### UNABLE_TO_ACQUIRE_MEMORY

[SQLSTATE: 53200](sql-error-conditions-sqlstates.html#class-53-insufficient-resources)

Unable to acquire `<requestedBytes>` bytes of memory, got `<receivedBytes>`.

### UNABLE_TO_CONVERT_TO_PROTOBUF_MESSAGE_TYPE

SQLSTATE: none assigned

Unable to convert SQL type `<toType>` to Protobuf type `<protobufType>`.

### UNABLE_TO_INFER_SCHEMA

[SQLSTATE: 42KD9](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Unable to infer schema for `<format>`. It must be specified manually.

### UNBOUND_SQL_PARAMETER

[SQLSTATE: 42P02](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Found the unbound parameter: `<name>`. Please, fix `args` and provide a mapping of the parameter to a SQL literal.

### UNCLOSED_BRACKETED_COMMENT

[SQLSTATE: 42601](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Found an unclosed bracketed comment. Please, append */ at the end of the comment.

### UNEXPECTED_INPUT_TYPE

[SQLSTATE: 42K09](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Parameter `<paramIndex>` of function `<functionName>` requires the `<requiredType>` type, however `<inputSql>` has the type `<inputType>`.

### UNEXPECTED_POSITIONAL_ARGUMENT

[SQLSTATE: 4274K](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot invoke function `<functionName>` because it contains positional argument(s) following the named argument assigned to `<parameterName>`; please rearrange them so the positional arguments come first and then retry the query again.

### UNKNOWN_PROTOBUF_MESSAGE_TYPE

SQLSTATE: none assigned

Attempting to treat `<descriptorName>` as a Message, but it was `<containingType>`.

### UNPIVOT_REQUIRES_ATTRIBUTES

[SQLSTATE: 42K0A](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

UNPIVOT requires all given `<given>` expressions to be columns when no `<empty>` expressions are given. These are not columns: [`<expressions>`].

### UNPIVOT_REQUIRES_VALUE_COLUMNS

[SQLSTATE: 42K0A](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

At least one value column needs to be specified for UNPIVOT, all columns specified as ids.

### UNPIVOT_VALUE_DATA_TYPE_MISMATCH

[SQLSTATE: 42K09](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Unpivot value columns must share a least common type, some types do not: [`<types>`].

### UNPIVOT_VALUE_SIZE_MISMATCH

[SQLSTATE: 428C4](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

All unpivot value columns must have the same size as there are value column names (`<names>`).

### UNRECOGNIZED_PARAMETER_NAME

[SQLSTATE: 4274K](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot invoke function `<functionName>` because the function call included a named argument reference for the argument named `<argumentName>`, but this function does not include any signature containing an argument with this name. Did you mean one of the following? [`<proposal>`].

### UNRECOGNIZED_SQL_TYPE

[SQLSTATE: 42704](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Unrecognized SQL type - name: `<typeName>`, id: `<jdbcType>`.

### UNRESOLVABLE_TABLE_VALUED_FUNCTION

SQLSTATE: none assigned

Could not resolve `<name>` to a table-valued function. Please make sure that `<name>` is defined as a table-valued function and that all required parameters are provided correctly. If `<name>` is not defined, please create the table-valued function before using it. For more information about defining table-valued functions, please refer to the Apache Spark documentation.

### UNRESOLVED_ALL_IN_GROUP_BY

[SQLSTATE: 42803](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot infer grouping columns for GROUP BY ALL based on the select clause. Please explicitly specify the grouping columns.

### [UNRESOLVED_COLUMN](sql-error-conditions-unresolved-column-error-class.html)

[SQLSTATE: 42703](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

A column or function parameter with name `<objectName>` cannot be resolved.

For more details see [UNRESOLVED_COLUMN](sql-error-conditions-unresolved-column-error-class.html)

### [UNRESOLVED_FIELD](sql-error-conditions-unresolved-field-error-class.html)

[SQLSTATE: 42703](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

A field with name `<fieldName>` cannot be resolved with the struct-type column `<columnPath>`.

For more details see [UNRESOLVED_FIELD](sql-error-conditions-unresolved-field-error-class.html)

### [UNRESOLVED_MAP_KEY](sql-error-conditions-unresolved-map-key-error-class.html)

[SQLSTATE: 42703](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot resolve column `<objectName>` as a map key. If the key is a string literal, add the single quotes '' around it.

For more details see [UNRESOLVED_MAP_KEY](sql-error-conditions-unresolved-map-key-error-class.html)

### UNRESOLVED_ROUTINE

[SQLSTATE: 42883](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot resolve function `<routineName>` on search path `<searchPath>`.

### UNRESOLVED_USING_COLUMN_FOR_JOIN

[SQLSTATE: 42703](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

USING column `<colName>` cannot be resolved on the `<side>` side of the join. The `<side>`-side columns: [`<suggestion>`].

### UNSET_NONEXISTENT_PROPERTIES

SQLSTATE: none assigned

Attempted to unset non-existent properties [`<properties>`] in table `<table>`.

### [UNSUPPORTED_ADD_FILE](sql-error-conditions-unsupported-add-file-error-class.html)

SQLSTATE: none assigned

Don't support add file.

For more details see [UNSUPPORTED_ADD_FILE](sql-error-conditions-unsupported-add-file-error-class.html)

### UNSUPPORTED_ARROWTYPE

[SQLSTATE: 0A000](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

Unsupported arrow type `<typeName>`.

### UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING

SQLSTATE: none assigned

The char/varchar type can't be used in the table schema. If you want Spark treat them as string type as same as Spark 3.0 and earlier, please set "spark.sql.legacy.charVarcharAsString" to "true".

### UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY

SQLSTATE: none assigned

Unsupported data source type for direct query on files: `<dataSourceType>`

### UNSUPPORTED_DATATYPE

[SQLSTATE: 0A000](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

Unsupported data type `<typeName>`.

### UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE

SQLSTATE: none assigned

The `<format>` datasource doesn't support the column `<columnName>` of the type `<columnType>`.

### [UNSUPPORTED_DEFAULT_VALUE](sql-error-conditions-unsupported-default-value-error-class.html)

SQLSTATE: none assigned

DEFAULT column values is not supported.

For more details see [UNSUPPORTED_DEFAULT_VALUE](sql-error-conditions-unsupported-default-value-error-class.html)

### [UNSUPPORTED_DESERIALIZER](sql-error-conditions-unsupported-deserializer-error-class.html)

[SQLSTATE: 0A000](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

The deserializer is not supported:

For more details see [UNSUPPORTED_DESERIALIZER](sql-error-conditions-unsupported-deserializer-error-class.html)

### UNSUPPORTED_EXPRESSION_GENERATED_COLUMN

SQLSTATE: none assigned

Cannot create generated column `<fieldName>` with generation expression `<expressionStr>` because `<reason>`.

### UNSUPPORTED_EXPR_FOR_OPERATOR

SQLSTATE: none assigned

A query operator contains one or more unsupported expressions. Consider to rewrite it to avoid window functions, aggregate functions, and generator functions in the WHERE clause.
Invalid expressions: [`<invalidExprSqls>`]

### UNSUPPORTED_EXPR_FOR_WINDOW

[SQLSTATE: 42P20](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Expression `<sqlExpr>` not supported within a window function.

### [UNSUPPORTED_FEATURE](sql-error-conditions-unsupported-feature-error-class.html)

[SQLSTATE: 0A000](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

The feature is not supported:

For more details see [UNSUPPORTED_FEATURE](sql-error-conditions-unsupported-feature-error-class.html)

### [UNSUPPORTED_GENERATOR](sql-error-conditions-unsupported-generator-error-class.html)

[SQLSTATE: 0A000](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

The generator is not supported:

For more details see [UNSUPPORTED_GENERATOR](sql-error-conditions-unsupported-generator-error-class.html)

### UNSUPPORTED_GROUPING_EXPRESSION

SQLSTATE: none assigned

grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup.

### [UNSUPPORTED_INSERT](sql-error-conditions-unsupported-insert-error-class.html)

SQLSTATE: none assigned

Can't insert into the target.

For more details see [UNSUPPORTED_INSERT](sql-error-conditions-unsupported-insert-error-class.html)

### [UNSUPPORTED_MERGE_CONDITION](sql-error-conditions-unsupported-merge-condition-error-class.html)

SQLSTATE: none assigned

MERGE operation contains unsupported `<condName>` condition.

For more details see [UNSUPPORTED_MERGE_CONDITION](sql-error-conditions-unsupported-merge-condition-error-class.html)

### [UNSUPPORTED_OVERWRITE](sql-error-conditions-unsupported-overwrite-error-class.html)

SQLSTATE: none assigned

Can't overwrite the target that is also being read from.

For more details see [UNSUPPORTED_OVERWRITE](sql-error-conditions-unsupported-overwrite-error-class.html)

### [UNSUPPORTED_SAVE_MODE](sql-error-conditions-unsupported-save-mode-error-class.html)

SQLSTATE: none assigned

The save mode `<saveMode>` is not supported for:

For more details see [UNSUPPORTED_SAVE_MODE](sql-error-conditions-unsupported-save-mode-error-class.html)

### [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY](sql-error-conditions-unsupported-subquery-expression-category-error-class.html)

[SQLSTATE: 0A000](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

Unsupported subquery expression:

For more details see [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY](sql-error-conditions-unsupported-subquery-expression-category-error-class.html)

### UNSUPPORTED_TYPED_LITERAL

[SQLSTATE: 0A000](sql-error-conditions-sqlstates.html#class-0A-feature-not-supported)

Literals of the type `<unsupportedType>` are not supported. Supported types are `<supportedTypes>`.

### UNTYPED_SCALA_UDF

SQLSTATE: none assigned

You're using untyped Scala UDF, which does not have the input type information. Spark may blindly pass null to the Scala closure with primitive-type argument, and the closure will see the default value of the Java type for the null argument, e.g. `udf((x: Int) => x, IntegerType)`, the result is 0 for null input. To get rid of this error, you could:
1. use typed Scala UDF APIs(without return type parameter), e.g. `udf((x: Int) => x)`.
2. use Java UDF APIs, e.g. `udf(new UDF1[String, Integer] { override def call(s: String): Integer = s.length() }, IntegerType)`, if input types are all non primitive.
3. set "spark.sql.legacy.allowUntypedScalaUDF" to "true" and use this API with caution.

### VIEW_ALREADY_EXISTS

[SQLSTATE: 42P07](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

Cannot create view `<relationName>` because it already exists.
Choose a different name, drop or replace the existing object, or add the IF NOT EXISTS clause to tolerate pre-existing objects.

### VIEW_NOT_FOUND

[SQLSTATE: 42P01](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The view `<relationName>` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.
To tolerate the error on drop use DROP VIEW IF EXISTS.

### WINDOW_FUNCTION_AND_FRAME_MISMATCH

SQLSTATE: none assigned

`<funcName>` function can only be evaluated in an ordered row-based window frame with a single offset: `<windowExpr>`.

### WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE

SQLSTATE: none assigned

Window function `<funcName>` requires an OVER clause.

### WRITE_STREAM_NOT_ALLOWED

SQLSTATE: none assigned

`writeStream` can be called only on streaming Dataset/DataFrame.

### WRONG_COMMAND_FOR_OBJECT_TYPE

SQLSTATE: none assigned

The operation `<operation>` requires a `<requiredType>`. But `<objectName>` is a `<foundType>`. Use `<alternative>` instead.

### [WRONG_NUM_ARGS](sql-error-conditions-wrong-num-args-error-class.html)

[SQLSTATE: 42605](sql-error-conditions-sqlstates.html#class-42-syntax-error-or-access-rule-violation)

The `<functionName>` requires `<expectedNum>` parameters but the actual number is `<actualNum>`.

For more details see [WRONG_NUM_ARGS](sql-error-conditions-wrong-num-args-error-class.html)
