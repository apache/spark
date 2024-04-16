---
layout: global
title: SQLSTATE Codes
displayTitle: SQLSTATE Codes
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

Most error classes returned by Spark SQL are associated with a 5 character `SQLSTATE`.
A `SQLSTATE` is a SQL standard encoding for error conditions commonly used by `JDBC`, `ODBC`, and other client APIs.

A `SQLSTATE` consists of two portions: A two character class, and a three character subclass.
Each character must be a digit `'0'` to `'9'` or `'A'` to `'Z'`.

While many `SQLSTATE` values are prescribed by the SQL standard, others are common in the industry, specific to Spark.

For an ordered list of error classes see: [Error Conditions in Spark SQL](sql-error-conditions.html)

Spark SQL uses the following `SQLSTATE` classes:

## Class `0A`: feature not supported

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>0A000</td>
  <td>feature not supported</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#invalid_pandas_udf_placement">INVALID_PANDAS_UDF_PLACEMENT</a>, <a href="sql-error-conditions.html#star_group_by_pos">STAR_GROUP_BY_POS</a>, <a href="sql-error-conditions.html#unsupported_arrowtype">UNSUPPORTED_ARROWTYPE</a>, <a href="sql-error-conditions.html#unsupported_datatype">UNSUPPORTED_DATATYPE</a>, <a href="unsupported-deserializer-error-class.md">UNSUPPORTED_DESERIALIZER</a>, <a href="unsupported-feature-error-class.md">UNSUPPORTED_FEATURE</a>, <a href="unsupported-generator-error-class.md">UNSUPPORTED_GENERATOR</a>, <a href="unsupported-subquery-expression-category-error-class.md">UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY</a>, <a href="sql-error-conditions.html#unsupported_typed_literal">UNSUPPORTED_TYPED_LITERAL</a>
  </td>
</tr>

</table>
## Class `21`: cardinality violation

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>21000</td>
  <td>cardinality violation</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#scalar_subquery_too_many_rows">SCALAR_SUBQUERY_TOO_MANY_ROWS</a>
  </td>
</tr>

</table>
## Class `22`: data exception

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>22003</td>
  <td>numeric value out of range</td>
</tr>
<tr>
  <td></td>
  <td><a href="arithmetic-overflow-error-class.md">ARITHMETIC_OVERFLOW</a>, <a href="sql-error-conditions.html#cast_overflow">CAST_OVERFLOW</a>, <a href="sql-error-conditions.html#cast_overflow_in_table_insert">CAST_OVERFLOW_IN_TABLE_INSERT</a>, <a href="sql-error-conditions.html#decimal_precision_exceeds_max_precision">DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION</a>, <a href="sql-error-conditions.html#invalid_index_of_zero">INVALID_INDEX_OF_ZERO</a>, <a href="sql-error-conditions.html#incorrect_ramp_up_rate">INCORRECT_RAMP_UP_RATE</a>, <a href="invalid-array-index-error-class.md">INVALID_ARRAY_INDEX</a>, <a href="invalid-array-index-in-element-at-error-class.md">INVALID_ARRAY_INDEX_IN_ELEMENT_AT</a>, <a href="sql-error-conditions.html#numeric_out_of_supported_range">NUMERIC_OUT_OF_SUPPORTED_RANGE</a>, <a href="sql-error-conditions.html#numeric_value_out_of_range">NUMERIC_VALUE_OUT_OF_RANGE</a>
  </td>
</tr>
    <tr>
  <td>22007</td>
  <td>invalid datetime format</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#cannot_parse_timestamp">CANNOT_PARSE_TIMESTAMP</a>
  </td>
</tr>
    <tr>
  <td>22008</td>
  <td>datetime field overflow</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#datetime_overflow">DATETIME_OVERFLOW</a>
  </td>
</tr>
    <tr>
  <td>2200E</td>
  <td>null value in array target</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#null_map_key">NULL_MAP_KEY</a>
  </td>
</tr>
    <tr>
  <td>22012</td>
  <td>division by zero</td>
</tr>
<tr>
  <td></td>
  <td><a href="divide-by-zero-error-class.md">DIVIDE_BY_ZERO</a>, <a href="sql-error-conditions.html#interval_divided_by_zero">INTERVAL_DIVIDED_BY_ZERO</a>
  </td>
</tr>
    <tr>
  <td>22015</td>
  <td>interval field overflow</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#interval_arithmetic_overflow">INTERVAL_ARITHMETIC_OVERFLOW</a>
  </td>
</tr>
    <tr>
  <td>22018</td>
  <td>invalid character value for cast</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#cannot_parse_decimal">CANNOT_PARSE_DECIMAL</a>, <a href="cast-invalid-input-error-class.md">CAST_INVALID_INPUT</a>, <a href="sql-error-conditions.html#conversion_invalid_input">CONVERSION_INVALID_INPUT</a>
  </td>
</tr>
    <tr>
  <td>22023</td>
  <td>invalid parameter value</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#invalid_fraction_of_second">INVALID_FRACTION_OF_SECOND</a>, <a href="invalid-parameter-value-error-class.md">INVALID_PARAMETER_VALUE</a>, <a href="sql-error-conditions.html#second_function_argument_not_integer">SECOND_FUNCTION_ARGUMENT_NOT_INTEGER</a>
  </td>
</tr>
    <tr>
  <td>22032</td>
  <td>invalid JSON text</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#invalid_json_root_field">INVALID_JSON_ROOT_FIELD</a>, <a href="sql-error-conditions.html#invalid_json_schema_map_type">INVALID_JSON_SCHEMA_MAP_TYPE</a>
  </td>
</tr>
    <tr>
  <td>2203G</td>
  <td>sql_json_item_cannot_be_cast_to_target_type</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#cannot_parse_json_field">CANNOT_PARSE_JSON_FIELD</a>
  </td>
</tr>
    <tr>
  <td>22546</td>
  <td>The value for a routine argument is not valid.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#cannot_decode_url">CANNOT_DECODE_URL</a>
  </td>
</tr>

</table>
## Class `23`: integrity constraint violation

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>23505</td>
  <td>A violation of the constraint imposed by a unique index or a unique constraint occurred.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#duplicated_map_key">DUPLICATED_MAP_KEY</a>, <a href="sql-error-conditions.html#duplicate_key">DUPLICATE_KEY</a>
  </td>
</tr>

</table>
## Class `2B`: dependent privilege descriptors still exist

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>2BP01</td>
  <td>dependent_objects_still_exist</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#schema_not_empty">SCHEMA_NOT_EMPTY</a>
  </td>
</tr>

</table>
## Class `38`: external routine exception

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>38000</td>
  <td>external routine exception</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#failed_function_call">FAILED_FUNCTION_CALL</a>
  </td>
</tr>

</table>
## Class `39`: external routine invocation exception

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>39000</td>
  <td>external routine invocation exception</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#failed_execute_udf">FAILED_EXECUTE_UDF</a>
  </td>
</tr>

</table>
## Class `42`: syntax error or access rule violation

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>42000</td>
  <td>syntax error or access rule violation</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#ambiguous_reference_to_fields">AMBIGUOUS_REFERENCE_TO_FIELDS</a>, <a href="sql-error-conditions.html#invalid_column_or_field_data_type">INVALID_COLUMN_OR_FIELD_DATA_TYPE</a>, <a href="sql-error-conditions.html#invalid_extract_base_field_type">INVALID_EXTRACT_BASE_FIELD_TYPE</a>, <a href="sql-error-conditions.html#invalid_extract_field_type">INVALID_EXTRACT_FIELD_TYPE</a>, <a href="sql-error-conditions.html#invalid_field_name">INVALID_FIELD_NAME</a>, <a href="sql-error-conditions.html#invalid_set_syntax">INVALID_SET_SYNTAX</a>, <a href="sql-error-conditions.html#invalid_sql_syntax">INVALID_SQL_SYNTAX</a>, <a href="sql-error-conditions.html#non_partition_column">NON_PARTITION_COLUMN</a>, <a href="not-null-constraint-violation-error-class.md">NOT_NULL_CONSTRAINT_VIOLATION</a>, <a href="sql-error-conditions.html#nullable_column_or_field">NULLABLE_COLUMN_OR_FIELD</a>, <a href="sql-error-conditions.html#nullable_row_id_attributes">NULLABLE_ROW_ID_ATTRIBUTES</a>
  </td>
</tr>
<tr>
  <td>42001</td>
  <td>Invalid encoder error</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#invalid_expression_encoder">INVALID_EXPRESSION_ENCODER</a>
</td>
</tr>
    <tr>
  <td>42601</td>
  <td>A character, token, or clause is invalid or missing.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#identifier_too_many_name_parts">IDENTIFIER_TOO_MANY_NAME_PARTS</a>, <a href="sql-error-conditions.html#invalid_extract_field">INVALID_EXTRACT_FIELD</a>, <a href="invalid-format-error-class.md">INVALID_FORMAT</a>, <a href="sql-error-conditions.html#parse_syntax_error">PARSE_SYNTAX_ERROR</a>, <a href="sql-error-conditions.html#unclosed_bracketed_comment">UNCLOSED_BRACKETED_COMMENT</a>
  </td>
</tr>
    <tr>
  <td>42602</td>
  <td>A character that is invalid in a name has been detected.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#invalid_identifier">INVALID_IDENTIFIER</a>, <a href="sql-error-conditions.html#invalid_property_key">INVALID_PROPERTY_KEY</a>, <a href="sql-error-conditions.html#invalid_property_value">INVALID_PROPERTY_VALUE</a>
  </td>
</tr>
    <tr>
  <td>42604</td>
  <td>An invalid numeric or string constant has been detected.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#empty_json_field_value">EMPTY_JSON_FIELD_VALUE</a>, <a href="sql-error-conditions.html#invalid_typed_literal">INVALID_TYPED_LITERAL</a>
  </td>
</tr>
    <tr>
  <td>42605</td>
  <td>The number of arguments specified for a scalar function is invalid.</td>
</tr>
<tr>
  <td></td>
  <td><a href="wrong-num-args-error-class.md">WRONG_NUM_ARGS</a>
  </td>
</tr>
    <tr>
  <td>42607</td>
  <td>An operand of an aggregate function or CONCAT operator is invalid.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#nested_aggregate_function">NESTED_AGGREGATE_FUNCTION</a>
  </td>
</tr>
    <tr>
  <td>42613</td>
  <td>Clauses are mutually exclusive.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#incompatible_join_types">INCOMPATIBLE_JOIN_TYPES</a>, <a href="sql-error-conditions.html#invalid_lateral_join_type">INVALID_LATERAL_JOIN_TYPE</a>, <a href="sql-error-conditions.html#non_last_matched_clause_omit_condition">NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION</a>, <a href="sql-error-conditions.html#non_last_not_matched_by_source_clause_omit_condition">NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION</a>, <a href="sql-error-conditions.html#non_last_not_matched_by_target_clause_omit_condition">NON_LAST_NOT_MATCHED_BY_TARGET_CLAUSE_OMIT_CONDITION</a>
  </td>
</tr>
    <tr>
  <td>42614</td>
  <td>A duplicate keyword or clause is invalid.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#repeated_clause">REPEATED_CLAUSE</a>
  </td>
</tr>
    <tr>
  <td>42617</td>
  <td>The statement string is blank or empty.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#parse_empty_statement">PARSE_EMPTY_STATEMENT</a>
  </td>
</tr>
    <tr>
  <td>42702</td>
  <td>A column reference is ambiguous, because of duplicate names.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#ambiguous_column_or_field">AMBIGUOUS_COLUMN_OR_FIELD</a>, <a href="sql-error-conditions.html#ambiguous_lateral_column_alias">AMBIGUOUS_LATERAL_COLUMN_ALIAS</a>
  </td>
</tr>
    <tr>
  <td>42703</td>
  <td>An undefined column or parameter name was detected.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#column_not_found">COLUMN_NOT_FOUND</a>, <a href="unresolved-column-error-class.md">UNRESOLVED_COLUMN</a>, <a href="unresolved-field-error-class.md">UNRESOLVED_FIELD</a>, <a href="unresolved-map-key-error-class.md">UNRESOLVED_MAP_KEY</a>, <a href="sql-error-conditions.html#unresolved_using_column_for_join">UNRESOLVED_USING_COLUMN_FOR_JOIN</a>
  </td>
</tr>
    <tr>
  <td>42704</td>
  <td>An undefined object or constraint name was detected.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#ambiguous_reference">AMBIGUOUS_REFERENCE</a>, <a href="sql-error-conditions.html#default_database_not_exists">DEFAULT_DATABASE_NOT_EXISTS</a>, <a href="sql-error-conditions.html#field_not_found">FIELD_NOT_FOUND</a>, <a href="sql-error-conditions.html#index_not_found">INDEX_NOT_FOUND</a>, <a href="sql-error-conditions.html#schema_not_found">SCHEMA_NOT_FOUND</a>, <a href="sql-error-conditions.html#unrecognized_sql_type">UNRECOGNIZED_SQL_TYPE</a>
  </td>
</tr>
    <tr>
  <td>42710</td>
  <td>A duplicate object or constraint name was detected.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#create_table_column_option_duplicate">CREATE_TABLE_COLUMN_OPTION_DUPLICATE</a>, <a href="sql-error-conditions.html#index_already_exists">INDEX_ALREADY_EXISTS</a>, <a href="sql-error-conditions.html#location_already_exists">LOCATION_ALREADY_EXISTS</a>
  </td>
</tr>
    <tr>
  <td>42711</td>
  <td>A duplicate column name was detected in the object definition or ALTER TABLE statement.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#column_already_exists">COLUMN_ALREADY_EXISTS</a>
  </td>
</tr>
    <tr>
  <td>42723</td>
  <td>A routine with the same signature already exists in the schema, module, or compound block where it is defined.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#routine_already_exists">ROUTINE_ALREADY_EXISTS</a>
  </td>
</tr>
    <tr>
  <td>42803</td>
  <td>A column reference in the SELECT or HAVING clause is invalid, because it is not a grouping column; or a column reference in the GROUP BY clause is invalid.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#grouping_column_mismatch">GROUPING_COLUMN_MISMATCH</a>, <a href="sql-error-conditions.html#grouping_id_column_mismatch">GROUPING_ID_COLUMN_MISMATCH</a>, <a href="missing-aggregation-error-class.md">MISSING_AGGREGATION</a>, <a href="sql-error-conditions.html#missing_group_by">MISSING_GROUP_BY</a>, <a href="sql-error-conditions.html#unresolved_all_in_group_by">UNRESOLVED_ALL_IN_GROUP_BY</a>
  </td>
</tr>
    <tr>
  <td>42805</td>
  <td>An integer in the ORDER BY clause does not identify a column of the result table.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#group_by_pos_out_of_range">GROUP_BY_POS_OUT_OF_RANGE</a>, <a href="sql-error-conditions.html#order_by_pos_out_of_range">ORDER_BY_POS_OUT_OF_RANGE</a>
  </td>
</tr>
    <tr>
  <td>42809</td>
  <td>The identified object is not the type of object to which the statement applies.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#forbidden_operation">FORBIDDEN_OPERATION</a>
  </td>
</tr>
    <tr>
  <td>42818</td>
  <td>The operands of an operator or function are not compatible or comparable.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#incomparable_pivot_column">INCOMPARABLE_PIVOT_COLUMN</a>
  </td>
</tr>
    <tr>
  <td>42823</td>
  <td>Multiple columns are returned from a subquery that only allows one column.</td>
</tr>
<tr>
  <td></td>
  <td><a href="invalid-subquery-expression-error-class.md">INVALID_SUBQUERY_EXPRESSION</a>
  </td>
</tr>
    <tr>
  <td>42825</td>
  <td>The rows of UNION, INTERSECT, EXCEPT, or VALUES do not have compatible columns.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#cannot_merge_incompatible_data_type">CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE</a>, <a href="sql-error-conditions.html#incompatible_column_type">INCOMPATIBLE_COLUMN_TYPE</a>
  </td>
</tr>
    <tr>
  <td>42826</td>
  <td>The rows of UNION, INTERSECT, EXCEPT, or VALUES do not have the same number of columns.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#num_columns_mismatch">NUM_COLUMNS_MISMATCH</a>
  </td>
</tr>
    <tr>
  <td>42846</td>
  <td>Cast from source type to target type is not supported.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#cannot_cast_datatype">CANNOT_CAST_DATATYPE</a>
  </td>
</tr>
    <tr>
  <td>42883</td>
  <td>No routine was found with a matching signature.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#routine_not_found">ROUTINE_NOT_FOUND</a>, <a href="unresolved-routine-error-class.md">UNRESOLVED_ROUTINE</a>
  </td>
</tr>
    <tr>
  <td>428C4</td>
  <td>The number of elements on each side of the predicate operator is not the same.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#unpivot_value_size_mismatch">UNPIVOT_VALUE_SIZE_MISMATCH</a>
  </td>
</tr>
    <tr>
  <td>428EK</td>
  <td>The schema qualifier is not valid.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#temp_view_name_too_many_name_parts">TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS</a>
  </td>
</tr>
    <tr>
  <td>428FT</td>
  <td>The partitioning clause specified on CREATE or ALTER is not valid.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#partitions_already_exist">PARTITIONS_ALREADY_EXIST</a>, <a href="sql-error-conditions.html#partitions_not_found">PARTITIONS_NOT_FOUND</a>
  </td>
</tr>
    <tr>
  <td>42903</td>
  <td>Invalid use of an aggregate function or OLAP function.</td>
</tr>
<tr>
  <td></td>
  <td><a href="group-by-aggregate-error-class.md">GROUP_BY_AGGREGATE</a>, <a href="sql-error-conditions.html#group_by_pos_aggregate">GROUP_BY_POS_AGGREGATE</a>, <a href="sql-error-conditions.html#invalid_where_condition">INVALID_WHERE_CONDITION</a>
  </td>
</tr>
    <tr>
  <td>429BB</td>
  <td>The data type of a column, parameter, or SQL variable is not supported.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#cannot_recognize_hive_type">CANNOT_RECOGNIZE_HIVE_TYPE</a>
  </td>
</tr>
    <tr>
  <td>42K01</td>
  <td>data type not fully specified</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#datatype_missing_size">DATATYPE_MISSING_SIZE</a>, <a href="incomplete-type-definition-error-class.md">INCOMPLETE_TYPE_DEFINITION</a>
  </td>
</tr>
    <tr>
  <td>42K02</td>
  <td>data source not found</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#data_source_not_found">DATA_SOURCE_NOT_FOUND</a>
  </td>
</tr>
    <tr>
  <td>42K03</td>
  <td>File not found</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#path_not_found">PATH_NOT_FOUND</a>, <a href="sql-error-conditions.html#rename_src_path_not_found">RENAME_SRC_PATH_NOT_FOUND</a>
  </td>
</tr>
    <tr>
  <td>42K04</td>
  <td>Duplicate file</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#failed_rename_path">FAILED_RENAME_PATH</a>, <a href="sql-error-conditions.html#path_already_exists">PATH_ALREADY_EXISTS</a>
  </td>
</tr>
    <tr>
  <td>42K05</td>
  <td>Name is not valid</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#invalid_empty_location">INVALID_EMPTY_LOCATION</a>, <a href="sql-error-conditions.html#requires_single_part_namespace">REQUIRES_SINGLE_PART_NAMESPACE</a>
  </td>
</tr>
    <tr>
  <td>42K06</td>
  <td>Invalid type for options</td>
</tr>
<tr>
  <td></td>
  <td><a href="invalid-options-error-class.md">INVALID_OPTIONS</a>
  </td>
</tr>
    <tr>
  <td>42K07</td>
  <td>Not a valid schema literal</td>
</tr>
<tr>
  <td></td>
  <td><a href="invalid-schema-error-class.md">INVALID_SCHEMA</a>
  </td>
</tr>
    <tr>
  <td>42K08</td>
  <td>Not a constant</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#non_literal_pivot_values">NON_LITERAL_PIVOT_VALUES</a>
  </td>
</tr>
    <tr>
  <td>42K09</td>
  <td>Data type mismatch</td>
</tr>
<tr>
  <td></td>
  <td><a href="datatype-mismatch-error-class.md">DATATYPE_MISMATCH</a>, <a href="sql-error-conditions.html#pivot_value_data_type_mismatch">PIVOT_VALUE_DATA_TYPE_MISMATCH</a>, <a href="sql-error-conditions.html#unexpected_input_type">UNEXPECTED_INPUT_TYPE</a>, <a href="sql-error-conditions.html#unpivot_value_data_type_mismatch">UNPIVOT_VALUE_DATA_TYPE_MISMATCH</a>
  </td>
</tr>
    <tr>
  <td>42K0A</td>
  <td>Invalid UNPIVOT clause</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#unpivot_requires_attributes">UNPIVOT_REQUIRES_ATTRIBUTES</a>, <a href="sql-error-conditions.html#unpivot_requires_value_columns">UNPIVOT_REQUIRES_VALUE_COLUMNS</a>
  </td>
</tr>
    <tr>
  <td>42K0B</td>
  <td>Legacy feature blocked</td>
</tr>
<tr>
  <td></td>
  <td><a href="inconsistent-behavior-cross-version-error-class.md">INCONSISTENT_BEHAVIOR_CROSS_VERSION</a>
  </td>
</tr>
    <tr>
  <td>42KD9</td>
  <td>Cannot infer table schema.</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#unable_to_infer_schema">UNABLE_TO_INFER_SCHEMA</a>
  </td>
</tr>
    <tr>
  <td>42P01</td>
  <td>undefined_table</td>
</tr>
<tr>
  <td></td>
  <td><a href="table-or-view-not-found-error-class.md">TABLE_OR_VIEW_NOT_FOUND</a>, <a href="sql-error-conditions.html#view_not_found">VIEW_NOT_FOUND</a>
  </td>
</tr>
    <tr>
  <td>42P02</td>
  <td>undefined_parameter</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#unbound_sql_parameter">UNBOUND_SQL_PARAMETER</a>
  </td>
</tr>
    <tr>
  <td>42P06</td>
  <td>duplicate_schema</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#schema_already_exists">SCHEMA_ALREADY_EXISTS</a>
  </td>
</tr>
    <tr>
  <td>42P07</td>
  <td>duplicate_table</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#table_or_view_already_exists">TABLE_OR_VIEW_ALREADY_EXISTS</a>, <a href="sql-error-conditions.html#temp_table_or_view_already_exists">TEMP_TABLE_OR_VIEW_ALREADY_EXISTS</a>, <a href="sql-error-conditions.html#view_already_exists">VIEW_ALREADY_EXISTS</a>
  </td>
</tr>
    <tr>
  <td>42P20</td>
  <td>windowing_error</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#unsupported_expr_for_window">UNSUPPORTED_EXPR_FOR_WINDOW</a>
  </td>
</tr>

</table>
## Class `46`: java ddl 1

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>46110</td>
  <td>unsupported feature</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#cannot_modify_config">CANNOT_MODIFY_CONFIG</a>
  </td>
</tr>
    <tr>
  <td>46121</td>
  <td>invalid column name</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#invalid_column_name_as_path">INVALID_COLUMN_NAME_AS_PATH</a>
  </td>
</tr>

</table>
## Class `53`: insufficient resources

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>53200</td>
  <td>out_of_memory</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#unable_to_acquire_memory">UNABLE_TO_ACQUIRE_MEMORY</a>
  </td>
</tr>

</table>
## Class `54`: program limit exceeded

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>54000</td>
  <td>program limit exceeded</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#grouping_size_limit_exceeded">GROUPING_SIZE_LIMIT_EXCEEDED</a>, <a href="sql-error-conditions.html#too_many_array_elements">TOO_MANY_ARRAY_ELEMENTS</a>
  </td>
</tr>

</table>
## Class `HY`: CLI-specific condition

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>HY008</td>
  <td>operation canceled</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#operation_canceled">OPERATION_CANCELED</a>
  </td>
</tr>

</table>
## Class `XX`: internal error

<table>
<thead><tr><th>SQLSTATE</th><th>Description and issuing error classes</th></tr></thead>
<tr>
  <td>XX000</td>
  <td>internal error</td>
</tr>
<tr>
  <td></td>
  <td><a href="sql-error-conditions.html#internal_error">INTERNAL_ERROR</a>
  </td>
</tr>

</table>
