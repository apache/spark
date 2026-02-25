..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

========================
Error classes in PySpark
========================

This is a list of common, named error classes returned by PySpark which are defined at `error-conditions.json <https://github.com/apache/spark/blob/master/python/pyspark/errors/error-conditions.json>`_.

When writing PySpark errors, developers must use an error class from the list. If an appropriate error class is not available, add a new one into the list. For more information, please refer to `Contributing Error and Exception <contributing.rst#contributing-error-and-exception>`_.


APPLICATION_NAME_NOT_SET
------------------------

An application name must be set in your configuration.


ARGUMENT_REQUIRED
-----------------

Argument `<arg_name>` is required when <condition>.


ARROW_LEGACY_IPC_FORMAT
-----------------------

Arrow legacy IPC format is not supported in PySpark, please unset ARROW_PRE_0_15_IPC_FORMAT.


ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION
-------------------------------------------

Operations that trigger DataFrame analysis or execution are not allowed in pipeline query functions. Move code outside of the pipeline query function.


ATTRIBUTE_NOT_CALLABLE
----------------------

Attribute `<attr_name>` in provided object `<obj_name>` is not callable.


ATTRIBUTE_NOT_SUPPORTED
-----------------------

Attribute `<attr_name>` is not supported.


AXIS_LENGTH_MISMATCH
--------------------

Length mismatch: Expected axis has <expected_length> element, new values have <actual_length> elements.


BROADCAST_VARIABLE_NOT_LOADED
-----------------------------

Broadcast variable `<variable>` not loaded.


CALL_BEFORE_INITIALIZE
----------------------

Not supported to call `<func_name>` before initialize <object>.


CANNOT_ACCEPT_OBJECT_IN_TYPE
----------------------------

`<data_type>` can not accept object `<obj_name>` in type `<obj_type>`.


CANNOT_ACCESS_TO_DUNDER
-----------------------

Dunder(double underscore) attribute is for internal use only.


CANNOT_APPLY_IN_FOR_COLUMN
--------------------------

Cannot apply 'in' operator against a column: please use 'contains' in a string column or 'array_contains' function for an array column.


CANNOT_BE_EMPTY
---------------

At least one <item> must be specified.


CANNOT_BE_NONE
--------------

Argument `<arg_name>` cannot be None.


CANNOT_CONFIGURE_SPARK_CONNECT
------------------------------

Spark Connect server cannot be configured: Existing [<existing_url>], New [<new_url>].


CANNOT_CONFIGURE_SPARK_CONNECT_MASTER
-------------------------------------

Spark Connect server and Spark master cannot be configured together: Spark master [<master_url>], Spark Connect [<connect_url>].


CANNOT_CONVERT_COLUMN_INTO_BOOL
-------------------------------

Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.


CANNOT_CONVERT_TYPE
-------------------

Cannot convert <from_type> into <to_type>.


CANNOT_DETERMINE_TYPE
---------------------

Some of types cannot be determined after inferring.


CANNOT_GET_BATCH_ID
-------------------

Could not get batch id from <obj_name>.


CANNOT_INFER_ARRAY_ELEMENT_TYPE
-------------------------------

Can not infer the element data type, an non-empty list starting with an non-None value is required.


CANNOT_INFER_EMPTY_SCHEMA
-------------------------

Can not infer schema from an empty dataset.


CANNOT_INFER_SCHEMA_FOR_TYPE
----------------------------

Can not infer schema for type: `<data_type>`.


CANNOT_INFER_TYPE_FOR_FIELD
---------------------------

Unable to infer the type of the field `<field_name>`.


CANNOT_MERGE_TYPE
-----------------

Can not merge type `<data_type1>` and `<data_type2>`.


CANNOT_OPEN_SOCKET
------------------

Can not open socket: <errors>.


CANNOT_PARSE_DATATYPE
---------------------

Unable to parse datatype. <msg>.


CANNOT_PROVIDE_METADATA
-----------------------

Metadata can only be provided for a single column.


CANNOT_REGISTER_UDTF
--------------------

Cannot register the UDTF '<name>': expected a 'UserDefinedTableFunction'. Please make sure the UDTF is correctly defined as a class, and then either wrap it in the `udtf()` function or annotate it with `@udtf(...)`.


CANNOT_SET_TOGETHER
-------------------

<arg_list> should not be set together.


CANNOT_SPECIFY_RETURN_TYPE_FOR_UDF
----------------------------------

returnType can not be specified when `<arg_name>` is a user-defined function, but got <return_type>.


CANNOT_WITHOUT
--------------

Cannot <condition1> without <condition2>.


CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF
-------------------------------------

Calling property or member '<member>' is not supported in PySpark Classic, please use Spark Connect instead.


COLLATION_INVALID_PROVIDER
--------------------------

The value <provider> does not represent a correct collation provider. Supported providers are: [<supportedProviders>].


COLUMN_IN_LIST
--------------

`<func_name>` does not allow a Column in a list.


CONFLICTING_PIPELINE_REFRESH_OPTIONS
------------------------------------

--full-refresh-all option conflicts with <conflicting_option>
The --full-refresh-all option performs a full refresh of all datasets, 
so specifying individual datasets with <conflicting_option> is not allowed.


CONNECT_URL_ALREADY_DEFINED
---------------------------

Only one Spark Connect client URL can be set; however, got a different URL [<new_url>] from the existing [<existing_url>].


CONNECT_URL_NOT_SET
-------------------

Cannot create a Spark Connect session because the Spark Connect remote URL has not been set. Please define the remote URL by setting either the 'spark.remote' option or the 'SPARK_REMOTE' environment variable.


CONTEXT_ONLY_VALID_ON_DRIVER
----------------------------

It appears that you are attempting to reference SparkContext from a broadcast variable, action, or transformation. SparkContext can only be used on the driver, not in code that it run on workers. For more information, see SPARK-5063.


CONTEXT_UNAVAILABLE_FOR_REMOTE_CLIENT
-------------------------------------

Remote client cannot create a SparkContext. Create SparkSession instead.


DATA_SOURCE_EXTRANEOUS_FILTERS
------------------------------

<type>.pushFilters() returned filters that are not part of the input. Make sure that each returned filter is one of the input filters by reference.


DATA_SOURCE_INVALID_RETURN_TYPE
-------------------------------

Unsupported return type ('<type>') from Python data source '<name>'. Expected types: <supported_types>.


DATA_SOURCE_PUSHDOWN_DISABLED
-----------------------------

<type> implements pushFilters() but filter pushdown is disabled because configuration '<conf>' is false. Set it to true to enable filter pushdown.


DATA_SOURCE_RETURN_SCHEMA_MISMATCH
----------------------------------

Return schema mismatch in the result from 'read' method. Expected: <expected> columns, Found: <actual> columns. Make sure the returned values match the required output schema.


DATA_SOURCE_TYPE_MISMATCH
-------------------------

Expected <expected>, but got <actual>.


DATA_SOURCE_UNSUPPORTED_FILTER
------------------------------

Unexpected filter <name>.


DECORATOR_ARGUMENT_NOT_CALLABLE
-------------------------------

The first positional argument passed to @<decorator_name> must be callable. Either add @<decorator_name> with no parameters to your function, or pass options to @<decorator_name> using keyword arguments (e.g. <example_usage>).


DIFFERENT_PANDAS_DATAFRAME
--------------------------

DataFrames are not almost equal:
Left:
<left>
<left_dtype>
Right:
<right>
<right_dtype>


DIFFERENT_PANDAS_INDEX
----------------------

Indices are not almost equal:
Left:
<left>
<left_dtype>
Right:
<right>
<right_dtype>


DIFFERENT_PANDAS_MULTIINDEX
---------------------------

MultiIndices are not almost equal:
Left:
<left>
<left_dtype>
Right:
<right>
<right_dtype>


DIFFERENT_PANDAS_SERIES
-----------------------

Series are not almost equal:
Left:
<left>
<left_dtype>
Right:
<right>
<right_dtype>


DIFFERENT_ROWS
--------------

<error_msg>


DIFFERENT_SCHEMA
----------------

Schemas do not match.
--- actual
+++ expected
<error_msg>


DISALLOWED_TYPE_FOR_CONTAINER
-----------------------------

Argument `<arg_name>`\(type: <arg_type>) should only contain a type in [<allowed_types>], got <item_type>


DUPLICATED_ARTIFACT
-------------------

Duplicate Artifact: <normalized_path>. Artifacts cannot be overwritten.


DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT
-------------------------------------

Duplicated field names in Arrow Struct are not allowed, got <field_names>


ERROR_OCCURRED_WHILE_CALLING
----------------------------

An error occurred while calling <func_name>: <error_msg>.


FIELD_DATA_TYPE_UNACCEPTABLE
----------------------------

<data_type> can not accept object <obj> in type <obj_type>.


FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME
--------------------------------------

<field_name>: <data_type> can not accept object <obj> in type <obj_type>.


FIELD_NOT_NULLABLE
------------------

Field is not nullable, but got None.


FIELD_NOT_NULLABLE_WITH_NAME
----------------------------

<field_name>: This field is not nullable, but got None.


FIELD_STRUCT_LENGTH_MISMATCH
----------------------------

Length of object (<object_length>) does not match with length of fields (<field_length>).


FIELD_STRUCT_LENGTH_MISMATCH_WITH_NAME
--------------------------------------

<field_name>: Length of object (<object_length>) does not match with length of fields (<field_length>).


FIELD_TYPE_MISMATCH
-------------------

<obj> is not an instance of type <data_type>.


FIELD_TYPE_MISMATCH_WITH_NAME
-----------------------------

<field_name>: <obj> is not an instance of type <data_type>.


GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE
-----------------------------------------------------

APIs that define elements of a declarative pipeline can only be invoked within the context of defining a pipeline.


HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN
------------------------------------------

Function `<func_name>` should return Column, got <return_type>.


INCORRECT_CONF_FOR_PROFILE
--------------------------

`spark.python.profile` or `spark.python.profile.memory` configuration
 must be set to `true` to enable Python profile.


INDEX_NOT_POSITIVE
------------------

Index must be positive, got '<index>'.


INDEX_OUT_OF_RANGE
------------------

<arg_name> index out of range, got '<index>'.


INVALID_ARROW_UDTF_RETURN_TYPE
------------------------------

The return type of the arrow-optimized Python UDTF should be of type 'pandas.DataFrame', but the '<func>' method returned a value of type <return_type> with value: <value>.


INVALID_ARROW_UDTF_WITH_ANALYZE
-------------------------------

The arrow UDTF '<name>' is invalid. Arrow UDTFs do not support the 'analyze' method. Please remove the 'analyze' method from '<name>' and specify a returnType instead.


INVALID_BROADCAST_OPERATION
---------------------------

Broadcast can only be <operation> in driver.


INVALID_CALL_ON_UNRESOLVED_OBJECT
---------------------------------

Invalid call to `<func_name>` on unresolved object.


INVALID_CONNECT_URL
-------------------

Invalid URL for Spark Connect: <detail>


INVALID_INTERVAL_CASTING
------------------------

Interval <start_field> to <end_field> is invalid.


INVALID_ITEM_FOR_CONTAINER
--------------------------

All items in `<arg_name>` should be in <allowed_types>, got <item_type>.


INVALID_JSON_DATA_TYPE_FOR_COLLATIONS
-------------------------------------

Collations can only be applied to string types, but the JSON data type is <jsonType>.


INVALID_MULTIPLE_ARGUMENT_CONDITIONS
------------------------------------

[{arg_names}] cannot be <condition>.


INVALID_NDARRAY_DIMENSION
-------------------------

NumPy array input should be of <dimensions> dimensions.


INVALID_NUMBER_OF_DATAFRAMES_IN_GROUP
-------------------------------------

Invalid number of dataframes in group <dataframes_in_group>.


INVALID_PANDAS_UDF
------------------

Invalid function: <detail>


INVALID_PANDAS_UDF_TYPE
-----------------------

`<arg_name>` should be one of the values from PandasUDFType, got <arg_type>


INVALID_RETURN_TYPE_FOR_ARROW_UDF
---------------------------------

Grouped and Cogrouped map Arrow UDF should return StructType for <eval_type>, got <return_type>.


INVALID_RETURN_TYPE_FOR_PANDAS_UDF
----------------------------------

Pandas UDF should return StructType for <eval_type>, got <return_type>.


INVALID_SESSION_UUID_ID
-----------------------

Parameter value <arg_name> must be a valid UUID format: <origin>


INVALID_TIMEOUT_TIMESTAMP
-------------------------

Timeout timestamp (<timestamp>) cannot be earlier than the current watermark (<watermark>).


INVALID_TYPE
------------

Argument `<arg_name>` should not be a <arg_type>.


INVALID_TYPENAME_CALL
---------------------

StructField does not have typeName. Use typeName on its type explicitly instead.


INVALID_TYPE_DF_EQUALITY_ARG
----------------------------

Expected type <expected_type> for `<arg_name>` but got type <actual_type>.


INVALID_UDF_EVAL_TYPE
---------------------

Eval type for UDF must be <eval_type>.


INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE
-----------------------------------------

The UDTF '<name>' is invalid. It has both its return type and an 'analyze' attribute. Please make it have one of either the return type or the 'analyze' static method in '<name>' and try again.


INVALID_UDTF_EVAL_TYPE
----------------------

The eval type for the UDTF '<name>' is invalid. It must be one of <eval_type>.


INVALID_UDTF_HANDLER_TYPE
-------------------------

The UDTF is invalid. The function handler must be a class, but got '<type>'. Please provide a class as the function handler.


INVALID_UDTF_NO_EVAL
--------------------

The UDTF '<name>' is invalid. It does not implement the required 'eval' method. Please implement the 'eval' method in '<name>' and try again.


INVALID_UDTF_RETURN_TYPE
------------------------

The UDTF '<name>' is invalid. It does not specify its return type or implement the required 'analyze' static method. Please specify the return type or implement the 'analyze' static method in '<name>' and try again.


INVALID_WHEN_USAGE
------------------

when() can only be applied on a Column previously generated by when() function, and cannot be applied once otherwise() is applied.


INVALID_WINDOW_BOUND_TYPE
-------------------------

Invalid window bound type: <window_bound_type>.


JAVA_GATEWAY_EXITED
-------------------

Java gateway process exited before sending its port number.


JVM_ATTRIBUTE_NOT_SUPPORTED
---------------------------

Attribute `<attr_name>` is not supported in Spark Connect as it depends on the JVM. If you need to use this attribute, do not use Spark Connect when creating your session. Visit https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession for creating regular Spark Session in detail.


KEY_NOT_EXISTS
--------------

Key `<key>` is not exists.


KEY_VALUE_PAIR_REQUIRED
-----------------------

Key-value pair or a list of pairs is required.


LENGTH_SHOULD_BE_THE_SAME
-------------------------

<arg1> and <arg2> should be of the same length, got <arg1_length> and <arg2_length>.


MALFORMED_VARIANT
-----------------

Variant binary is malformed. Please check the data source is valid.


MASTER_URL_INVALID
------------------

Master must either be yarn or start with spark, k8s, or local.


MASTER_URL_NOT_SET
------------------

A master URL must be set in your configuration.


MEMORY_PROFILE_INVALID_SOURCE
-----------------------------

Memory profiler can only be used on editors with line numbers.


MISSING_LIBRARY_FOR_PROFILER
----------------------------

Install the 'memory_profiler' library in the cluster to enable memory profiling.


MISSING_VALID_PLAN
------------------

Argument to <operator> does not contain a valid plan.


MIXED_TYPE_REPLACEMENT
----------------------

Mixed type replacements are not supported.


MULTIPLE_PIPELINE_SPEC_FILES_FOUND
----------------------------------

Multiple pipeline spec files found in the directory `<dir_path>`. Please remove one or choose a particular one with the --spec argument.


NEGATIVE_VALUE
--------------

Value for `<arg_name>` must be greater than or equal to 0, got '<arg_value>'.


NOT_BOOL
--------

Argument `<arg_name>` should be a bool, got <arg_type>.


NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE
--------------------------------------------------------

Argument `<arg_name>` should be a bool, dict, float, int, str or tuple, got <arg_type>.


NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR
---------------------------------------

Argument `<arg_name>` should be a bool, dict, float, int or str, got <arg_type>.


NOT_BOOL_OR_FLOAT_OR_INT
------------------------

Argument `<arg_name>` should be a bool, float or int, got <arg_type>.


NOT_BOOL_OR_FLOAT_OR_INT_OR_LIST_OR_NONE_OR_STR_OR_TUPLE
--------------------------------------------------------

Argument `<arg_name>` should be a bool, float, int, list, None, str or tuple, got <arg_type>.


NOT_BOOL_OR_FLOAT_OR_INT_OR_STR
-------------------------------

Argument `<arg_name>` should be a bool, float, int or str, got <arg_type>.


NOT_BOOL_OR_LIST
----------------

Argument `<arg_name>` should be a bool or list, got <arg_type>.


NOT_BOOL_OR_STR
---------------

Argument `<arg_name>` should be a bool or str, got <arg_type>.


NOT_CALLABLE
------------

Argument `<arg_name>` should be a callable, got <arg_type>.


NOT_COLUMN
----------

Argument `<arg_name>` should be a Column, got <arg_type>.


NOT_COLUMN_OR_DATATYPE_OR_STR
-----------------------------

Argument `<arg_name>` should be a Column, str or DataType, but got <arg_type>.


NOT_COLUMN_OR_FLOAT_OR_INT_OR_LIST_OR_STR
-----------------------------------------

Argument `<arg_name>` should be a Column, float, integer, list or string, got <arg_type>.


NOT_COLUMN_OR_INT
-----------------

Argument `<arg_name>` should be a Column or int, got <arg_type>.


NOT_COLUMN_OR_INT_OR_LIST_OR_STR_OR_TUPLE
-----------------------------------------

Argument `<arg_name>` should be a Column, int, list, str or tuple, got <arg_type>.


NOT_COLUMN_OR_INT_OR_STR
------------------------

Argument `<arg_name>` should be a Column, int or str, got <arg_type>.


NOT_COLUMN_OR_LIST_OR_STR
-------------------------

Argument `<arg_name>` should be a Column, list or str, got <arg_type>.


NOT_COLUMN_OR_STR
-----------------

Argument `<arg_name>` should be a Column or str, got <arg_type>.


NOT_COLUMN_OR_STR_OR_STRUCT
---------------------------

Argument `<arg_name>` should be a StructType, Column or str, got <arg_type>.


NOT_DATAFRAME
-------------

Argument `<arg_name>` should be a DataFrame, got <arg_type>.


NOT_DATATYPE_OR_STR
-------------------

Argument `<arg_name>` should be a DataType or str, got <arg_type>.


NOT_DICT
--------

Argument `<arg_name>` should be a dict, got <arg_type>.


NOT_EXPRESSION
--------------

Argument `<arg_name>` should be an Expression, got <arg_type>.


NOT_FLOAT_OR_INT
----------------

Argument `<arg_name>` should be a float or int, got <arg_type>.


NOT_FLOAT_OR_INT_OR_LIST_OR_STR
-------------------------------

Argument `<arg_name>` should be a float, int, list or str, got <arg_type>.


NOT_IMPLEMENTED
---------------

<feature> is not implemented.


NOT_INT
-------

Argument `<arg_name>` should be an int, got <arg_type>.


NOT_INT_OR_SLICE_OR_STR
-----------------------

Argument `<arg_name>` should be an int, slice or str, got <arg_type>.


NOT_IN_BARRIER_STAGE
--------------------

It is not in a barrier stage.


NOT_ITERABLE
------------

<objectName> is not iterable.


NOT_LIST
--------

Argument `<arg_name>` should be a list, got <arg_type>.


NOT_LIST_OF_COLUMN
------------------

Argument `<arg_name>` should be a list[Column].


NOT_LIST_OF_COLUMN_OR_STR
-------------------------

Argument `<arg_name>` should be a list[Column].


NOT_LIST_OF_FLOAT_OR_INT
------------------------

Argument `<arg_name>` should be a list[float, int], got <arg_type>.


NOT_LIST_OF_STR
---------------

Argument `<arg_name>` should be a list[str], got <arg_type>.


NOT_LIST_OR_NONE_OR_STRUCT
--------------------------

Argument `<arg_name>` should be a list, None or StructType, got <arg_type>.


NOT_LIST_OR_STR_OR_TUPLE
------------------------

Argument `<arg_name>` should be a list, str or tuple, got <arg_type>.


NOT_LIST_OR_TUPLE
-----------------

Argument `<arg_name>` should be a list or tuple, got <arg_type>.


NOT_NUMERIC_COLUMNS
-------------------

Numeric aggregation function can only be applied on numeric columns, got <invalid_columns>.


NOT_OBSERVATION_OR_STR
----------------------

Argument `<arg_name>` should be an Observation or str, got <arg_type>.


NOT_SAME_TYPE
-------------

Argument `<arg_name1>` and `<arg_name2>` should be the same type, got <arg_type1> and <arg_type2>.


NOT_STR
-------

Argument `<arg_name>` should be a str, got <arg_type>.


NOT_STRUCT
----------

Argument `<arg_name>` should be a struct type, got <arg_type>.


NOT_STR_OR_LIST_OF_RDD
----------------------

Argument `<arg_name>` should be a str or list[RDD], got <arg_type>.


NOT_STR_OR_STRUCT
-----------------

Argument `<arg_name>` should be a str or struct type, got <arg_type>.


NOT_WINDOWSPEC
--------------

Argument `<arg_name>` should be a WindowSpec, got <arg_type>.


NO_ACTIVE_EXCEPTION
-------------------

No active exception.


NO_ACTIVE_OR_DEFAULT_SESSION
----------------------------

No active or default Spark session found. Please create a new Spark session before running the code.


NO_ACTIVE_SESSION
-----------------

No active Spark session found. Please create a new Spark session before running the code.


NO_OBSERVE_BEFORE_GET
---------------------

Should observe by calling `DataFrame.observe` before `get`.


NO_SCHEMA_AND_DRIVER_DEFAULT_SCHEME
-----------------------------------

Only allows <arg_name> to be a path without scheme, and Spark Driver should use the default scheme to determine the destination file system.


ONLY_ALLOWED_FOR_SINGLE_COLUMN
------------------------------

Argument `<arg_name>` can only be provided for a single column.


ONLY_ALLOW_SINGLE_TRIGGER
-------------------------

Only a single trigger is allowed.


ONLY_SUPPORTED_WITH_SPARK_CONNECT
---------------------------------

<feature> is only supported with Spark Connect; however, the current Spark session does not use Spark Connect.


PACKAGE_NOT_INSTALLED
---------------------

<package_name> >= <minimum_version> must be installed; however, it was not found.


PANDAS_API_ON_SPARK_FAIL_ON_ANSI_MODE
-------------------------------------

Pandas API on Spark does not properly work on ANSI mode.
Please set a Spark config 'spark.sql.ansi.enabled' to `false`.
Alternatively set a pandas-on-spark option 'compute.fail_on_ansi_mode' to `False` to force it to work, although it can cause unexpected behavior.


PANDAS_UDF_OUTPUT_EXCEEDS_INPUT_ROWS
------------------------------------

The Pandas SCALAR_ITER UDF outputs more rows than input rows.


PIPELINE_SPEC_DICT_KEY_NOT_STRING
---------------------------------

For pipeline spec field `<field_name>`, key should be a string, got <key_type>.


PIPELINE_SPEC_DICT_VALUE_NOT_STRING
-----------------------------------

For pipeline spec field `<field_name>`, value for key `<key_name>` should be a string, got <value_type>.


PIPELINE_SPEC_FIELD_NOT_DICT
----------------------------

Pipeline spec field `<field_name>` should be a dict, got <field_type>.


PIPELINE_SPEC_FILE_DOES_NOT_EXIST
---------------------------------

The pipeline spec file `<spec_path>` does not exist.


PIPELINE_SPEC_FILE_NOT_FOUND
----------------------------

No pipeline.yaml or pipeline.yml file provided in arguments or found in directory `<dir_path>` or readable ancestor directories.


PIPELINE_SPEC_MISSING_REQUIRED_FIELD
------------------------------------

Pipeline spec missing required field `<field_name>`.


PIPELINE_SPEC_UNEXPECTED_FIELD
------------------------------

Pipeline spec field `<field_name>` is unexpected.


PIPELINE_UNSUPPORTED_DEFINITIONS_FILE_EXTENSION
-----------------------------------------------

Pipeline definitions file `<file_path>` has an unsupported extension. Supported extensions are `.py` and `.sql`.


PIPE_FUNCTION_EXITED
--------------------

Pipe function `<func_name>` exited with error code <error_code>.


PLOT_INVALID_TYPE_COLUMN
------------------------

Column <col_name> must be one of <valid_types> for plotting, got <col_type>.


PLOT_NOT_NUMERIC_COLUMN_ARGUMENT
--------------------------------

Argument <arg_name> must be a numerical column for plotting, got <arg_type>.


PYTHON_HASH_SEED_NOT_SET
------------------------

Randomness of hash of string should be disabled via PYTHONHASHSEED.


PYTHON_STREAMING_DATA_SOURCE_RUNTIME_ERROR
------------------------------------------

Failed when running Python streaming data source: <msg>


PYTHON_VERSION_MISMATCH
-----------------------

Python in worker has different version: <worker_version> than that in driver: <driver_version>, PySpark cannot run with different minor versions.
Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.


RDD_TRANSFORM_ONLY_VALID_ON_DRIVER
----------------------------------

It appears that you are attempting to broadcast an RDD or reference an RDD from an 
action or transformation. RDD transformations and actions can only be invoked by the 
driver, not inside of other transformations; for example, 
rdd1.map(lambda x: rdd2.values.count() * x) is invalid because the values 
transformation and count action cannot be performed inside of the rdd1.map 
transformation. For more information, see SPARK-5063.


READ_ONLY
---------

<object> is read-only.


RESPONSE_ALREADY_RECEIVED
-------------------------

<error_type> on the server but responses were already received from it.


RESULT_COLUMNS_MISMATCH_FOR_ARROW_UDF
-------------------------------------

Column names of the returned pyarrow.Table do not match specified schema.<missing><extra>


RESULT_COLUMNS_MISMATCH_FOR_PANDAS_UDF
--------------------------------------

Column names of the returned pandas.DataFrame do not match specified schema.<missing><extra>


RESULT_LENGTH_MISMATCH_FOR_PANDAS_UDF
-------------------------------------

Number of columns of the returned pandas.DataFrame doesn't match specified schema. Expected: <expected> Actual: <actual>


RESULT_LENGTH_MISMATCH_FOR_SCALAR_ITER_PANDAS_UDF
-------------------------------------------------

The length of output in Scalar iterator pandas UDF should be the same with the input's; however, the length of output was <output_length> and the length of input was <input_length>.


RESULT_TYPE_MISMATCH_FOR_ARROW_UDF
----------------------------------

Columns do not match in their data type: <mismatch>.


REUSE_OBSERVATION
-----------------

An Observation can be used with a DataFrame only once.


SCHEMA_MISMATCH_FOR_ARROW_PYTHON_UDF
------------------------------------

Result vector from <udf_type> was not the required length: expected <expected>, got <actual>.


SCHEMA_MISMATCH_FOR_PANDAS_UDF
------------------------------

Result vector from <udf_type> was not the required length: expected <expected>, got <actual>.


SESSION_ALREADY_EXIST
---------------------

Cannot start a remote Spark session because there is a regular Spark session already running.


SESSION_MUTATION_IN_DECLARATIVE_PIPELINE
----------------------------------------

Session mutation <method> is not allowed in declarative pipelines.


SESSION_NEED_CONN_STR_OR_BUILDER
--------------------------------

Needs either connection string or channelBuilder (mutually exclusive) to create a new SparkSession.


SESSION_NOT_SAME
----------------

Both Datasets must belong to the same SparkSession.


SESSION_OR_CONTEXT_EXISTS
-------------------------

There should not be an existing Spark Session or Spark Context.


SESSION_OR_CONTEXT_NOT_EXISTS
-----------------------------

SparkContext or SparkSession should be created first.


SLICE_WITH_STEP
---------------

Slice with step is not supported.


STATE_NOT_EXISTS
----------------

State is either not defined or has already been removed.


STOP_ITERATION_OCCURRED
-----------------------

Caught StopIteration thrown from user's code; failing the task: <exc>


STOP_ITERATION_OCCURRED_FROM_SCALAR_ITER_PANDAS_UDF
---------------------------------------------------

pandas iterator UDF should exhaust the input iterator.


STREAMING_CONNECT_SERIALIZATION_ERROR
-------------------------------------

Cannot serialize the function `<name>`. If you accessed the Spark session, or a DataFrame defined outside of the function, or any object that contains a Spark session, please be aware that they are not allowed in Spark Connect. For `foreachBatch`, please access the Spark session using `df.sparkSession`, where `df` is the first parameter in your `foreachBatch` function. For `StreamingQueryListener`, please access the Spark session using `self.spark`. For details please check out the PySpark doc for `foreachBatch` and `StreamingQueryListener`.


TEST_CLASS_NOT_COMPILED
-----------------------

<test_class_path> doesn't exist. Spark sql test classes are not compiled.


TOO_MANY_VALUES
---------------

Expected <expected> values for `<item>`, got <actual>.


TYPE_HINT_SHOULD_BE_SPECIFIED
-----------------------------

Type hints for <target> should be specified; however, got <sig>.


UDF_RETURN_TYPE
---------------

Return type of the user-defined function should be <expected>, but is <actual>.


UDTF_ARROW_TYPE_CONVERSION_ERROR
--------------------------------

PyArrow UDTF must return an iterator of pyarrow.Table or pyarrow.RecordBatch objects.


UDTF_CONSTRUCTOR_INVALID_IMPLEMENTS_ANALYZE_METHOD
--------------------------------------------------

Failed to evaluate the user-defined table function '<name>' because its constructor is invalid: the function implements the 'analyze' method, but its constructor has more than two arguments (including the 'self' reference). Please update the table function so that its constructor accepts exactly one 'self' argument, or one 'self' argument plus another argument for the result of the 'analyze' method, and try the query again.


UDTF_CONSTRUCTOR_INVALID_NO_ANALYZE_METHOD
------------------------------------------

Failed to evaluate the user-defined table function '<name>' because its constructor is invalid: the function does not implement the 'analyze' method, and its constructor has more than one argument (including the 'self' reference). Please update the table function so that its constructor accepts exactly one 'self' argument, and try the query again.


UDTF_EVAL_METHOD_ARGUMENTS_DO_NOT_MATCH_SIGNATURE
-------------------------------------------------

Failed to evaluate the user-defined table function '<name>' because the function arguments did not match the expected signature of the 'eval' method (<reason>). Please update the query so that this table function call provides arguments matching the expected signature, or else update the table function so that its 'eval' method accepts the provided arguments, and then try the query again.


UDTF_EXEC_ERROR
---------------

User defined table function encountered an error in the '<method_name>' method: <error>


UDTF_INVALID_OUTPUT_ROW_TYPE
----------------------------

The type of an individual output row in the '<func>' method of the UDTF is invalid. Each row should be a tuple, list, or dict, but got '<type>'. Please make sure that the output rows are of the correct type.


UDTF_RETURN_NOT_ITERABLE
------------------------

The return value of the '<func>' method of the UDTF is invalid. It should be an iterable (e.g., generator or list), but got '<type>'. Please make sure that the UDTF returns one of these types.


UDTF_RETURN_SCHEMA_MISMATCH
---------------------------

The number of columns in the result does not match the specified schema. Expected column count: <expected>, Actual column count: <actual>. Please make sure the values returned by the '<func>' method have the same number of columns as specified in the output schema.


UDTF_RETURN_TYPE_MISMATCH
-------------------------

Mismatch in return type for the UDTF '<name>'. Expected a 'StructType', but got '<return_type>'. Please ensure the return type is a correctly formatted StructType.


UDTF_SERIALIZATION_ERROR
------------------------

Cannot serialize the UDTF '<name>': <message>


UNEXPECTED_RESPONSE_FROM_SERVER
-------------------------------

Unexpected response from iterator server.


UNEXPECTED_TUPLE_WITH_STRUCT
----------------------------

Unexpected tuple <tuple> with StructType.


UNKNOWN_EXPLAIN_MODE
--------------------

Unknown explain mode: '<explain_mode>'. Accepted explain modes are 'simple', 'extended', 'codegen', 'cost', 'formatted'.


UNKNOWN_INTERRUPT_TYPE
----------------------

Unknown interrupt type: '<interrupt_type>'. Accepted interrupt types are 'all'.


UNKNOWN_RESPONSE
----------------

Unknown response: <response>.


UNKNOWN_VALUE_FOR
-----------------

Unknown value for `<var>`.


UNSUPPORTED_DATA_TYPE
---------------------

Unsupported DataType `<data_type>`.


UNSUPPORTED_DATA_TYPE_FOR_ARROW
-------------------------------

Single data type <data_type> is not supported with Arrow.


UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION
------------------------------------------

<data_type> is not supported in conversion to Arrow.


UNSUPPORTED_DATA_TYPE_FOR_ARROW_VERSION
---------------------------------------

<data_type> is only supported with pyarrow 2.0.0 and above.


UNSUPPORTED_JOIN_TYPE
---------------------

Unsupported join type: '<typ>'. Supported join types include: <supported>.


UNSUPPORTED_LITERAL
-------------------

Unsupported Literal '<literal>'.


UNSUPPORTED_LOCAL_CONNECTION_STRING
-----------------------------------

Creating new SparkSessions with `local` connection string is not supported.


UNSUPPORTED_NUMPY_ARRAY_SCALAR
------------------------------

The type of array scalar '<dtype>' is not supported.


UNSUPPORTED_OPERATION
---------------------

<operation> is not supported.


UNSUPPORTED_PACKAGE_VERSION
---------------------------

<package_name> >= <minimum_version> must be installed; however, your version is <current_version>.


UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION
------------------------------------------------

Function `<func_name>` should use only POSITIONAL or POSITIONAL OR KEYWORD arguments.


UNSUPPORTED_PIE_PLOT_PARAM
--------------------------

Pie plot requires either a `y` column or `subplots=True`.


UNSUPPORTED_PIPELINES_DATASET_TYPE
----------------------------------

Unsupported pipelines dataset type: <dataset_type>.


UNSUPPORTED_PLOT_BACKEND
------------------------

`<backend>` is not supported, it should be one of the values from <supported_backends>


UNSUPPORTED_PLOT_BACKEND_PARAM
------------------------------

`<backend>` does not support `<param>` set to <value>, it should be one of the values from <supported_values>


UNSUPPORTED_PLOT_KIND
---------------------

`<plot_type>` is not supported, it should be one of the values from <supported_plot_types>


UNSUPPORTED_SIGNATURE
---------------------

Unsupported signature: <signature>.


UNSUPPORTED_WITH_ARROW_OPTIMIZATION
-----------------------------------

<feature> is not supported with Arrow optimization enabled in Python UDFs. Disable 'spark.sql.execution.pythonUDF.arrow.enabled' to workaround.


VALUE_ALLOWED
-------------

Value for `<arg_name>` does not allow <disallowed_value>.


VALUE_NOT_ACCESSIBLE
--------------------

Value `<value>` cannot be accessed inside tasks.


VALUE_NOT_ALLOWED
-----------------

Value for `<arg_name>` has to be amongst the following values: <allowed_values>.


VALUE_NOT_ANY_OR_ALL
--------------------

Value for `<arg_name>` must be 'any' or 'all', got '<arg_value>'.


VALUE_NOT_BETWEEN
-----------------

Value for `<arg_name>` must be between <min> and <max>.


VALUE_NOT_NON_EMPTY_STR
-----------------------

Value for `<arg_name>` must be a non-empty string, got '<arg_value>'.


VALUE_NOT_PEARSON
-----------------

Value for `<arg_name>` only supports the 'pearson', got '<arg_value>'.


VALUE_NOT_PLAIN_COLUMN_REFERENCE
--------------------------------

Value `<val>` in `<field_name>` should be a plain column reference such as `df.col` or `col('column')`.


VALUE_NOT_POSITIVE
------------------

Value for `<arg_name>` must be positive, got '<arg_value>'.


VALUE_NOT_TRUE
--------------

Value for `<arg_name>` must be True, got '<arg_value>'.


VALUE_OUT_OF_BOUNDS
-------------------

Value for `<arg_name>` must be between <lower_bound> and <upper_bound> (inclusive), got <actual>


WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION
----------------------------------------

Function `<func_name>` should take between 1 and 3 arguments, but the provided function takes <num_args>.


WRONG_NUM_COLUMNS
-----------------

Function `<func_name>` should take at least <num_cols> columns.


ZERO_INDEX
----------

Index must be non-zero.


