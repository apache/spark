#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import json


ERROR_CLASSES_JSON = """
{
  "APPLICATION_NAME_NOT_SET" : {
    "message" : [
      "An application name must be set in your configuration."
    ]
  },
  "ARGUMENT_REQUIRED": {
    "message": [
      "Argument `<arg_name>` is required when <condition>."
    ]
  },
  "BARRIER_TASK_CONTEXT_NOT_INITIALIZE": {
    "message": [
      "Not supported to call `<func_name>` before initialize BarrierTaskContext."
    ]
  },
  "BROADCAST_VARIABLE_NOT_LOADED": {
    "message": [
      "Broadcast variable `<variable>` not loaded."
    ]
  },
  "CALL_BEFORE_SESSION_INITIALIZE" : {
    "message" : [
      "`<func_name>` was called before SparkSession was initialized."
    ]
  },
  "CANNOT_ACCESS_TO_DUNDER": {
    "message": [
      "Dunder(double underscore) attribute is for internal use only."
    ]
  },
  "CANNOT_APPLY_IN_FOR_COLUMN": {
    "message": [
      "Cannot apply 'in' operator against a column: please use 'contains' in a string column or 'array_contains' function for an array column."
    ]
  },
  "CANNOT_CONVERT_COLUMN_INTO_BOOL": {
    "message": [
      "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions."
    ]
  },
  "CANNOT_DESTROY_BROADCAST": {
    "message": [
      "Broadcast can only be destroyed in driver."
    ]
  },
  "CANNOT_OPEN_SOCKET": {
    "message": [
      "Can not open socket: <errors>."
    ]
  },
  "CANNOT_REDUCE_BROADCAST": {
    "message": [
      "Broadcast can only be serialized in driver."
    ]
  },
  "CANNOT_UNPERSIST_BROADCAST": {
    "message": [
      "Broadcast can only be unpersisted in driver."
    ]
  },
  "COLUMN_IN_LIST": {
    "message": [
      "`<func_name>` does not allow a Column in a list."
    ]
  },
  "CONTEXT_ONLY_VALID_ON_DRIVER" : {
    "message" : [
      "It appears that you are attempting to reference SparkContext from a broadcast ",
      "variable, action, or transformation. SparkContext can only be used on the driver, ",
      "not in code that it run on workers. For more information, see SPARK-5063."
    ]
  },
  "CONTEXT_UNAVAILABLE_FOR_REMOTE_CLIENT" : {
    "message" : [
      "Remote client cannot create a SparkContext. Create SparkSession instead."
    ]
  },
  "DISALLOWED_TYPE_FOR_CONTAINER" : {
    "message" : [
      "Argument `<arg_name>`(type: <arg_type>) should only contain a type in [<allowed_types>], got <return_type>"
    ]
  },
  "HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN" : {
    "message" : [
      "Function `<func_name>` should return Column, got <return_type>."
    ]
  },
  "INCORRECT_CONF_FOR_PROFILE" : {
    "message" : [
      "`spark.python.profile` or `spark.python.profile.memory` configuration",
      " must be set to `true` to enable Python profile."
    ]
  },
  "INVALID_ITEM_FOR_CONTAINER": {
    "message": [
      "All items in `<arg_name>` should be in <allowed_types>, got <item_type>."
    ]
  },
  "INVALID_VERSION_FORMAT" : {
    "message" : [
      "Spark version should start with 'spark-' prefix; however, got <version>"
    ]
  },
  "INVALID_WINDOW_BOUND_TYPE" : {
    "message" : [
      "Invalid window bound type: <window_bound_type>."
    ]
  },
  "JAVA_GATEWAY_EXITED" : {
    "message" : [
      "Java gateway process exited before sending its port number."
    ]
  },
  "JVM_ATTRIBUTE_NOT_SUPPORTED" : {
    "message" : [
      "Attribute `<attr_name>` is not supported in Spark Connect as it depends on the JVM. If you need to use this attribute, do not use Spark Connect when creating your session."
    ]
  },
  "KEY_VALUE_PAIR_REQUIRED" : {
    "message" : [
      "Key-value pair or a list of pairs is required."
    ]
  },
  "MASTER_URL_NOT_SET" : {
    "message" : [
      "A master URL must be set in your configuration."
    ]
  },
  "MISSING_LIBRARY_FOR_PROFILER" : {
    "message" : [
      "Install the 'memory_profiler' library in the cluster to enable memory profiling."
    ]
  },
  "NOT_BOOL" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, dict, float, int, str or tuple, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, dict, float, int or str, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_FLOAT_OR_INT" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, float or str, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_FLOAT_OR_INT_OR_LIST_OR_NONE_OR_STR_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, float, int, list, None, str or tuple, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_FLOAT_OR_INT_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, float, int or str, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_LIST" : {
    "message" : [
      "Argument `<arg_name>` should be a bool or list, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a bool or str, got <arg_type>."
    ]
  },
  "NOT_COLUMN" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_DATATYPE_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, str or DataType, but got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_FLOAT_OR_INT_OR_LIST_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a column, float, integer, list or string, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_INT" : {
    "message" : [
      "Argument `<arg_name>` should be a Column or int, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_INT_OR_LIST_OR_STR_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, int, list, str or tuple, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_INT_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, int or str, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a Column or str, got <arg_type>."
    ]
  },
  "NOT_DATAFRAME" : {
    "message" : [
      "Argument `<arg_name>` should be a DataFrame, got <arg_type>."
    ]
  },
  "NOT_DATATYPE_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a DataType or str, got <arg_type>."
    ]
  },
  "NOT_DICT" : {
    "message" : [
      "Argument `<arg_name>` should be a dict, got <arg_type>."
    ]
  },
  "NOT_EXPRESSION" : {
    "message" : [
      "Argument `<arg_name>` should be a Expression, got <arg_type>."
    ]
  },
  "NOT_FLOAT_OR_INT" : {
    "message" : [
      "Argument `<arg_name>` should be a float or int, got <arg_type>."
    ]
  },
  "NOT_FLOAT_OR_INT_OR_LIST_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a float, int, list or str, got <arg_type>."
    ]
  },
  "NOT_INT" : {
    "message" : [
      "Argument `<arg_name>` should be an int, got <arg_type>."
    ]
  },
  "NOT_IN_BARRIER_STAGE" : {
    "message" : [
      "It is not in a barrier stage."
    ]
  },
  "NOT_ITERABLE" : {
    "message" : [
      "<objectName> is not iterable."
    ]
  },
  "NOT_LIST_OF_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a list[str], got <arg_type>."
    ]
  },
  "NOT_LIST_OR_STR_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a list, str or tuple, got <arg_type>."
    ]
  },
  "NOT_LIST_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a list or tuple, got <arg_type>."
    ]
  },
  "NOT_SAME_TYPE" : {
    "message" : [
      "Argument `<arg_name1>` and `<arg_name2>` should be the same type, got <arg_type1> and <arg_type2>."
    ]
  },
  "NOT_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a str, got <arg_type>."
    ]
  },
  "NOT_WINDOWSPEC" : {
    "message" : [
      "Argument `<arg_name>` should be a WindowSpec, got <arg_type>."
    ]
  },
  "NO_ACTIVE_SESSION" : {
    "message" : [
      "No active Spark session found. Please create a new Spark session before running the code."
    ]
  },
  "ONLY_ALLOWED_FOR_SINGLE_COLUMN" : {
    "message" : [
      "Argument `<arg_name>` can only be provided for a single column."
    ]
  },
  "PIPE_FUNCTION_EXITED" : {
    "message" : [
      "Pipe function `<func_name>` exited with error code <error_code>."
    ]
  },
  "PYTHON_HASH_SEED_NOT_SET" : {
    "message" : [
      "Randomness of hash of string should be disabled via PYTHONHASHSEED."
    ]
  },
  "PYTHON_VERSION_MISMATCH" : {
    "message" : [
      "Python in worker has different version <worker_version> than that in driver <driver_version>, PySpark cannot run with different minor versions.",
      "Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set."
    ]
  },
  "RDD_TRANSFORM_ONLY_VALID_ON_DRIVER" : {
    "message" : [
      "It appears that you are attempting to broadcast an RDD or reference an RDD from an ",
      "action or transformation. RDD transformations and actions can only be invoked by the ",
      "driver, not inside of other transformations; for example, ",
      "rdd1.map(lambda x: rdd2.values.count() * x) is invalid because the values ",
      "transformation and count action cannot be performed inside of the rdd1.map ",
      "transformation. For more information, see SPARK-5063."
    ]
  },
  "RESULT_COLUMNS_MISMATCH_FOR_PANDAS_UDF" : {
    "message" : [
      "Column names of the returned pandas.DataFrame do not match specified schema.<missing><extra>"
    ]
  },
  "RESULT_LENGTH_MISMATCH_FOR_PANDAS_UDF" : {
    "message" : [
      "Number of columns of the returned pandas.DataFrame doesn't match specified schema. Expected: <expected> Actual: <actual>"
    ]
  },
  "RESULT_LENGTH_MISMATCH_FOR_SCALAR_ITER_PANDAS_UDF" : {
    "message" : [
      "The length of output in Scalar iterator pandas UDF should be ",
      "the same with the input's; however, the length of output was <output_length> and the ",
      "length of input was <input_length>."
    ]
  },
  "SCHEMA_MISMATCH_FOR_PANDAS_UDF" : {
    "message" : [
      "Result vector from pandas_udf was not the required length: expected <expected>, got <actual>."
    ]
  },
  "SLICE_WITH_STEP" : {
    "message" : [
      "Slice with step is not supported."
    ]
  },
  "STOP_ITERATION_OCCURRED" : {
    "message" : [
      "Caught StopIteration thrown from user's code; failing the task: <exc>"
    ]
  },
  "STOP_ITERATION_OCCURRED_FROM_SCALAR_ITER_PANDAS_UDF" : {
    "message" : [
      "pandas iterator UDF should exhaust the input iterator."
    ]
  },
  "UNEXPECTED_RESPONSE_FROM_SERVER" : {
    "message" : [
      "Unexpected response from iterator server."
    ]
  },
  "UNSUPPORTED_NUMPY_ARRAY_SCALAR" : {
    "message" : [
      "The type of array scalar '<dtype>' is not supported."
    ]
  },
  "UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION" : {
    "message" : [
      "Function `<func_name>` should use only POSITIONAL or POSITIONAL OR KEYWORD arguments."
    ]
  },
  "UNSUPPORTED_SPARK_DISTRIBUTION" : {
    "message" : [
      "Spark distribution of <hive_version> is not supported. Hive version should be one of [<supported_hadoop_versions>]"
    ]
  },
  "VALUE_NOT_ACCESSIBLE": {
    "message": [
      "Value `<value>` cannot be accessed inside tasks."
    ]
  },
  "WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION" : {
    "message" : [
      "Function `<func_name>` should take between 1 and 3 arguments, but provided function takes <num_args>."
    ]
  },
  "WRONG_NUM_COLUMNS" : {
    "message" : [
      "Function `<func_name>` should take at least <num_cols> columns."
    ]
  }
}
"""

ERROR_CLASSES_MAP = json.loads(ERROR_CLASSES_JSON)
