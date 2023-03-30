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
  "ARGUMENT_REQUIRED": {
    "message": [
      "Argument `<arg_name>` is required when <condition>."
    ]
  },
  "COLUMN_IN_LIST": {
    "message": [
      "`<func_name>` does not allow a Column in a list."
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
  "JVM_ATTRIBUTE_NOT_SUPPORTED" : {
    "message" : [
      "Attribute `<attr_name>` is not supported in Spark Connect as it depends on the JVM. If you need to use this attribute, do not use Spark Connect when creating your session."
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
  "NOT_ITERABLE" : {
    "message" : [
      "<objectName> is not iterable."
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
