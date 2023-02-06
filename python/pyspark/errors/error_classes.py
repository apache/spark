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
      "<func_name> does not allow a Column in a list."
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
  "NOT_AN_INTEGER" : {
    "message" : [
      "Argument `<arg_name>` should be a integer, got <arg_type>."
    ]
  },
  "NOT_A_BOOLEAN" : {
    "message" : [
      "Argument `<arg_name>` should be a boolean, got <arg_type>."
    ]
  },
  "NOT_A_COLUMN" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, got <arg_type>."
    ]
  },
  "NOT_A_DATAFRAME" : {
    "message" : [
      "Argument `<arg_name>` must be a DataFrame, got <arg_type>."
    ]
  },
  "NOT_A_DICT" : {
    "message" : [
      "Argument `<arg_name>` should be a dict, got <arg_type>."
    ]
  },
  "NOT_A_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a str, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_DICT_OR_FLOAT_OR_INTEGER_OR_LIST_OR_STRING_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a boolean, dict, float, integer, string or tuple, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_DICT_OR_FLOAT_OR_INTEGER_OR_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a boolean, dict, float, integer or string, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_LIST" : {
    "message" : [
      "Argument `<arg_name>` should be a boolean or list, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a boolean or string, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_DATATYPE_OR_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a Column or str or DataType, but got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_FLOAT_OR_INTEGER_OR_LIST_OR_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a column, float, integer, list or string, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_INTEGER" : {
    "message" : [
      "Argument `<arg_name>` should be a Column or int, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_INTEGER_OR_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, int or str, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a Column or str, got <arg_type>."
    ]
  },
  "NOT_FLOAT_OR_INTEGER" : {
    "message" : [
      "Argument `<arg_name>` should be a float or integer, got <arg_type>."
    ]
  },
  "NOT_FLOAT_OR_INTEGER_OR_LIST_OR_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a float, integer, list or string, got <arg_type>."
    ]
  },
  "NOT_LIST_OR_STRING_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a list, string or tuple, got <arg_type>."
    ]
  },
  "NOT_LIST_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a list or tuple, got <arg_type>."
    ]
  },
  "UNSUPPORTED_NUMPY_ARRAY_SCALAR" : {
    "message" : [
      "The type of array scalar '<dtype>' is not supported."
    ]
  },
  "UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION" : {
    "message" : [
      "Function `<func_name>` should use only POSITIONAL or POSITIONAL OR KEYWORD arguments"
    ]
  },
  "WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION" : {
    "message" : [
      "Function `<func_name>` should take between 1 and 3 arguments, but provided function takes <num_args>"
    ]
  },
  "WRONG_NUM_COLUMNS" : {
    "message" : [
      "Function `<func_name>` should take at least <num_cols> columns"
    ]
  }
}
"""

ERROR_CLASSES_MAP = json.loads(ERROR_CLASSES_JSON)
