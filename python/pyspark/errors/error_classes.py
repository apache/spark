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
  "COLUMN_IN_LIST": {
    "message": [
      "<func_name> does not allow a column in a list."
    ]
  },
  "HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN" : {
    "message" : [
      "Function `<func_name>` should return Column, got <return_type>"
    ]
  },
  "NOT_A_COLUMN" : {
    "message" : [
      "Argument `<arg_name>` should be a column, got <arg_type>."
    ]
  },
  "NOT_A_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a string, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_INTEGER" : {
    "message" : [
      "Argument `<arg_name>` should be a column or integer, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_INTEGER_OR_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a column or integer or string, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_STRING" : {
    "message" : [
      "Argument `<arg_name>` should be a column or string, got <arg_type>."
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
