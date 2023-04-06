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

This is a list of common, named error classes returned by PySpark which are defined at `error_classes.py <https://github.com/apache/spark/blob/master/python/pyspark/errors/error_classes.py>`_.

When writing PySpark errors, developers must use an error class from the list. If an appropriate error class is not available, add a new one into the list. For more information, please refer to `Contributing Error and Exception <https://spark.apache.org/docs/latest/api/python/development/contributing.html#contributing-error-and-exception>`_.

+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| Error class                                                | Error message                                                                                                |
+============================================================+==============================================================================================================+
| ARGUMENT_REQUIRED                                          | Argument `<arg_name>` is required when <condition>.                                                          |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| COLUMN_IN_LIST                                             | `<func_name>` does not allow a Column in a list.                                                             |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| DISALLOWED_TYPE_FOR_CONTAINER                              | Argument `<arg_name>`(`type`: <arg_type>) should only contain a type in [<allowed_types>], got <return_type> |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN                 | Function `<func_name>` should return Column, got <return_type>.                                              |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_BOOL                                                   | Argument `<arg_name>` should be a bool, got <arg_type>.                                                      |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE   | Argument `<arg_name>` should be a bool, dict, float, int, str or tuple, got <arg_type>.                      |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR                    | Argument `<arg_name>` should be a bool, dict, float, int or str, got <arg_type>.                             |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_BOOL_OR_LIST                                           | Argument `<arg_name>` should be a bool or list, got <arg_type>.                                              |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_BOOL_OR_STR                                            | Argument `<arg_name>` should be a bool or str, got <arg_type>.                                               |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_COLUMN                                                 | Argument `<arg_name>` should be a Column, got <arg_type>.                                                    |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_COLUMN_OR_DATATYPE_OR_STR                              | Argument `<arg_name>` should be a Column, str or DataType, but got <arg_type>.                               |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_COLUMN_OR_FLOAT_OR_INT_OR_LIST_OR_STR                  | Argument `<arg_name>` should be a column, float, integer, list or string, got <arg_type>.                    |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_COLUMN_OR_INT                                          | Argument `<arg_name>` should be a Column or int, got <arg_type>.                                             |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_COLUMN_OR_INT_OR_STR                                   | Argument `<arg_name>` should be a Column, int or str, got <arg_type>.                                        |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_COLUMN_OR_STR                                          | Argument `<arg_name>` should be a Column or str, got <arg_type>.                                             |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_DATAFRAME                                              | Argument `<arg_name>` should be a DataFrame, got <arg_type>.                                                 |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_DATATYPE_OR_STR                                        | Argument `<arg_name>` should be a DataType or str, got <arg_type>.                                           |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_DICT                                                   | Argument `<arg_name>` should be a dict, got <arg_type>.                                                      |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_EXPRESSION                                             | Argument <arg_name> should be a Expression, got <arg_type>.                                                  |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_FLOAT_OR_INT                                           | Argument <arg_name> should be a float or int, got <arg_type>.                                                |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_FLOAT_OR_INT_OR_LIST_OR_STR                            | Argument <arg_name> should be a float, int, list or str, got <arg_type>.                                     |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_INT                                                    | Argument <arg_name> should be an int, got <arg_type>.                                                        |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_ITERABLE                                               | <objectName> is not iterable.                                                                                |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_LIST_OR_STR_OR_TUPLE                                   | Argument <arg_name> should be a list, str or tuple, got <arg_type>.                                          |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_LIST_OR_TUPLE                                          | Argument <arg_name> should be a list or tuple, got <arg_type>.                                               |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_SAME_TYPE                                              | Argument <arg_name1> and <arg_name2> should be the same type, got <arg_type1> and <arg_type2>.               |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_STR                                                    | Argument <arg_name> should be a str, got <arg_type>.                                                         |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| NOT_WINDOWSPEC                                             | Argument <arg_name> should be a WindowSpec, got <arg_type>.                                                  |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| UNSUPPORTED_NUMPY_ARRAY_SCALAR                             | The type of array scalar '<dtype>' is not supported.                                                         |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION           | Function <func_name> should use only POSITIONAL or POSITIONAL OR KEYWORD arguments.                          |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION                   | Function <func_name> should take between 1 and 3 arguments, but provided function takes <num_args>.          |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
| WRONG_NUM_COLUMNS                                          | Function <func_name> should take at least <num_cols> columns.                                                |
+------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
