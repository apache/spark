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

from typing import Union, Any, Type
from pyspark.errors.exceptions import PySparkException
from py4j.java_gateway import JavaClass


def _get_pyspark_errors() -> JavaClass:
    from pyspark.sql import SparkSession

    spark = SparkSession._getActiveSessionOrCreate()
    assert spark._jvm is not None
    return spark._jvm.org.apache.spark.python.errors.PySparkErrors


def column_in_list_error(func_name: str) -> "PySparkException":
    pyspark_errors = _get_pyspark_errors()
    e = pyspark_errors.columnInListError(func_name)
    return PySparkException(origin=e)


def higher_order_function_should_return_column_error(
    func_name: str, return_type: Type[Any]
) -> "PySparkException":
    pyspark_errors = _get_pyspark_errors()
    e = pyspark_errors.higherOrderFunctionShouldReturnColumnError(func_name, return_type.__name__)
    return PySparkException(origin=e)


def not_column_error(arg_name: str, arg_type: Type[Any]) -> "PySparkException":
    pyspark_errors = _get_pyspark_errors()
    e = pyspark_errors.notColumnError(arg_name, arg_type.__name__)
    return PySparkException(origin=e)


def not_string_error(arg_name: str, arg_type: Type[Any]) -> "PySparkException":
    pyspark_errors = _get_pyspark_errors()
    e = pyspark_errors.notStringError(arg_name, arg_type.__name__)
    return PySparkException(origin=e)


def not_column_or_integer_error(arg_name: str, arg_type: Type[Any]) -> "PySparkException":
    pyspark_errors = _get_pyspark_errors()
    e = pyspark_errors.notColumnOrIntegerError(arg_name, arg_type.__name__)
    return PySparkException(origin=e)


def not_column_or_integer_or_string_error(arg_name: str, arg_type: Type[Any]) -> "PySparkException":
    pyspark_errors = _get_pyspark_errors()
    e = pyspark_errors.notColumnOrIntegerOrStringError(arg_name, arg_type.__name__)
    return PySparkException(origin=e)


def not_column_or_string_error(arg_name: str, arg_type: Type[Any]) -> "PySparkException":
    pyspark_errors = _get_pyspark_errors()
    e = pyspark_errors.notColumnOrStringError(arg_name, arg_type.__name__)
    return PySparkException(origin=e)


def unsupported_param_type_for_higher_order_function_error(func_name: str) -> "PySparkException":
    pyspark_errors = _get_pyspark_errors()
    e = pyspark_errors.unsupportedParamTypeForHigherOrderFunctionError(func_name)
    return PySparkException(origin=e)


def invalid_number_of_columns_error(func_name: str) -> "PySparkException":
    pyspark_errors = _get_pyspark_errors()
    e = pyspark_errors.invalidNumberOfColumnsError(func_name)
    return PySparkException(origin=e)


def invalid_higher_order_function_argument_number_error(
    func_name: str, num_args: Union[str, int]
) -> "PySparkException":
    pyspark_errors = _get_pyspark_errors()
    num_args = str(num_args) if isinstance(num_args, int) else num_args
    e = pyspark_errors.invalidHigherOrderFunctionArgumentNumberError(func_name, num_args)
    return PySparkException(origin=e)
