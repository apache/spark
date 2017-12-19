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
"""
User-defined function related classes and functions
"""
import functools

from pyspark import SparkContext
from pyspark.rdd import _prepare_for_python_RDD, PythonEvalType
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.types import StringType, DataType, StructType, _parse_datatype_string


def _wrap_function(sc, func, returnType):
    command = (func, returnType)
    pickled_command, broadcast_vars, env, includes = _prepare_for_python_RDD(sc, command)
    return sc._jvm.PythonFunction(bytearray(pickled_command), env, includes, sc.pythonExec,
                                  sc.pythonVer, broadcast_vars, sc._javaAccumulator)


def _create_udf(f, returnType, evalType):
    from pyspark.sql.types import _require_minimum_pyarrow_version

    _require_minimum_pyarrow_version()

    if evalType == PythonEvalType.SQL_PANDAS_SCALAR_UDF:
        import inspect
        argspec = inspect.getargspec(f)
        if len(argspec.args) == 0 and argspec.varargs is None:
            raise ValueError(
                "Invalid function: 0-arg pandas_udfs are not supported. "
                "Instead, create a 1-arg pandas_udf and ignore the arg in your function."
            )

    elif evalType == PythonEvalType.SQL_PANDAS_GROUP_MAP_UDF:
        import inspect
        argspec = inspect.getargspec(f)
        if len(argspec.args) != 1:
            raise ValueError(
                "Invalid function: pandas_udfs with function type GROUP_MAP "
                "must take a single arg that is a pandas DataFrame."
            )

    # Set the name of the UserDefinedFunction object to be the name of function f
    udf_obj = UserDefinedFunction(f, returnType=returnType, name=None, evalType=evalType)
    return udf_obj._wrapped()


class UserDefinedFunction(object):
    """
    User defined function in Python

    .. versionadded:: 1.3
    """
    def __init__(self, func,
                 returnType=StringType(), name=None,
                 evalType=PythonEvalType.SQL_BATCHED_UDF):
        if not callable(func):
            raise TypeError(
                "Invalid function: not a function or callable (__call__ is not defined): "
                "{0}".format(type(func)))

        if not isinstance(returnType, (DataType, str)):
            raise TypeError(
                "Invalid returnType: returnType should be DataType or str "
                "but is {}".format(returnType))

        if not isinstance(evalType, int):
            raise TypeError(
                "Invalid evalType: evalType should be an int but is {}".format(evalType))

        self.func = func
        self._returnType = returnType
        # Stores UserDefinedPythonFunctions jobj, once initialized
        self._returnType_placeholder = None
        self._judf_placeholder = None
        self._name = name or (
            func.__name__ if hasattr(func, '__name__')
            else func.__class__.__name__)
        self.evalType = evalType

    @property
    def returnType(self):
        # This makes sure this is called after SparkContext is initialized.
        # ``_parse_datatype_string`` accesses to JVM for parsing a DDL formatted string.
        if self._returnType_placeholder is None:
            if isinstance(self._returnType, DataType):
                self._returnType_placeholder = self._returnType
            else:
                self._returnType_placeholder = _parse_datatype_string(self._returnType)

        if self.evalType == PythonEvalType.SQL_PANDAS_GROUP_MAP_UDF \
                and not isinstance(self._returnType_placeholder, StructType):
            raise ValueError("Invalid returnType: returnType must be a StructType for "
                             "pandas_udf with function type GROUP_MAP")

        return self._returnType_placeholder

    @property
    def _judf(self):
        # It is possible that concurrent access, to newly created UDF,
        # will initialize multiple UserDefinedPythonFunctions.
        # This is unlikely, doesn't affect correctness,
        # and should have a minimal performance impact.
        if self._judf_placeholder is None:
            self._judf_placeholder = self._create_judf()
        return self._judf_placeholder

    def _create_judf(self):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext

        wrapped_func = _wrap_function(sc, self.func, self.returnType)
        jdt = spark._jsparkSession.parseDataType(self.returnType.json())
        judf = sc._jvm.org.apache.spark.sql.execution.python.UserDefinedPythonFunction(
            self._name, wrapped_func, jdt, self.evalType)
        return judf

    def __call__(self, *cols):
        judf = self._judf
        sc = SparkContext._active_spark_context
        return Column(judf.apply(_to_seq(sc, cols, _to_java_column)))

    def _wrapped(self):
        """
        Wrap this udf with a function and attach docstring from func
        """

        # It is possible for a callable instance without __name__ attribute or/and
        # __module__ attribute to be wrapped here. For example, functools.partial. In this case,
        # we should avoid wrapping the attributes from the wrapped function to the wrapper
        # function. So, we take out these attribute names from the default names to set and
        # then manually assign it after being wrapped.
        assignments = tuple(
            a for a in functools.WRAPPER_ASSIGNMENTS if a != '__name__' and a != '__module__')

        @functools.wraps(self.func, assigned=assignments)
        def wrapper(*args):
            return self(*args)

        wrapper.__name__ = self._name
        wrapper.__module__ = (self.func.__module__ if hasattr(self.func, '__module__')
                              else self.func.__class__.__module__)

        wrapper.func = self.func
        wrapper.returnType = self.returnType
        wrapper.evalType = self.evalType

        return wrapper
