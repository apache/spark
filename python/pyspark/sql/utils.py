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
import functools
import os
from typing import Any, Callable, Optional, Sequence, TYPE_CHECKING, cast, TypeVar

import py4j
from py4j.java_collections import JavaArray
from py4j.java_gateway import (
    JavaClass,
    JavaGateway,
    JavaObject,
    is_instance_of,
)
from py4j.protocol import Py4JJavaError

from pyspark import SparkContext
from pyspark.find_spark_home import _find_spark_home

if TYPE_CHECKING:
    from pyspark.sql.session import SparkSession
    from pyspark.sql.dataframe import DataFrame

has_numpy = False
try:
    import numpy as np  # noqa: F401

    has_numpy = True
except ImportError:
    pass


FuncT = TypeVar("FuncT", bound=Callable[..., Any])


class CapturedException(Exception):
    def __init__(
        self,
        desc: Optional[str] = None,
        stackTrace: Optional[str] = None,
        cause: Optional[Py4JJavaError] = None,
        origin: Optional[Py4JJavaError] = None,
    ):
        # desc & stackTrace vs origin are mutually exclusive.
        # cause is optional.
        assert (origin is not None and desc is None and stackTrace is None) or (
            origin is None and desc is not None and stackTrace is not None
        )

        self.desc = desc if desc is not None else cast(Py4JJavaError, origin).getMessage()
        assert SparkContext._jvm is not None
        self.stackTrace = (
            stackTrace
            if stackTrace is not None
            else (SparkContext._jvm.org.apache.spark.util.Utils.exceptionString(origin))
        )
        self.cause = convert_exception(cause) if cause is not None else None
        if self.cause is None and origin is not None and origin.getCause() is not None:
            self.cause = convert_exception(origin.getCause())
        self._origin = origin

    def __str__(self) -> str:
        assert SparkContext._jvm is not None

        jvm = SparkContext._jvm
        sql_conf = jvm.org.apache.spark.sql.internal.SQLConf.get()
        debug_enabled = sql_conf.pysparkJVMStacktraceEnabled()
        desc = self.desc
        if debug_enabled:
            desc = desc + "\n\nJVM stacktrace:\n%s" % self.stackTrace
        return str(desc)

    def getErrorClass(self) -> Optional[str]:
        assert SparkContext._gateway is not None

        gw = SparkContext._gateway
        if self._origin is not None and is_instance_of(
            gw, self._origin, "org.apache.spark.SparkThrowable"
        ):
            return self._origin.getErrorClass()
        else:
            return None

    def getSqlState(self) -> Optional[str]:
        assert SparkContext._gateway is not None

        gw = SparkContext._gateway
        if self._origin is not None and is_instance_of(
            gw, self._origin, "org.apache.spark.SparkThrowable"
        ):
            return self._origin.getSqlState()
        else:
            return None


class AnalysisException(CapturedException):
    """
    Failed to analyze a SQL query plan.
    """


class ParseException(CapturedException):
    """
    Failed to parse a SQL command.
    """


class IllegalArgumentException(CapturedException):
    """
    Passed an illegal or inappropriate argument.
    """


class StreamingQueryException(CapturedException):
    """
    Exception that stopped a :class:`StreamingQuery`.
    """


class QueryExecutionException(CapturedException):
    """
    Failed to execute a query.
    """


class PythonException(CapturedException):
    """
    Exceptions thrown from Python workers.
    """


class UnknownException(CapturedException):
    """
    None of the above exceptions.
    """


class SparkUpgradeException(CapturedException):
    """
    Exception thrown because of Spark upgrade
    """


def convert_exception(e: Py4JJavaError) -> CapturedException:
    assert e is not None
    assert SparkContext._jvm is not None
    assert SparkContext._gateway is not None

    jvm = SparkContext._jvm
    gw = SparkContext._gateway

    if is_instance_of(gw, e, "org.apache.spark.sql.catalyst.parser.ParseException"):
        return ParseException(origin=e)
    # Order matters. ParseException inherits AnalysisException.
    elif is_instance_of(gw, e, "org.apache.spark.sql.AnalysisException"):
        return AnalysisException(origin=e)
    elif is_instance_of(gw, e, "org.apache.spark.sql.streaming.StreamingQueryException"):
        return StreamingQueryException(origin=e)
    elif is_instance_of(gw, e, "org.apache.spark.sql.execution.QueryExecutionException"):
        return QueryExecutionException(origin=e)
    elif is_instance_of(gw, e, "java.lang.IllegalArgumentException"):
        return IllegalArgumentException(origin=e)
    elif is_instance_of(gw, e, "org.apache.spark.SparkUpgradeException"):
        return SparkUpgradeException(origin=e)

    c: Py4JJavaError = e.getCause()
    stacktrace: str = jvm.org.apache.spark.util.Utils.exceptionString(e)
    if c is not None and (
        is_instance_of(gw, c, "org.apache.spark.api.python.PythonException")
        # To make sure this only catches Python UDFs.
        and any(
            map(
                lambda v: "org.apache.spark.sql.execution.python" in v.toString(), c.getStackTrace()
            )
        )
    ):
        msg = (
            "\n  An exception was thrown from the Python worker. "
            "Please see the stack trace below.\n%s" % c.getMessage()
        )
        return PythonException(msg, stacktrace)

    return UnknownException(desc=e.toString(), stackTrace=stacktrace, cause=c)


def capture_sql_exception(f: Callable[..., Any]) -> Callable[..., Any]:
    def deco(*a: Any, **kw: Any) -> Any:
        try:
            return f(*a, **kw)
        except Py4JJavaError as e:
            converted = convert_exception(e.java_exception)
            if not isinstance(converted, UnknownException):
                # Hide where the exception came from that shows a non-Pythonic
                # JVM exception message.
                raise converted from None
            else:
                raise

    return deco


def install_exception_handler() -> None:
    """
    Hook an exception handler into Py4j, which could capture some SQL exceptions in Java.

    When calling Java API, it will call `get_return_value` to parse the returned object.
    If any exception happened in JVM, the result will be Java exception object, it raise
    py4j.protocol.Py4JJavaError. We replace the original `get_return_value` with one that
    could capture the Java exception and throw a Python one (with the same error message).

    It's idempotent, could be called multiple times.
    """
    original = py4j.protocol.get_return_value
    # The original `get_return_value` is not patched, it's idempotent.
    patched = capture_sql_exception(original)
    # only patch the one used in py4j.java_gateway (call Java API)
    py4j.java_gateway.get_return_value = patched


def toJArray(gateway: JavaGateway, jtype: JavaClass, arr: Sequence[Any]) -> JavaArray:
    """
    Convert python list to java type array

    Parameters
    ----------
    gateway :
        Py4j Gateway
    jtype :
        java type of element in array
    arr :
        python type list
    """
    jarray: JavaArray = gateway.new_array(jtype, len(arr))
    for i in range(0, len(arr)):
        jarray[i] = arr[i]
    return jarray


def require_test_compiled() -> None:
    """Raise Exception if test classes are not compiled"""
    import os
    import glob

    test_class_path = os.path.join(_find_spark_home(), "sql", "core", "target", "*", "test-classes")
    paths = glob.glob(test_class_path)

    if len(paths) == 0:
        raise RuntimeError(
            "%s doesn't exist. Spark sql test classes are not compiled." % test_class_path
        )


class ForeachBatchFunction:
    """
    This is the Python implementation of Java interface 'ForeachBatchFunction'. This wraps
    the user-defined 'foreachBatch' function such that it can be called from the JVM when
    the query is active.
    """

    def __init__(self, session: "SparkSession", func: Callable[["DataFrame", int], None]):
        self.func = func
        self.session = session

    def call(self, jdf: JavaObject, batch_id: int) -> None:
        from pyspark.sql.dataframe import DataFrame
        from pyspark.sql.session import SparkSession

        try:
            session_jdf = jdf.sparkSession()
            # assuming that spark context is still the same between JVM and PySpark
            wrapped_session_jdf = SparkSession(self.session.sparkContext, session_jdf)
            self.func(DataFrame(jdf, wrapped_session_jdf), batch_id)
        except Exception as e:
            self.error = e
            raise e

    class Java:
        implements = ["org.apache.spark.sql.execution.streaming.sources.PythonForeachBatchFunction"]


def to_str(value: Any) -> Optional[str]:
    """
    A wrapper over str(), but converts bool values to lower case strings.
    If None is given, just returns None, instead of converting it to string "None".
    """
    if isinstance(value, bool):
        return str(value).lower()
    elif value is None:
        return value
    else:
        return str(value)


def is_timestamp_ntz_preferred() -> bool:
    """
    Return a bool if TimestampNTZType is preferred according to the SQL configuration set.
    """
    jvm = SparkContext._jvm
    return jvm is not None and jvm.PythonSQLUtils.isTimestampNTZPreferred()


def is_remote() -> bool:
    """
    Returns if the current running environment is for Spark Connect.
    """
    return "SPARK_REMOTE" in os.environ


def try_remote_functions(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:

        if is_remote():
            from pyspark.sql.connect import functions

            return getattr(functions, f.__name__)(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_window(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:

        if is_remote():
            from pyspark.sql.connect.window import Window

            return getattr(Window, f.__name__)(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_windowspec(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:

        if is_remote():
            from pyspark.sql.connect.window import WindowSpec

            return getattr(WindowSpec, f.__name__)(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_observation(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        # TODO(SPARK-41527): Add the support of Observation.
        if is_remote():
            raise NotImplementedError()
        return f(*args, **kwargs)

    return cast(FuncT, wrapped)
