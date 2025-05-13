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
import warnings
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional, cast, List, TYPE_CHECKING

from pyspark.errors.exceptions.base import (
    AnalysisException as BaseAnalysisException,
    IllegalArgumentException as BaseIllegalArgumentException,
    ArithmeticException as BaseArithmeticException,
    UnsupportedOperationException as BaseUnsupportedOperationException,
    ArrayIndexOutOfBoundsException as BaseArrayIndexOutOfBoundsException,
    DateTimeException as BaseDateTimeException,
    NumberFormatException as BaseNumberFormatException,
    ParseException as BaseParseException,
    PySparkException,
    PythonException as BasePythonException,
    QueryExecutionException as BaseQueryExecutionException,
    SparkRuntimeException as BaseSparkRuntimeException,
    SparkUpgradeException as BaseSparkUpgradeException,
    SparkNoSuchElementException as BaseNoSuchElementException,
    StreamingQueryException as BaseStreamingQueryException,
    UnknownException as BaseUnknownException,
    QueryContext as BaseQueryContext,
    QueryContextType,
)

if TYPE_CHECKING:
    from py4j.protocol import Py4JJavaError
    from py4j.java_gateway import JavaObject


class CapturedException(PySparkException):
    def __init__(
        self,
        desc: Optional[str] = None,
        stackTrace: Optional[str] = None,
        cause: Optional["Py4JJavaError"] = None,
        origin: Optional["Py4JJavaError"] = None,
    ):
        from pyspark import SparkContext
        from py4j.protocol import Py4JJavaError

        # desc & stackTrace vs origin are mutually exclusive.
        # cause is optional.
        assert (origin is not None and desc is None and stackTrace is None) or (
            origin is None and desc is not None and stackTrace is not None
        )

        self._desc = desc if desc is not None else cast(Py4JJavaError, origin).getMessage()
        if self._desc is None:
            self._desc = ""
        assert SparkContext._jvm is not None
        self._stackTrace = (
            stackTrace
            if stackTrace is not None
            else (getattr(SparkContext._jvm, "org.apache.spark.util.Utils").exceptionString(origin))
        )
        self._cause = convert_exception(cause) if cause is not None else None
        if self._cause is None and origin is not None and origin.getCause() is not None:
            self._cause = convert_exception(origin.getCause())
        self._origin = origin
        self._log_exception()

    def __str__(self) -> str:
        from pyspark import SparkContext

        assert SparkContext._jvm is not None

        jvm = SparkContext._jvm

        # SPARK-42752: default to True to see issues with initialization
        debug_enabled = True
        try:
            sql_conf = getattr(jvm, "org.apache.spark.sql.internal.SQLConf").get()
            debug_enabled = sql_conf.pysparkJVMStacktraceEnabled()
        except BaseException:
            pass

        desc = self._desc
        if debug_enabled:
            desc = desc + "\n\nJVM stacktrace:\n%s" % self._stackTrace
        return str(desc)

    def getCondition(self) -> Optional[str]:
        from pyspark import SparkContext
        from py4j.java_gateway import is_instance_of

        assert SparkContext._gateway is not None

        gw = SparkContext._gateway
        if self._origin is not None and is_instance_of(
            gw, self._origin, "org.apache.spark.SparkThrowable"
        ):
            return self._origin.getCondition()
        else:
            return None

    def getErrorClass(self) -> Optional[str]:
        warnings.warn("Deprecated in 4.0.0, use getCondition instead.", FutureWarning)
        return self.getCondition()

    def getMessageParameters(self) -> Optional[Dict[str, str]]:
        from pyspark import SparkContext
        from py4j.java_gateway import is_instance_of
        from py4j.protocol import Py4JError

        assert SparkContext._gateway is not None

        gw = SparkContext._gateway
        if self._origin is not None and is_instance_of(
            gw, self._origin, "org.apache.spark.SparkThrowable"
        ):
            try:
                return dict(self._origin.getMessageParameters())
            except Py4JError as e:
                if "py4j.Py4JException" in str(e) and "Method getMessageParameters" in str(e):
                    return None
                raise e
        else:
            return None

    def getSqlState(self) -> Optional[str]:
        from pyspark import SparkContext
        from py4j.java_gateway import is_instance_of
        from py4j.protocol import Py4JError

        assert SparkContext._gateway is not None
        gw = SparkContext._gateway
        if self._origin is not None and is_instance_of(
            gw, self._origin, "org.apache.spark.SparkThrowable"
        ):
            try:
                return self._origin.getSqlState()
            except Py4JError as e:
                if "py4j.Py4JException" in str(e) and "Method getSqlState" in str(e):
                    return None
                raise e
        else:
            return None

    def getMessage(self) -> str:
        from pyspark import SparkContext
        from py4j.java_gateway import is_instance_of
        from py4j.protocol import Py4JError

        assert SparkContext._gateway is not None
        gw = SparkContext._gateway

        if self._origin is not None and is_instance_of(
            gw, self._origin, "org.apache.spark.SparkThrowable"
        ):
            try:
                error_class = self._origin.getCondition()
            except Py4JError as e:
                if "py4j.Py4JException" in str(e) and "Method getCondition" in str(e):
                    return ""
                raise e
            try:
                message_parameters = self._origin.getMessageParameters()
            except Py4JError as e:
                if "py4j.Py4JException" in str(e) and "Method getMessageParameters" in str(e):
                    return ""
                raise e

            error_message = getattr(gw.jvm, "org.apache.spark.SparkThrowableHelper").getMessage(
                error_class, message_parameters
            )

            return error_message
        else:
            return ""

    def getQueryContext(self) -> List[BaseQueryContext]:
        from pyspark import SparkContext
        from py4j.java_gateway import is_instance_of
        from py4j.protocol import Py4JError

        assert SparkContext._gateway is not None

        gw = SparkContext._gateway
        if self._origin is not None and is_instance_of(
            gw, self._origin, "org.apache.spark.SparkThrowable"
        ):
            contexts: List[BaseQueryContext] = []
            try:
                context = self._origin.getQueryContext()
            except Py4JError as e:
                if "py4j.Py4JException" in str(e) and "Method getQueryContext" in str(e):
                    return []
                raise e
            for q in context:
                if q.contextType().toString() == "SQL":
                    contexts.append(SQLQueryContext(q))
                else:
                    contexts.append(DataFrameQueryContext(q))

            return contexts
        else:
            return []


def convert_exception(e: "Py4JJavaError") -> CapturedException:
    from pyspark import SparkContext
    from py4j.java_gateway import is_instance_of

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
    # Order matters. NumberFormatException inherits IllegalArgumentException.
    elif is_instance_of(gw, e, "java.lang.NumberFormatException"):
        return NumberFormatException(origin=e)
    elif is_instance_of(gw, e, "java.lang.IllegalArgumentException"):
        return IllegalArgumentException(origin=e)
    elif is_instance_of(gw, e, "java.lang.ArithmeticException"):
        return ArithmeticException(origin=e)
    elif is_instance_of(gw, e, "java.lang.UnsupportedOperationException"):
        return UnsupportedOperationException(origin=e)
    elif is_instance_of(gw, e, "java.lang.ArrayIndexOutOfBoundsException"):
        return ArrayIndexOutOfBoundsException(origin=e)
    elif is_instance_of(gw, e, "java.time.DateTimeException"):
        return DateTimeException(origin=e)
    elif is_instance_of(gw, e, "org.apache.spark.SparkRuntimeException"):
        return SparkRuntimeException(origin=e)
    elif is_instance_of(gw, e, "org.apache.spark.SparkUpgradeException"):
        return SparkUpgradeException(origin=e)
    elif is_instance_of(gw, e, "org.apache.spark.SparkNoSuchElementException"):
        return SparkNoSuchElementException(origin=e)

    c: "Py4JJavaError" = e.getCause()
    stacktrace: str = getattr(jvm, "org.apache.spark.util.Utils").exceptionString(e)
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
        from py4j.protocol import Py4JJavaError

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


@contextmanager
def unwrap_spark_exception() -> Iterator[Any]:
    from pyspark import SparkContext
    from py4j.protocol import Py4JJavaError
    from py4j.java_gateway import is_instance_of

    assert SparkContext._gateway is not None

    gw = SparkContext._gateway
    try:
        yield
    except Py4JJavaError as e:
        je: "Py4JJavaError" = e.java_exception
        if je is not None and is_instance_of(gw, je, "org.apache.spark.SparkException"):
            converted = convert_exception(je.getCause())
            if not isinstance(converted, UnknownException):
                raise converted from None
        raise


def install_exception_handler() -> None:
    """
    Hook an exception handler into Py4j, which could capture some SQL exceptions in Java.

    When calling Java API, it will call `get_return_value` to parse the returned object.
    If any exception happened in JVM, the result will be Java exception object, it raise
    py4j.protocol.Py4JJavaError. We replace the original `get_return_value` with one that
    could capture the Java exception and throw a Python one (with the same error message).

    It's idempotent, could be called multiple times.
    """
    import py4j

    original = py4j.protocol.get_return_value
    # The original `get_return_value` is not patched, it's idempotent.
    patched = capture_sql_exception(original)
    # only patch the one used in py4j.java_gateway (call Java API)
    py4j.java_gateway.get_return_value = patched


class AnalysisException(CapturedException, BaseAnalysisException):
    """
    Failed to analyze a SQL query plan.
    """


class ParseException(AnalysisException, BaseParseException):
    """
    Failed to parse a SQL command.
    """


class IllegalArgumentException(CapturedException, BaseIllegalArgumentException):
    """
    Passed an illegal or inappropriate argument.
    """


class StreamingQueryException(CapturedException, BaseStreamingQueryException):
    """
    Exception that stopped a :class:`StreamingQuery`.
    """


class QueryExecutionException(CapturedException, BaseQueryExecutionException):
    """
    Failed to execute a query.
    """


class PythonException(CapturedException, BasePythonException):
    """
    Exceptions thrown from Python workers.
    """


class ArithmeticException(CapturedException, BaseArithmeticException):
    """
    Arithmetic exception.
    """


class UnsupportedOperationException(CapturedException, BaseUnsupportedOperationException):
    """
    Unsupported operation exception.
    """


class ArrayIndexOutOfBoundsException(CapturedException, BaseArrayIndexOutOfBoundsException):
    """
    Array index out of bounds exception.
    """


class DateTimeException(CapturedException, BaseDateTimeException):
    """
    Datetime exception.
    """


class NumberFormatException(IllegalArgumentException, BaseNumberFormatException):
    """
    Number format exception.
    """


class SparkRuntimeException(CapturedException, BaseSparkRuntimeException):
    """
    Runtime exception.
    """


class SparkUpgradeException(CapturedException, BaseSparkUpgradeException):
    """
    Exception thrown because of Spark upgrade.
    """


class SparkNoSuchElementException(CapturedException, BaseNoSuchElementException):
    """
    No such element exception.
    """


class UnknownException(CapturedException, BaseUnknownException):
    """
    None of the other exceptions.
    """


class SQLQueryContext(BaseQueryContext):
    def __init__(self, q: "JavaObject"):
        self._q = q

    def contextType(self) -> QueryContextType:
        return QueryContextType.SQL

    def objectType(self) -> str:
        return str(self._q.objectType())

    def objectName(self) -> str:
        return str(self._q.objectName())

    def startIndex(self) -> int:
        return int(self._q.startIndex())

    def stopIndex(self) -> int:
        return int(self._q.stopIndex())

    def fragment(self) -> str:
        return str(self._q.fragment())

    def callSite(self) -> str:
        return str(self._q.callSite())

    def summary(self) -> str:
        return str(self._q.summary())


class DataFrameQueryContext(BaseQueryContext):
    def __init__(self, q: "JavaObject"):
        self._q = q

    def contextType(self) -> QueryContextType:
        return QueryContextType.DataFrame

    def objectType(self) -> str:
        return str(self._q.objectType())

    def objectName(self) -> str:
        return str(self._q.objectName())

    def startIndex(self) -> int:
        return int(self._q.startIndex())

    def stopIndex(self) -> int:
        return int(self._q.stopIndex())

    def fragment(self) -> str:
        return str(self._q.fragment())

    def callSite(self) -> str:
        return str(self._q.callSite())

    def summary(self) -> str:
        return str(self._q.summary())
