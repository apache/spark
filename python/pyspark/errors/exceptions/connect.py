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
import pyspark.sql.connect.proto as pb2
import json
from typing import Dict, List, Optional, TYPE_CHECKING

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
    StreamingQueryException as BaseStreamingQueryException,
    QueryExecutionException as BaseQueryExecutionException,
    SparkRuntimeException as BaseSparkRuntimeException,
    SparkNoSuchElementException as BaseNoSuchElementException,
    SparkUpgradeException as BaseSparkUpgradeException,
    QueryContext as BaseQueryContext,
    QueryContextType,
)

if TYPE_CHECKING:
    from google.rpc.error_details_pb2 import ErrorInfo


class SparkConnectException(PySparkException):
    """
    Exception thrown from Spark Connect.
    """


def convert_exception(
    info: "ErrorInfo",
    truncated_message: str,
    resp: Optional[pb2.FetchErrorDetailsResponse],
    display_server_stacktrace: bool = False,
) -> SparkConnectException:
    classes = []
    sql_state = None
    error_class = None
    message_parameters = None
    query_contexts: Optional[List[BaseQueryContext]] = None

    if "classes" in info.metadata:
        classes = json.loads(info.metadata["classes"])

    if "sqlState" in info.metadata:
        sql_state = info.metadata["sqlState"]

    if "errorClass" in info.metadata:
        error_class = info.metadata["errorClass"]

    if "messageParameters" in info.metadata:
        message_parameters = json.loads(info.metadata["messageParameters"])

    stacktrace: Optional[str] = None
    if resp is not None and resp.HasField("root_error_idx"):
        message = resp.errors[resp.root_error_idx].message
        stacktrace = _extract_jvm_stacktrace(resp)
    else:
        message = truncated_message
        stacktrace = info.metadata["stackTrace"] if "stackTrace" in info.metadata else None
        display_server_stacktrace = display_server_stacktrace if stacktrace is not None else False

    if (
        resp is not None
        and resp.errors
        and hasattr(resp.errors[resp.root_error_idx], "spark_throwable")
    ):
        message_parameters = dict(
            resp.errors[resp.root_error_idx].spark_throwable.message_parameters
        )
        query_contexts = []
        for query_context in resp.errors[resp.root_error_idx].spark_throwable.query_contexts:
            if query_context.context_type == pb2.FetchErrorDetailsResponse.QueryContext.SQL:
                query_contexts.append(SQLQueryContext(query_context))
            else:
                query_contexts.append(DataFrameQueryContext(query_context))

    if "org.apache.spark.sql.catalyst.parser.ParseException" in classes:
        return ParseException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    # Order matters. ParseException inherits AnalysisException.
    elif "org.apache.spark.sql.AnalysisException" in classes:
        return AnalysisException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    elif "org.apache.spark.sql.streaming.StreamingQueryException" in classes:
        return StreamingQueryException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    elif "org.apache.spark.sql.execution.QueryExecutionException" in classes:
        return QueryExecutionException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    # Order matters. NumberFormatException inherits IllegalArgumentException.
    elif "java.lang.NumberFormatException" in classes:
        return NumberFormatException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    elif "java.lang.IllegalArgumentException" in classes:
        return IllegalArgumentException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    elif "java.lang.ArithmeticException" in classes:
        return ArithmeticException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    elif "java.lang.UnsupportedOperationException" in classes:
        return UnsupportedOperationException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    elif "java.lang.ArrayIndexOutOfBoundsException" in classes:
        return ArrayIndexOutOfBoundsException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    elif "java.time.DateTimeException" in classes:
        return DateTimeException(
            message,
            error_class=error_class,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    elif "org.apache.spark.SparkRuntimeException" in classes:
        return SparkRuntimeException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    elif "org.apache.spark.SparkUpgradeException" in classes:
        return SparkUpgradeException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    elif "org.apache.spark.api.python.PythonException" in classes:
        return PythonException(
            "\n  An exception was thrown from the Python worker. "
            "Please see the stack trace below.\n%s" % message
        )
    elif "org.apache.spark.SparkNoSuchElementException" in classes:
        return SparkNoSuchElementException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    # Make sure that the generic SparkException is handled last.
    elif "org.apache.spark.SparkException" in classes:
        return SparkException(
            message,
            error_class=error_class,
            message_parameters=message_parameters,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )
    else:
        return SparkConnectGrpcException(
            message,
            reason=info.reason,
            message_parameters=message_parameters,
            error_class=error_class,
            sql_state=sql_state,
            server_stacktrace=stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            query_contexts=query_contexts,
        )


def _extract_jvm_stacktrace(resp: pb2.FetchErrorDetailsResponse) -> str:
    if len(resp.errors[resp.root_error_idx].stack_trace) == 0:
        return ""

    lines: List[str] = []

    def format_stacktrace(error: pb2.FetchErrorDetailsResponse.Error) -> None:
        message = f"{error.error_type_hierarchy[0]}: {error.message}"
        if len(lines) == 0:
            lines.append(error.error_type_hierarchy[0])
        else:
            lines.append(f"Caused by: {message}")
        for elem in error.stack_trace:
            lines.append(
                f"\tat {elem.declaring_class}.{elem.method_name}"
                f"({elem.file_name}:{elem.line_number})"
            )

        # If this error has a cause, format that recursively
        if error.HasField("cause_idx"):
            format_stacktrace(resp.errors[error.cause_idx])

    format_stacktrace(resp.errors[resp.root_error_idx])

    return "\n".join(lines)


class SparkConnectGrpcException(SparkConnectException):
    """
    Base class to handle the errors from GRPC.
    """

    def __init__(
        self,
        message: Optional[str] = None,
        error_class: Optional[str] = None,
        message_parameters: Optional[Dict[str, str]] = None,
        reason: Optional[str] = None,
        sql_state: Optional[str] = None,
        server_stacktrace: Optional[str] = None,
        display_server_stacktrace: bool = False,
        query_contexts: Optional[List[BaseQueryContext]] = None,
    ) -> None:
        if query_contexts is None:
            query_contexts = []
        self._message = message  # type: ignore[assignment]
        if reason is not None:
            self._message = f"({reason}) {self._message}"

        # PySparkException has the assumption that error_class and message_parameters are
        # only occurring together. If only one is set, we assume the message to be fully
        # parsed.
        tmp_error_class = error_class
        tmp_message_parameters = message_parameters
        if error_class is not None and message_parameters is None:
            tmp_error_class = None
        elif error_class is None and message_parameters is not None:
            tmp_message_parameters = None

        super().__init__(
            message=self._message,
            error_class=tmp_error_class,
            message_parameters=tmp_message_parameters,
        )
        self._error_class = error_class
        self._sql_state: Optional[str] = sql_state
        self._stacktrace: Optional[str] = server_stacktrace
        self._display_stacktrace: bool = display_server_stacktrace
        self._query_contexts: List[BaseQueryContext] = query_contexts

    def getSqlState(self) -> Optional[str]:
        if self._sql_state is not None:
            return self._sql_state
        else:
            return super().getSqlState()

    def getStackTrace(self) -> Optional[str]:
        return self._stacktrace

    def getMessage(self) -> str:
        desc = self._message
        if self._display_stacktrace:
            desc += "\n\nJVM stacktrace:\n%s" % self._stacktrace
        return desc

    def __str__(self) -> str:
        return self.getMessage()


class AnalysisException(SparkConnectGrpcException, BaseAnalysisException):
    """
    Failed to analyze a SQL query plan, thrown from Spark Connect.
    """


class ParseException(AnalysisException, BaseParseException):
    """
    Failed to parse a SQL command, thrown from Spark Connect.
    """


class IllegalArgumentException(SparkConnectGrpcException, BaseIllegalArgumentException):
    """
    Passed an illegal or inappropriate argument, thrown from Spark Connect.
    """


class StreamingQueryException(SparkConnectGrpcException, BaseStreamingQueryException):
    """
    Exception that stopped a :class:`StreamingQuery` thrown from Spark Connect.
    """


class QueryExecutionException(SparkConnectGrpcException, BaseQueryExecutionException):
    """
    Failed to execute a query, thrown from Spark Connect.
    """


class PythonException(SparkConnectGrpcException, BasePythonException):
    """
    Exceptions thrown from Spark Connect.
    """


class ArithmeticException(SparkConnectGrpcException, BaseArithmeticException):
    """
    Arithmetic exception thrown from Spark Connect.
    """


class UnsupportedOperationException(SparkConnectGrpcException, BaseUnsupportedOperationException):
    """
    Unsupported operation exception thrown from Spark Connect.
    """


class ArrayIndexOutOfBoundsException(SparkConnectGrpcException, BaseArrayIndexOutOfBoundsException):
    """
    Array index out of bounds exception thrown from Spark Connect.
    """


class DateTimeException(SparkConnectGrpcException, BaseDateTimeException):
    """
    Datetime exception thrown from Spark Connect.
    """


class NumberFormatException(IllegalArgumentException, BaseNumberFormatException):
    """
    Number format exception thrown from Spark Connect.
    """


class SparkRuntimeException(SparkConnectGrpcException, BaseSparkRuntimeException):
    """
    Runtime exception thrown from Spark Connect.
    """


class SparkUpgradeException(SparkConnectGrpcException, BaseSparkUpgradeException):
    """
    Exception thrown because of Spark upgrade from Spark Connect.
    """


class SparkException(SparkConnectGrpcException):
    """ """


class SparkNoSuchElementException(SparkConnectGrpcException, BaseNoSuchElementException):
    """
    No such element exception.
    """


class SQLQueryContext(BaseQueryContext):
    def __init__(self, q: pb2.FetchErrorDetailsResponse.QueryContext):
        self._q = q

    def contextType(self) -> QueryContextType:
        return QueryContextType.SQL

    def objectType(self) -> str:
        return str(self._q.object_type)

    def objectName(self) -> str:
        return str(self._q.object_name)

    def startIndex(self) -> int:
        return int(self._q.start_index)

    def stopIndex(self) -> int:
        return int(self._q.stop_index)

    def fragment(self) -> str:
        return str(self._q.fragment)

    def callSite(self) -> str:
        raise UnsupportedOperationException(
            "",
            error_class="UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
            message_parameters={"className": "SQLQueryContext", "methodName": "callSite"},
            sql_state="0A000",
            server_stacktrace=None,
            display_server_stacktrace=False,
            query_contexts=[],
        )

    def summary(self) -> str:
        return str(self._q.summary)


class DataFrameQueryContext(BaseQueryContext):
    def __init__(self, q: pb2.FetchErrorDetailsResponse.QueryContext):
        self._q = q

    def contextType(self) -> QueryContextType:
        return QueryContextType.DataFrame

    def objectType(self) -> str:
        raise UnsupportedOperationException(
            "",
            error_class="UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
            message_parameters={"className": "DataFrameQueryContext", "methodName": "objectType"},
            sql_state="0A000",
            server_stacktrace=None,
            display_server_stacktrace=False,
            query_contexts=[],
        )

    def objectName(self) -> str:
        raise UnsupportedOperationException(
            "",
            error_class="UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
            message_parameters={"className": "DataFrameQueryContext", "methodName": "objectName"},
            sql_state="0A000",
            server_stacktrace=None,
            display_server_stacktrace=False,
            query_contexts=[],
        )

    def startIndex(self) -> int:
        raise UnsupportedOperationException(
            "",
            error_class="UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
            message_parameters={"className": "DataFrameQueryContext", "methodName": "startIndex"},
            sql_state="0A000",
            server_stacktrace=None,
            display_server_stacktrace=False,
            query_contexts=[],
        )

    def stopIndex(self) -> int:
        raise UnsupportedOperationException(
            "",
            error_class="UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
            message_parameters={"className": "DataFrameQueryContext", "methodName": "stopIndex"},
            sql_state="0A000",
            server_stacktrace=None,
            display_server_stacktrace=False,
            query_contexts=[],
        )

    def fragment(self) -> str:
        return str(self._q.fragment)

    def callSite(self) -> str:
        return str(self._q.call_site)

    def summary(self) -> str:
        return str(self._q.summary)
