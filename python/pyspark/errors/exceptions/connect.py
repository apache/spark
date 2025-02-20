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
    StreamingPythonRunnerInitializationException as BaseStreamingPythonRunnerInitException,
    PickleException as BasePickleException,
    UnknownException as BaseUnknownException,
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
    raw_classes = info.metadata.get("classes")
    classes: List[str] = json.loads(raw_classes) if raw_classes else []
    sql_state = info.metadata.get("sqlState")
    error_class = info.metadata.get("errorClass")
    raw_message_parameters = info.metadata.get("messageParameters")
    message_parameters: Dict[str, str] = (
        json.loads(raw_message_parameters) if raw_message_parameters else {}
    )
    stacktrace: Optional[str] = None

    if resp is not None and resp.HasField("root_error_idx"):
        message = resp.errors[resp.root_error_idx].message
        stacktrace = _extract_jvm_stacktrace(resp)
    else:
        message = truncated_message
        stacktrace = info.metadata.get("stackTrace")
        display_server_stacktrace = display_server_stacktrace if stacktrace else False

    contexts = None
    if resp and resp.HasField("root_error_idx"):
        root_error = resp.errors[resp.root_error_idx]
        if hasattr(root_error, "spark_throwable"):
            message_parameters = dict(root_error.spark_throwable.message_parameters)
            contexts = [
                SQLQueryContext(c)
                if c.context_type == pb2.FetchErrorDetailsResponse.QueryContext.SQL
                else DataFrameQueryContext(c)
                for c in root_error.spark_throwable.query_contexts
            ]

    if "org.apache.spark.api.python.PythonException" in classes:
        return PythonException(
            "\n  An exception was thrown from the Python worker. "
            "Please see the stack trace below.\n%s" % message
        )

    # Return exception based on class mapping
    for error_class_name in classes:
        ExceptionClass = EXCEPTION_CLASS_MAPPING.get(error_class_name)
        if ExceptionClass is SparkException:
            for third_party_exception_class in THIRD_PARTY_EXCEPTION_CLASS_MAPPING:
                ExceptionClass = (
                    THIRD_PARTY_EXCEPTION_CLASS_MAPPING.get(third_party_exception_class)
                    if third_party_exception_class in message
                    else SparkException
                )

        if ExceptionClass:
            return ExceptionClass(
                message,
                errorClass=error_class,
                messageParameters=message_parameters,
                sql_state=sql_state,
                server_stacktrace=stacktrace,
                display_server_stacktrace=display_server_stacktrace,
                contexts=contexts,
            )

    # Return UnknownException if there is no matched exception class
    return UnknownException(
        message,
        reason=info.reason,
        messageParameters=message_parameters,
        errorClass=error_class,
        sql_state=sql_state,
        server_stacktrace=stacktrace,
        display_server_stacktrace=display_server_stacktrace,
        contexts=contexts,
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
        errorClass: Optional[str] = None,
        messageParameters: Optional[Dict[str, str]] = None,
        reason: Optional[str] = None,
        sql_state: Optional[str] = None,
        server_stacktrace: Optional[str] = None,
        display_server_stacktrace: bool = False,
        contexts: Optional[List[BaseQueryContext]] = None,
    ) -> None:
        if contexts is None:
            contexts = []
        self._message = message  # type: ignore[assignment]
        if reason is not None:
            self._message = f"({reason}) {self._message}"

        # PySparkException has the assumption that errorClass and messageParameters are
        # only occurring together. If only one is set, we assume the message to be fully
        # parsed.
        tmp_error_class = errorClass
        tmp_message_parameters = messageParameters
        if errorClass is not None and messageParameters is None:
            tmp_error_class = None
        elif errorClass is None and messageParameters is not None:
            tmp_message_parameters = None

        super().__init__(
            message=self._message,
            errorClass=tmp_error_class,
            messageParameters=tmp_message_parameters,
        )
        self._errorClass = errorClass
        self._sql_state: Optional[str] = sql_state
        self._stacktrace: Optional[str] = server_stacktrace
        self._display_stacktrace: bool = display_server_stacktrace
        self._contexts: List[BaseQueryContext] = contexts
        self._log_exception()

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


class UnknownException(SparkConnectGrpcException, BaseUnknownException):
    """
    Exception for unmapped errors in Spark Connect.
    This class is functionally identical to SparkConnectGrpcException but has a different name
    for consistency.
    """

    def __init__(
        self,
        message: Optional[str] = None,
        errorClass: Optional[str] = None,
        messageParameters: Optional[Dict[str, str]] = None,
        reason: Optional[str] = None,
        sql_state: Optional[str] = None,
        server_stacktrace: Optional[str] = None,
        display_server_stacktrace: bool = False,
        contexts: Optional[List[BaseQueryContext]] = None,
    ) -> None:
        super().__init__(
            message=message,
            errorClass=errorClass,
            messageParameters=messageParameters,
            reason=reason,
            sql_state=sql_state,
            server_stacktrace=server_stacktrace,
            display_server_stacktrace=display_server_stacktrace,
            contexts=contexts,
        )


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


class InvalidPlanInput(SparkConnectGrpcException):
    """
    Error thrown when a connect plan is not valid.
    """


class InvalidCommandInput(SparkConnectGrpcException):
    """
    Error thrown when a connect command is not valid.
    """


class StreamingPythonRunnerInitializationException(
    SparkConnectGrpcException, BaseStreamingPythonRunnerInitException
):
    """
    Failed to initialize a streaming Python runner.
    """


class PickleException(SparkConnectGrpcException, BasePickleException):
    """
    Represents an exception which is failed while pickling from server side
    such as `net.razorvine.pickle.PickleException`. This is different from `PySparkPicklingError`
    which represents an exception failed from Python built-in `pickle.PicklingError`.
    """


# Update EXCEPTION_CLASS_MAPPING here when adding a new exception
EXCEPTION_CLASS_MAPPING = {
    "org.apache.spark.sql.catalyst.parser.ParseException": ParseException,
    "org.apache.spark.sql.AnalysisException": AnalysisException,
    "org.apache.spark.sql.streaming.StreamingQueryException": StreamingQueryException,
    "org.apache.spark.sql.execution.QueryExecutionException": QueryExecutionException,
    "java.lang.NumberFormatException": NumberFormatException,
    "java.lang.IllegalArgumentException": IllegalArgumentException,
    "java.lang.ArithmeticException": ArithmeticException,
    "java.lang.UnsupportedOperationException": UnsupportedOperationException,
    "java.lang.ArrayIndexOutOfBoundsException": ArrayIndexOutOfBoundsException,
    "java.time.DateTimeException": DateTimeException,
    "org.apache.spark.SparkRuntimeException": SparkRuntimeException,
    "org.apache.spark.SparkUpgradeException": SparkUpgradeException,
    "org.apache.spark.api.python.PythonException": PythonException,
    "org.apache.spark.SparkNoSuchElementException": SparkNoSuchElementException,
    "org.apache.spark.SparkException": SparkException,
    "org.apache.spark.sql.connect.common.InvalidPlanInput": InvalidPlanInput,
    "org.apache.spark.sql.connect.common.InvalidCommandInput": InvalidCommandInput,
    "org.apache.spark.api.python.StreamingPythonRunner"
    "$StreamingPythonRunnerInitializationException": StreamingPythonRunnerInitializationException,
}

THIRD_PARTY_EXCEPTION_CLASS_MAPPING = {
    "net.razorvine.pickle.PickleException": PickleException,
}


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
            errorClass="UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
            messageParameters={"className": "SQLQueryContext", "methodName": "callSite"},
            sql_state="0A000",
            server_stacktrace=None,
            display_server_stacktrace=False,
            contexts=[],
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
            errorClass="UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
            messageParameters={"className": "DataFrameQueryContext", "methodName": "objectType"},
            sql_state="0A000",
            server_stacktrace=None,
            display_server_stacktrace=False,
            contexts=[],
        )

    def objectName(self) -> str:
        raise UnsupportedOperationException(
            "",
            errorClass="UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
            messageParameters={"className": "DataFrameQueryContext", "methodName": "objectName"},
            sql_state="0A000",
            server_stacktrace=None,
            display_server_stacktrace=False,
            contexts=[],
        )

    def startIndex(self) -> int:
        raise UnsupportedOperationException(
            "",
            errorClass="UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
            messageParameters={"className": "DataFrameQueryContext", "methodName": "startIndex"},
            sql_state="0A000",
            server_stacktrace=None,
            display_server_stacktrace=False,
            contexts=[],
        )

    def stopIndex(self) -> int:
        raise UnsupportedOperationException(
            "",
            errorClass="UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
            messageParameters={"className": "DataFrameQueryContext", "methodName": "stopIndex"},
            sql_state="0A000",
            server_stacktrace=None,
            display_server_stacktrace=False,
            contexts=[],
        )

    def fragment(self) -> str:
        return str(self._q.fragment)

    def callSite(self) -> str:
        return str(self._q.call_site)

    def summary(self) -> str:
        return str(self._q.summary)
