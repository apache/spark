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
from typing import Dict, Optional, TYPE_CHECKING


from pyspark.errors.exceptions.base import (
    AnalysisException as BaseAnalysisException,
    IllegalArgumentException as BaseIllegalArgumentException,
    ParseException as BaseParseException,
    PySparkException,
    PythonException as BasePythonException,
    StreamingQueryException as BaseStreamingQueryException,
    QueryExecutionException as BaseQueryExecutionException,
    SparkUpgradeException as BaseSparkUpgradeException,
)

if TYPE_CHECKING:
    from google.rpc.error_details_pb2 import ErrorInfo


class SparkConnectException(PySparkException):
    """
    Exception thrown from Spark Connect.
    """


def convert_exception(info: "ErrorInfo", message: str) -> SparkConnectException:
    classes = []
    if "classes" in info.metadata:
        classes = json.loads(info.metadata["classes"])

    if "org.apache.spark.sql.catalyst.parser.ParseException" in classes:
        return ParseException(message)
    # Order matters. ParseException inherits AnalysisException.
    elif "org.apache.spark.sql.AnalysisException" in classes:
        return AnalysisException(message)
    elif "org.apache.spark.sql.streaming.StreamingQueryException" in classes:
        return StreamingQueryException(message)
    elif "org.apache.spark.sql.execution.QueryExecutionException" in classes:
        return QueryExecutionException(message)
    elif "java.lang.IllegalArgumentException" in classes:
        return IllegalArgumentException(message)
    elif "org.apache.spark.SparkUpgradeException" in classes:
        return SparkUpgradeException(message)
    elif "org.apache.spark.api.python.PythonException" in classes:
        return PythonException(
            "\n  An exception was thrown from the Python worker. "
            "Please see the stack trace below.\n%s" % message
        )
    else:
        return SparkConnectGrpcException(message, reason=info.reason)


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
    ) -> None:
        self.message = message  # type: ignore[assignment]
        if reason is not None:
            self.message = f"({reason}) {self.message}"

        super().__init__(
            message=self.message,
            error_class=error_class,
            message_parameters=message_parameters,
        )


class AnalysisException(SparkConnectGrpcException, BaseAnalysisException):
    """
    Failed to analyze a SQL query plan from Spark Connect server.
    """


class ParseException(SparkConnectGrpcException, BaseParseException):
    """
    Failed to parse a SQL command from Spark Connect server.
    """


class IllegalArgumentException(SparkConnectGrpcException, BaseIllegalArgumentException):
    """
    Passed an illegal or inappropriate argument from Spark Connect server.
    """


class StreamingQueryException(SparkConnectGrpcException, BaseStreamingQueryException):
    """
    Exception that stopped a :class:`StreamingQuery` from Spark Connect server.
    """


class QueryExecutionException(SparkConnectGrpcException, BaseQueryExecutionException):
    """
    Failed to execute a query from Spark Connect server.
    """


class SparkUpgradeException(SparkConnectGrpcException, BaseSparkUpgradeException):
    """
    Exception thrown because of Spark upgrade from Spark Connect
    """


class PythonException(SparkConnectGrpcException, BasePythonException):
    """
    Exceptions thrown from Spark Connect server.
    """
