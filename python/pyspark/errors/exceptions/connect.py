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

from typing import Dict, Optional

from pyspark.errors.exceptions.base import (
    AnalysisException as BaseAnalysisException,
    IllegalArgumentException as BaseIllegalArgumentException,
    ParseException as BaseParseException,
    PySparkException,
    PythonException as BasePythonException,
    TempTableAlreadyExistsException as BaseTempTableAlreadyExistsException,
)


class SparkConnectException(PySparkException):
    """
    Exception thrown from Spark Connect.
    """


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

    def __init__(
        self,
        message: Optional[str] = None,
        error_class: Optional[str] = None,
        message_parameters: Optional[Dict[str, str]] = None,
        plan: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        self.message = message  # type: ignore[assignment]
        if plan is not None:
            self.message = f"{self.message}\nPlan: {plan}"

        super().__init__(
            message=self.message,
            error_class=error_class,
            message_parameters=message_parameters,
            reason=reason,
        )


class TempTableAlreadyExistsException(AnalysisException, BaseTempTableAlreadyExistsException):
    """
    Failed to create temp view from Spark Connect server since it is already exists.
    """


class ParseException(SparkConnectGrpcException, BaseParseException):
    """
    Failed to parse a SQL command from Spark Connect server.
    """


class IllegalArgumentException(SparkConnectGrpcException, BaseIllegalArgumentException):
    """
    Passed an illegal or inappropriate argument from Spark Connect server.
    """


class PythonException(SparkConnectGrpcException, BasePythonException):
    """
    Exceptions thrown from Spark Connect server.
    """
