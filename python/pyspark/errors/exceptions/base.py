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

from typing import Dict, Optional, cast

from pyspark.errors.utils import ErrorClassesReader


class PySparkException(Exception):
    """
    Base Exception for handling errors generated from PySpark.
    """

    def __init__(
        self,
        message: Optional[str] = None,
        error_class: Optional[str] = None,
        message_parameters: Optional[Dict[str, str]] = None,
    ):
        # `message` vs `error_class` & `message_parameters` are mutually exclusive.
        assert (message is not None and (error_class is None and message_parameters is None)) or (
            message is None and (error_class is not None and message_parameters is not None)
        )

        self.error_reader = ErrorClassesReader()

        if message is None:
            self.message = self.error_reader.get_error_message(
                cast(str, error_class), cast(Dict[str, str], message_parameters)
            )
        else:
            self.message = message

        self.error_class = error_class
        self.message_parameters = message_parameters

    def getErrorClass(self) -> Optional[str]:
        """
        Returns an error class as a string.

        .. versionadded:: 3.4.0

        See Also
        --------
        :meth:`PySparkException.getMessageParameters`
        :meth:`PySparkException.getSqlState`
        """
        return self.error_class

    def getMessageParameters(self) -> Optional[Dict[str, str]]:
        """
        Returns a message parameters as a dictionary.

        .. versionadded:: 3.4.0

        See Also
        --------
        :meth:`PySparkException.getErrorClass`
        :meth:`PySparkException.getSqlState`
        """
        return self.message_parameters

    def getSqlState(self) -> None:
        """
        Returns an SQLSTATE as a string.

        Errors generated in Python have no SQLSTATE, so it always returns None.

        .. versionadded:: 3.4.0

        See Also
        --------
        :meth:`PySparkException.getErrorClass`
        :meth:`PySparkException.getMessageParameters`
        """
        return None

    def __str__(self) -> str:
        if self.getErrorClass() is not None:
            return f"[{self.getErrorClass()}] {self.message}"
        else:
            return self.message


class AnalysisException(PySparkException):
    """
    Failed to analyze a SQL query plan.
    """


class SessionNotSameException(PySparkException):
    """
    Performed the same operation on different SparkSession.
    """


class TempTableAlreadyExistsException(AnalysisException):
    """
    Failed to create temp view since it is already exists.
    """


class ParseException(AnalysisException):
    """
    Failed to parse a SQL command.
    """


class IllegalArgumentException(PySparkException):
    """
    Passed an illegal or inappropriate argument.
    """


class ArithmeticException(PySparkException):
    """
    Arithmetic exception thrown from Spark with an error class.
    """


class UnsupportedOperationException(PySparkException):
    """
    Unsupported operation exception thrown from Spark with an error class.
    """


class ArrayIndexOutOfBoundsException(PySparkException):
    """
    Array index out of bounds exception thrown from Spark with an error class.
    """


class DateTimeException(PySparkException):
    """
    Datetime exception thrown from Spark with an error class.
    """


class NumberFormatException(IllegalArgumentException):
    """
    Number format exception thrown from Spark with an error class.
    """


class StreamingQueryException(PySparkException):
    """
    Exception that stopped a :class:`StreamingQuery`.
    """


class QueryExecutionException(PySparkException):
    """
    Failed to execute a query.
    """


class PythonException(PySparkException):
    """
    Exceptions thrown from Python workers.
    """


class SparkRuntimeException(PySparkException):
    """
    Runtime exception thrown from Spark with an error class.
    """


class SparkUpgradeException(PySparkException):
    """
    Exception thrown because of Spark upgrade.
    """


class UnknownException(PySparkException):
    """
    None of the above exceptions.
    """


class PySparkValueError(PySparkException, ValueError):
    """
    Wrapper class for ValueError to support error classes.
    """


class PySparkTypeError(PySparkException, TypeError):
    """
    Wrapper class for TypeError to support error classes.
    """


class PySparkAttributeError(PySparkException, AttributeError):
    """
    Wrapper class for AttributeError to support error classes.
    """


class PySparkRuntimeError(PySparkException, RuntimeError):
    """
    Wrapper class for RuntimeError to support error classes.
    """


class PySparkAssertionError(PySparkException, AssertionError):
    """
    Wrapper class for AssertionError to support error classes.
    """


class PySparkNotImplementedError(PySparkException, NotImplementedError):
    """
    Wrapper class for NotImplementedError to support error classes.
    """
