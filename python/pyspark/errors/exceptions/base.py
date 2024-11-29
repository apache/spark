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
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Optional, cast, Iterable, TYPE_CHECKING, List

from pyspark.errors.utils import ErrorClassesReader
from pyspark.logger import PySparkLogger
from pickle import PicklingError

if TYPE_CHECKING:
    from pyspark.sql.types import Row


class PySparkException(Exception):
    """
    Base Exception for handling errors generated from PySpark.
    """

    def __init__(
        self,
        message: Optional[str] = None,
        errorClass: Optional[str] = None,
        messageParameters: Optional[Dict[str, str]] = None,
        contexts: Optional[List["QueryContext"]] = None,
    ):
        if contexts is None:
            contexts = []
        self._error_reader = ErrorClassesReader()

        if message is None:
            self._message = self._error_reader.get_error_message(
                cast(str, errorClass), cast(Dict[str, str], messageParameters)
            )
        else:
            self._message = message

        self._errorClass = errorClass
        self._messageParameters = messageParameters
        self._contexts = contexts

    def getErrorClass(self) -> Optional[str]:
        """
        Returns an error class as a string.

        .. versionadded:: 3.4.0

        See Also
        --------
        :meth:`PySparkException.getMessage`
        :meth:`PySparkException.getMessageParameters`
        :meth:`PySparkException.getQueryContext`
        :meth:`PySparkException.getSqlState`
        """
        return self._errorClass

    def getMessageParameters(self) -> Optional[Dict[str, str]]:
        """
        Returns a message parameters as a dictionary.

        .. versionadded:: 3.4.0

        See Also
        --------
        :meth:`PySparkException.getErrorClass`
        :meth:`PySparkException.getMessage`
        :meth:`PySparkException.getQueryContext`
        :meth:`PySparkException.getSqlState`
        """
        return self._messageParameters

    def getSqlState(self) -> Optional[str]:
        """
        Returns an SQLSTATE as a string.

        Errors generated in Python have no SQLSTATE, so it always returns None.

        .. versionadded:: 3.4.0

        See Also
        --------
        :meth:`PySparkException.getErrorClass`
        :meth:`PySparkException.getMessage`
        :meth:`PySparkException.getMessageParameters`
        :meth:`PySparkException.getQueryContext`
        """
        return None

    def getMessage(self) -> str:
        """
        Returns full error message.

        .. versionadded:: 4.0.0

        See Also
        --------
        :meth:`PySparkException.getErrorClass`
        :meth:`PySparkException.getMessageParameters`
        :meth:`PySparkException.getQueryContext`
        :meth:`PySparkException.getSqlState`
        """
        return f"[{self.getErrorClass()}] {self._message}"

    def getQueryContext(self) -> List["QueryContext"]:
        """
        Returns :class:`QueryContext`.

        .. versionadded:: 4.0.0

        See Also
        --------
        :meth:`PySparkException.getErrorClass`
        :meth:`PySparkException.getMessageParameters`
        :meth:`PySparkException.getMessage`
        :meth:`PySparkException.getSqlState`
        """
        return self._contexts

    def _log_exception(self) -> None:
        contexts = self.getQueryContext()
        context = contexts[0] if len(contexts) != 0 else None
        if context:
            if context.contextType().name == "DataFrame":
                logger = PySparkLogger.getLogger("DataFrameQueryContextLogger")
                call_site = context.callSite().split(":")
                line = call_site[1] if len(call_site) == 2 else ""
                logger.exception(
                    self.getMessage(),
                    file=call_site[0],
                    line=line,
                    fragment=context.fragment(),
                    errorClass=self.getErrorClass(),
                )
            else:
                logger = PySparkLogger.getLogger("SQLQueryContextLogger")
                logger.exception(
                    self.getMessage(),
                    errorClass=self.getErrorClass(),
                )

    def __str__(self) -> str:
        if self.getErrorClass() is not None:
            return self.getMessage()
        else:
            return self._message


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


class SparkNoSuchElementException(PySparkException):
    """
    Exception thrown for `java.util.NoSuchElementException`.
    """


class UnknownException(PySparkException):
    """
    None of the other exceptions.
    """


class PySparkValueError(PySparkException, ValueError):
    """
    Wrapper class for ValueError to support error classes.
    """


class PySparkTypeError(PySparkException, TypeError):
    """
    Wrapper class for TypeError to support error classes.
    """


class PySparkIndexError(PySparkException, IndexError):
    """
    Wrapper class for IndexError to support error classes.
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

    def __init__(
        self,
        message: Optional[str] = None,
        errorClass: Optional[str] = None,
        messageParameters: Optional[Dict[str, str]] = None,
        data: Optional[Iterable["Row"]] = None,
    ):
        super().__init__(message, errorClass, messageParameters)
        self.data = data


class PySparkNotImplementedError(PySparkException, NotImplementedError):
    """
    Wrapper class for NotImplementedError to support error classes.
    """


class PySparkPicklingError(PySparkException, PicklingError):
    """
    Wrapper class for pickle.PicklingError to support error classes.
    """


class RetriesExceeded(PySparkException):
    """
    Represents an exception which is considered retriable, but retry limits
    were exceeded
    """


class PySparkKeyError(PySparkException, KeyError):
    """
    Wrapper class for KeyError to support error classes.
    """


class PySparkImportError(PySparkException, ImportError):
    """
    Wrapper class for ImportError to support error classes.
    """


class QueryContextType(Enum):
    """
    The type of :class:`QueryContext`.

    .. versionadded:: 4.0.0
    """

    SQL = 0
    DataFrame = 1


class QueryContext(ABC):
    """
    Query context of a :class:`PySparkException`. It helps users understand
    where error occur while executing queries.

    .. versionadded:: 4.0.0
    """

    @abstractmethod
    def contextType(self) -> QueryContextType:
        """
        The type of this query context.
        """
        ...

    @abstractmethod
    def objectType(self) -> str:
        """
        The object type of the query which throws the exception.
        If the exception is directly from the main query, it should be an empty string.
        Otherwise, it should be the exact object type in upper case. For example, a "VIEW".
        """
        ...

    @abstractmethod
    def objectName(self) -> str:
        """
        The object name of the query which throws the exception.
        If the exception is directly from the main query, it should be an empty string.
        Otherwise, it should be the object name. For example, a view name "V1".
        """
        ...

    @abstractmethod
    def startIndex(self) -> int:
        """
        The starting index in the query text which throws the exception. The index starts from 0.
        """
        ...

    @abstractmethod
    def stopIndex(self) -> int:
        """
        The stopping index in the query which throws the exception. The index starts from 0.
        """
        ...

    @abstractmethod
    def fragment(self) -> str:
        """
        The corresponding fragment of the query which throws the exception.
        """
        ...

    @abstractmethod
    def callSite(self) -> str:
        """
        The user code (call site of the API) that caused throwing the exception.
        """
        ...

    @abstractmethod
    def summary(self) -> str:
        """
        Summary of the exception cause.
        """
        ...
