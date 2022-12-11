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

from threading import RLock
from collections.abc import Sized

import numpy as np
import pandas as pd
import pyarrow as pa

from pyspark.sql.types import DataType, StructType

from pyspark.sql.connect.client import SparkConnectClient
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import SQL, Range, LocalRelation
from pyspark.sql.connect.readwriter import DataFrameReader
from pyspark.sql.utils import to_str

from typing import (
    Optional,
    Any,
    Union,
    Dict,
    List,
    Tuple,
    cast,
    overload,
    Iterable,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import OptionalPrimitiveType


# TODO(SPARK-38912): This method can be dropped once support for Python 3.8 is dropped
# In Python 3.9, the @property decorator has been made compatible with the
# @classmethod decorator (https://docs.python.org/3.9/library/functions.html#classmethod)
#
# @classmethod + @property is also affected by a bug in Python's docstring which was backported
# to Python 3.9.6 (https://github.com/python/cpython/pull/28838)
class classproperty(property):
    """Same as Python's @property decorator, but for class attributes.

    Examples
    --------
    >>> class Builder:
    ...    def build(self):
    ...        return MyClass()
    ...
    >>> class MyClass:
    ...     @classproperty
    ...     def builder(cls):
    ...         print("instantiating new builder")
    ...         return Builder()
    ...
    >>> c1 = MyClass.builder
    instantiating new builder
    >>> c2 = MyClass.builder
    instantiating new builder
    >>> c1 == c2
    False
    >>> isinstance(c1.build(), MyClass)
    True
    """

    def __get__(self, instance: Any, owner: Any = None) -> "SparkSession.Builder":
        # The "type: ignore" below silences the following error from mypy:
        # error: Argument 1 to "classmethod" has incompatible
        # type "Optional[Callable[[Any], Any]]";
        # expected "Callable[..., Any]"  [arg-type]
        return classmethod(self.fget).__get__(None, owner)()  # type: ignore


class SparkSession(object):
    """Conceptually the remote spark session that communicates with the server"""

    class Builder:
        """Builder for :class:`SparkSession`."""

        _lock = RLock()

        def __init__(self) -> None:
            self._options: Dict[str, Any] = {}

        @overload
        def config(self, key: str, value: Any) -> "SparkSession.Builder":
            ...

        @overload
        def config(self, *, map: Dict[str, "OptionalPrimitiveType"]) -> "SparkSession.Builder":
            ...

        def config(
            self,
            key: Optional[str] = None,
            value: Optional[Any] = None,
            *,
            map: Optional[Dict[str, "OptionalPrimitiveType"]] = None,
        ) -> "SparkSession.Builder":
            """Sets a config option. Options set using this method are automatically propagated to
            both :class:`SparkConf` and :class:`SparkSession`'s own configuration.

            .. versionadded:: 2.0.0

            Parameters
            ----------
            key : str, optional
                a key name string for configuration property
            value : str, optional
                a value for configuration property
            map: dictionary, optional
                a dictionary of configurations to set

                .. versionadded:: 3.4.0

            Returns
            -------
            :class:`SparkSession.Builder`

            Examples
            --------
            For a (key, value) pair, you can omit parameter names.

            >>> SparkSession.builder.config("spark.some.config.option", "some-value")
            <pyspark.sql.session.SparkSession.Builder...

            Additionally, you can pass a dictionary of configurations to set.

            >>> SparkSession.builder.config(
            ...     map={"spark.some.config.number": 123, "spark.some.config.float": 0.123})
            <pyspark.sql.session.SparkSession.Builder...
            """
            with self._lock:
                if map is not None:
                    for k, v in map.items():
                        self._options[k] = to_str(v)
                else:
                    self._options[cast(str, key)] = to_str(value)
                return self

        def master(self, master: str) -> "SparkSession.Builder":
            return self

        def appName(self, name: str) -> "SparkSession.Builder":
            """Sets a name for the application, which will be shown in the Spark web UI.

            If no application name is set, a randomly generated name will be used.

            .. versionadded:: 2.0.0

            Parameters
            ----------
            name : str
                an application name

            Returns
            -------
            :class:`SparkSession.Builder`

            Examples
            --------
            >>> SparkSession.builder.appName("My app")
            <pyspark.sql.session.SparkSession.Builder...
            """
            return self.config("spark.app.name", name)

        def remote(self, location: str = "sc://localhost") -> "SparkSession.Builder":
            return self.config("spark.connect.location", location)

        def enableHiveSupport(self) -> "SparkSession.Builder":
            raise NotImplementedError("enableHiveSupport not  implemented for Spark Connect")

        def getOrCreate(self) -> "SparkSession":
            """Creates a new instance."""
            return SparkSession(connectionString=self._options["spark.connect.location"])

    _client: SparkConnectClient

    # TODO(SPARK-38912): Replace @classproperty with @classmethod + @property once support for
    # Python 3.8 is dropped.
    #
    # In Python 3.9, the @property decorator has been made compatible with the
    # @classmethod decorator (https://docs.python.org/3.9/library/functions.html#classmethod)
    #
    # @classmethod + @property is also affected by a bug in Python's docstring which was backported
    # to Python 3.9.6 (https://github.com/python/cpython/pull/28838)
    @classproperty
    def builder(cls) -> Builder:
        """Creates a :class:`Builder` for constructing a :class:`SparkSession`."""
        return cls.Builder()

    def __init__(self, connectionString: str, userId: Optional[str] = None):
        """
        Creates a new SparkSession for the Spark Connect interface.

        Parameters
        ----------
        connectionString: Optional[str]
            Connection string that is used to extract the connection parameters and configure
            the GRPC connection. Defaults to `sc://localhost`.
        userId : Optional[str]
            Optional unique user ID that is used to differentiate multiple users and
            isolate their Spark Sessions. If the `user_id` is not set, will default to
            the $USER environment. Defining the user ID as part of the connection string
            takes precedence.
        """
        # Parse the connection string.
        self._client = SparkConnectClient(connectionString)

    @property
    def read(self) -> "DataFrameReader":
        """
        Returns a :class:`DataFrameReader` that can be used to read data
        in as a :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Returns
        -------
        :class:`DataFrameReader`

        Examples
        --------
        >>> spark.read
        <pyspark.sql.connect.readwriter.DataFrameReader object ...>

        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.format('json').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        return DataFrameReader(self)

    def createDataFrame(
        self,
        data: Union["pd.DataFrame", "np.ndarray", Iterable[Any]],
        schema: Optional[Union[StructType, str, List[str], Tuple[str, ...]]] = None,
    ) -> "DataFrame":
        """
        Creates a :class:`DataFrame` from a :class:`pandas.DataFrame`.

        .. versionadded:: 3.4.0


        Parameters
        ----------
        data : :class:`pandas.DataFrame` or :class:`list`, or :class:`numpy.ndarray`.
        schema : :class:`pyspark.sql.types.DataType`, str or list, optional

            When ``schema`` is :class:`pyspark.sql.types.DataType` or a datatype string, it must
            match the real data, or an exception will be thrown at runtime. If the given schema is
            not :class:`pyspark.sql.types.StructType`, it will be wrapped into a
            :class:`pyspark.sql.types.StructType` as its only field, and the field name will be
            "value". Each record will also be wrapped into a tuple, which can be converted to row
            later.

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> import pandas
        >>> pdf = pandas.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        >>> self.connect.createDataFrame(pdf).collect()
        [Row(a=1, b='a'), Row(a=2, b='b'), Row(a=3, b='c')]

        """
        assert data is not None
        if isinstance(data, DataFrame):
            raise TypeError("data is already a DataFrame")
        if isinstance(data, Sized) and len(data) == 0:
            raise ValueError("Input data cannot be empty")

        _schema: Optional[StructType] = None
        _schema_str: Optional[str] = None
        _cols: Optional[List[str]] = None

        if isinstance(schema, StructType):
            _schema = schema

        elif isinstance(schema, str):
            _schema_str = schema

        elif isinstance(schema, (list, tuple)):
            # Must re-encode any unicode strings to be consistent with StructField names
            _cols = [x.encode("utf-8") if not isinstance(x, str) else x for x in schema]

        # Create the Pandas DataFrame
        if isinstance(data, pd.DataFrame):
            pdf = data

        elif isinstance(data, np.ndarray):
            # `data` of numpy.ndarray type will be converted to a pandas DataFrame,
            if data.ndim not in [1, 2]:
                raise ValueError("NumPy array input should be of 1 or 2 dimensions.")

            pdf = pd.DataFrame(data)

            if _cols is None:
                if data.ndim == 1 or data.shape[1] == 1:
                    _cols = ["value"]
                else:
                    _cols = ["_%s" % i for i in range(1, data.shape[1] + 1)]

        else:
            pdf = pd.DataFrame(list(data))

            if _cols is None:
                _cols = ["_%s" % i for i in range(1, pdf.shape[1] + 1)]

        # Validate number of columns
        num_cols = pdf.shape[1]
        if _schema is not None and len(_schema.fields) != num_cols:
            raise ValueError(
                f"Length mismatch: Expected axis has {num_cols} elements, "
                f"new values have {len(_schema.fields)} elements"
            )
        elif _cols is not None and len(_cols) != num_cols:
            raise ValueError(
                f"Length mismatch: Expected axis has {num_cols} elements, "
                f"new values have {len(_cols)} elements"
            )

        table = pa.Table.from_pandas(pdf)

        if _schema is not None:
            return DataFrame.withPlan(LocalRelation(table, schema=_schema), self)
        elif _schema_str is not None:
            return DataFrame.withPlan(LocalRelation(table, schema=_schema_str), self)
        elif _cols is not None and len(_cols) > 0:
            return DataFrame.withPlan(LocalRelation(table), self).toDF(*_cols)
        else:
            return DataFrame.withPlan(LocalRelation(table), self)

    @property
    def client(self) -> "SparkConnectClient":
        """
        Gives access to the Spark Connect client. In normal cases this is not necessary to be used
        and only relevant for testing.
        Returns
        -------
        :class:`SparkConnectClient`
        """
        return self._client

    def register_udf(self, function: Any, return_type: Union[str, DataType]) -> str:
        return self._client.register_udf(function, return_type)

    def sql(self, sql_string: str) -> "DataFrame":
        return DataFrame.withPlan(SQL(sql_string), self)

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numPartitions: Optional[int] = None,
    ) -> DataFrame:
        """
        Create a :class:`DataFrame` with column named ``id`` and typed Long,
        containing elements in a range from ``start`` to ``end`` (exclusive) with
        step value ``step``.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        start : int
            the start value
        end : int
            the end value (exclusive)
        step : int, optional
            the incremental step (default: 1)
        numPartitions : int, optional
            the number of partitions of the DataFrame

        Returns
        -------
        :class:`DataFrame`
        """
        if end is None:
            actual_end = start
            start = 0
        else:
            actual_end = end

        return DataFrame.withPlan(
            Range(start=start, end=actual_end, step=step, num_partitions=numPartitions), self
        )
