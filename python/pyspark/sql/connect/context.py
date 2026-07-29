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
from typing import (
    Optional,
    Union,
    Callable,
    Any,
    Iterable,
    List,
    Tuple,
    ClassVar,
    TYPE_CHECKING,
)

from pyspark import _NoValue
from pyspark._globals import _NoValueType
from pyspark.errors import PySparkNotImplementedError
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.connect.readwriter import DataFrameReader
from pyspark.sql.connect.streaming.readwriter import DataStreamReader
from pyspark.sql.connect.streaming.query import StreamingQueryManager
from pyspark.sql.types import AtomicType, BooleanType, DataType, StringType, StructField, StructType

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.connect.udf import UDFRegistration
    from pyspark.sql.connect.udtf import UDTFRegistration
    from pyspark.sql._typing import UserDefinedFunctionLike

# Internal module - not part of the public PySpark API surface.
# The public SQLContext/HiveContext are in pyspark.sql.context; this module
# is an implementation detail used by the Connect dispatch in that file.


class SQLContext:
    """The entry point for working with structured data (rows and columns) in Spark, in Spark 1.x.

    As of Spark 2.0, this is replaced by :class:`SparkSession`. However, we are keeping the class
    here for backward compatibility.

    This is the Spark Connect-compatible implementation. Unlike the classic implementation,
    it wraps a Connect :class:`SparkSession` directly and does not require a
    :class:`~pyspark.SparkContext`.

    .. deprecated:: 4.3.0
        Use :func:`SparkSession.builder.getOrCreate()` instead.

    Parameters
    ----------
    sparkSession : :class:`SparkSession`
        The Connect :class:`SparkSession` to wrap.
    """

    _instantiatedContext: ClassVar[Optional["SQLContext"]] = None

    def __init__(self, sparkSession: "SparkSession") -> None:
        warnings.warn(
            "Deprecated in 4.3.0. Use SparkSession.builder.getOrCreate() instead.",
            FutureWarning,
            stacklevel=2,
        )
        self.sparkSession = sparkSession
        if type(self)._instantiatedContext is None:
            type(self)._instantiatedContext = self

    @classmethod
    def _from_session(cls, sparkSession: "SparkSession") -> "SQLContext":
        """Create a new instance without emitting a deprecation warning."""
        ctx = object.__new__(cls)
        ctx.sparkSession = sparkSession
        return ctx

    @classmethod
    def _get_or_create_from_session(cls, sparkSession: "SparkSession") -> "SQLContext":
        """Return the cached instance or create one from an active Connect SparkSession.

        Called by the classic :meth:`pyspark.sql.context.SQLContext.getOrCreate` when
        running in Spark Connect mode, so users do not need to import from
        ``pyspark.sql.connect`` directly.

        Unlike the classic path (which checks ``_sc._jsc is None`` to detect a dead
        SparkContext), Connect sessions have no JVM lifecycle sentinel. Instead we
        re-create whenever the incoming ``sparkSession`` is not the same object as the
        one stored in the cached context, which handles the case where the previous
        session was stopped and a new one started.
        """
        if (
            cls._instantiatedContext is None
            or cls._instantiatedContext.sparkSession is not sparkSession
        ):
            cls._instantiatedContext = cls._from_session(sparkSession)
        return cls._instantiatedContext

    def newSession(self) -> "SQLContext":
        """Returns a new SQLContext as a new session, that has separate SQLConf,
        registered temporary views and UDFs, but shared table cache.

        .. versionadded:: 4.3.0

        Notes
        -----
        The returned session starts with empty state rather than inheriting this session's
        configuration, temporary views, or registered functions, matching the Scala Connect
        ``newSession()`` semantics. Unlike the classic
        :meth:`pyspark.sql.context.SQLContext.newSession`, configurations set through
        ``SparkSession.builder.config(...)`` are not reapplied to the new session.
        """
        return self._from_session(self.sparkSession.newSession())

    def setConf(self, key: str, value: Union[bool, int, str]) -> None:
        """Sets the given Spark SQL configuration property.

        .. versionadded:: 4.3.0
        """
        self.sparkSession.conf.set(key, value)

    def getConf(
        self, key: str, defaultValue: Union[Optional[str], _NoValueType] = _NoValue
    ) -> Optional[str]:
        """Returns the value of Spark SQL configuration property for the given key.

        If the key is not set and defaultValue is set, return
        defaultValue. If the key is not set and defaultValue is not set, return
        the system default value.

        .. versionadded:: 4.3.0
        """
        return self.sparkSession.conf.get(key, defaultValue)

    @property
    def udf(self) -> "UDFRegistration":
        """Returns a :class:`UDFRegistration` for UDF registration.

        .. versionadded:: 4.3.0

        Returns
        -------
        :class:`UDFRegistration`
        """
        return self.sparkSession.udf

    @property
    def udtf(self) -> "UDTFRegistration":
        """Returns a :class:`UDTFRegistration` for UDTF registration.

        .. versionadded:: 4.3.0

        Returns
        -------
        :class:`UDTFRegistration`
        """
        return self.sparkSession.udtf

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numPartitions: Optional[int] = None,
    ) -> DataFrame:
        """Create a :class:`DataFrame` with single :class:`~pyspark.sql.types.LongType` column
        named ``id``, containing elements in a range from ``start`` to ``end`` (exclusive) with
        step value ``step``.

        .. versionadded:: 4.3.0

        Parameters
        ----------
        start : int
            the start value
        end : int, optional
            the end value (exclusive)
        step : int, optional
            the incremental step (default: 1)
        numPartitions : int, optional
            the number of partitions of the DataFrame

        Returns
        -------
        :class:`DataFrame`
        """
        return self.sparkSession.range(start, end, step, numPartitions)

    def registerFunction(
        self, name: str, f: Callable[..., Any], returnType: Optional[DataType] = None
    ) -> "UserDefinedFunctionLike":
        """An alias for :func:`spark.udf.register`.
        See :meth:`pyspark.sql.UDFRegistration.register`.

        .. versionadded:: 4.3.0

        .. deprecated:: 4.3.0
            Use :func:`spark.udf.register` instead.
        """
        warnings.warn("Deprecated in 4.3.0. Use spark.udf.register instead.", FutureWarning)
        return self.sparkSession.udf.register(name, f, returnType)

    def registerJavaFunction(
        self, name: str, javaClassName: str, returnType: Optional[DataType] = None
    ) -> None:
        """Not supported in Spark Connect.

        .. versionadded:: 4.3.0
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "registerJavaFunction"},
        )

    def createDataFrame(
        self,
        data: Union["pd.DataFrame", "np.ndarray", "pa.Table", Iterable[Any]],
        schema: Optional[Union[AtomicType, StructType, str, List[str], Tuple[str, ...]]] = None,
        samplingRatio: Optional[float] = None,
        verifySchema: Optional[bool] = None,
    ) -> DataFrame:
        """Creates a :class:`DataFrame` from an iterable, a :class:`pandas.DataFrame`,
        or a :class:`pyarrow.Table`.

        .. versionadded:: 4.3.0

        Parameters
        ----------
        data : iterable
            an iterable of any kind of SQL data representation (:class:`Row`,
            :class:`tuple`, ``int``, ``boolean``, etc.), :class:`list`,
            :class:`pandas.DataFrame`, or :class:`pyarrow.Table`.
        schema : :class:`~pyspark.sql.types.DataType`, str or list, optional
            a :class:`~pyspark.sql.types.DataType` or a datatype string or a list/tuple of
            column names.
        samplingRatio : float, optional
            the sample ratio of rows used for inferring the schema.
        verifySchema : bool, optional
            verify data types of every row against schema.

        Returns
        -------
        :class:`DataFrame`
        """
        return self.sparkSession.createDataFrame(data, schema, samplingRatio, verifySchema)

    def registerDataFrameAsTable(self, df: DataFrame, tableName: str) -> None:
        """Registers the given :class:`DataFrame` as a temporary table in the catalog.

        Temporary tables exist only during the lifetime of this instance of :class:`SQLContext`.

        .. versionadded:: 4.3.0
        """
        df.createOrReplaceTempView(tableName)

    def dropTempTable(self, tableName: str) -> None:
        """Remove the temporary table from catalog.

        .. versionadded:: 4.3.0
        """
        self.sparkSession.catalog.dropTempView(tableName)

    def createExternalTable(
        self,
        tableName: str,
        path: Optional[str] = None,
        source: Optional[str] = None,
        schema: Optional[StructType] = None,
        **options: str,
    ) -> DataFrame:
        """Creates an external table based on the dataset in a data source.

        It returns the DataFrame associated with the external table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created external table.

        .. versionadded:: 4.3.0

        Returns
        -------
        :class:`DataFrame`
        """
        return self.sparkSession.catalog.createExternalTable(
            tableName, path, source, schema, **options
        )

    def sql(self, sqlQuery: str) -> DataFrame:
        """Returns a :class:`DataFrame` representing the result of the given query.

        .. versionadded:: 4.3.0

        Returns
        -------
        :class:`DataFrame`
        """
        return self.sparkSession.sql(sqlQuery)

    def table(self, tableName: str) -> DataFrame:
        """Returns the specified table or view as a :class:`DataFrame`.

        .. versionadded:: 4.3.0

        Returns
        -------
        :class:`DataFrame`
        """
        return self.sparkSession.table(tableName)

    def tables(self, dbName: Optional[str] = None) -> DataFrame:
        """Returns a :class:`DataFrame` containing names of tables in the given database.

        If ``dbName`` is not specified, the current database will be used.

        The returned DataFrame has three columns: ``namespace``, ``tableName`` and
        ``isTemporary`` (a column with :class:`~pyspark.sql.types.BooleanType` indicating if a
        table is a temporary one or not).

        .. versionadded:: 4.3.0

        Parameters
        ----------
        dbName: str, optional
            name of the database to use.

        Returns
        -------
        :class:`DataFrame`
        """
        schema = StructType(
            [
                StructField("namespace", StringType(), nullable=True),
                StructField("tableName", StringType(), nullable=True),
                StructField("isTemporary", BooleanType(), nullable=False),
            ]
        )
        # Use catalog.listTables() rather than SHOW TABLES so the column names are always
        # (namespace, tableName, isTemporary), matching the classic implementation.
        # SHOW TABLES returns "database" vs "namespace" depending on the active catalog.
        rows = [
            # Join the full namespace ("a.b") to match classic SHOW TABLES, which emits the
            # quoted namespace; keeping only the last part would drop levels under a v2 catalog.
            (".".join(t.namespace) if t.namespace else "", t.name, t.isTemporary)
            for t in self.sparkSession.catalog.listTables(dbName)
        ]
        return self.sparkSession.createDataFrame(rows, schema)

    def tableNames(self, dbName: Optional[str] = None) -> List[str]:
        """Returns a list of names of tables in the database ``dbName``.

        .. versionadded:: 4.3.0

        Parameters
        ----------
        dbName: str
            name of the database to use. Default to the current database.

        Returns
        -------
        list
            list of table names as strings
        """
        return [t.name for t in self.sparkSession.catalog.listTables(dbName)]

    def cacheTable(self, tableName: str) -> None:
        """Caches the specified table in-memory.

        .. versionadded:: 4.3.0
        """
        self.sparkSession.catalog.cacheTable(tableName)

    def uncacheTable(self, tableName: str) -> None:
        """Removes the specified table from the in-memory cache.

        .. versionadded:: 4.3.0
        """
        self.sparkSession.catalog.uncacheTable(tableName)

    def clearCache(self) -> None:
        """Removes all cached tables from the in-memory cache.

        .. versionadded:: 4.3.0
        """
        self.sparkSession.catalog.clearCache()

    @property
    def read(self) -> DataFrameReader:
        """Returns a :class:`DataFrameReader` that can be used to read data
        in as a :class:`DataFrame`.

        .. versionadded:: 4.3.0

        Returns
        -------
        :class:`DataFrameReader`
        """
        return self.sparkSession.read

    @property
    def readStream(self) -> DataStreamReader:
        """Returns a :class:`DataStreamReader` that can be used to read data streams
        as a streaming :class:`DataFrame`.

        .. versionadded:: 4.3.0

        Notes
        -----
        This API is evolving.

        Returns
        -------
        :class:`DataStreamReader`
        """
        return self.sparkSession.readStream

    @property
    def streams(self) -> StreamingQueryManager:
        """Returns a :class:`StreamingQueryManager` that allows managing all the
        :class:`~pyspark.sql.streaming.StreamingQuery` instances active on this
        context.

        .. versionadded:: 4.3.0

        Notes
        -----
        This API is evolving.
        """
        return self.sparkSession.streams


class HiveContext(SQLContext):
    """Not supported in Spark Connect.

    .. deprecated:: 4.3.0
        Use SparkSession.builder.enableHiveSupport().getOrCreate().
    """

    # Override to prevent inheriting SQLContext's cached instance, which would cause
    # _get_or_create_from_session to skip _from_session and return the wrong type.
    _instantiatedContext: ClassVar[Optional["SQLContext"]] = None

    def __init__(self, sparkSession: "SparkSession") -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "HiveContext"},
        )

    @classmethod
    def _from_session(cls, sparkSession: "SparkSession") -> "SQLContext":
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "HiveContext"},
        )
