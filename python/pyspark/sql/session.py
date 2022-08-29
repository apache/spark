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

import sys
import warnings
from collections.abc import Sized
from functools import reduce
from threading import RLock
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
    no_type_check,
    overload,
    TYPE_CHECKING,
)

from py4j.java_gateway import JavaObject

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD
from pyspark.sql.conf import RuntimeConfig
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.pandas.conversion import SparkConversionMixin
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.sql_formatter import SQLStringFormatter
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import (
    AtomicType,
    DataType,
    StructField,
    StructType,
    _make_type_verifier,
    _infer_schema,
    _has_nulltype,
    _merge_type,
    _create_converter,
    _parse_datatype_string,
    _from_numpy_type,
)
from pyspark.sql.utils import install_exception_handler, is_timestamp_ntz_preferred

if TYPE_CHECKING:
    from pyspark.sql._typing import AtomicValue, RowLike, OptionalPrimitiveType
    from pyspark.sql.catalog import Catalog
    from pyspark.sql.pandas._typing import ArrayLike, DataFrameLike as PandasDataFrameLike
    from pyspark.sql.streaming import StreamingQueryManager
    from pyspark.sql.udf import UDFRegistration


__all__ = ["SparkSession"]


def _monkey_patch_RDD(sparkSession: "SparkSession") -> None:
    @no_type_check
    def toDF(self, schema=None, sampleRatio=None):
        """
        Converts current :class:`RDD` into a :class:`DataFrame`

        This is a shorthand for ``spark.createDataFrame(rdd, schema, sampleRatio)``

        Parameters
        ----------
        schema : :class:`pyspark.sql.types.DataType`, str or list, optional
            a :class:`pyspark.sql.types.DataType` or a datatype string or a list of
            column names, default is None.  The data type string format equals to
            :class:`pyspark.sql.types.DataType.simpleString`, except that top level struct type can
            omit the ``struct<>`` and atomic types use ``typeName()`` as their format, e.g. use
            ``byte`` instead of ``tinyint`` for :class:`pyspark.sql.types.ByteType`.
            We can also use ``int`` as a short name for :class:`pyspark.sql.types.IntegerType`.
        sampleRatio : float, optional
            the sample ratio of rows used for inferring

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> rdd = spark.range(1).rdd.map(lambda x: tuple(x))
        >>> rdd.collect()
        [(0,)]
        >>> rdd.toDF().show()
        +---+
        | _1|
        +---+
        |  0|
        +---+
        """
        return sparkSession.createDataFrame(self, schema, sampleRatio)

    RDD.toDF = toDF  # type: ignore[assignment]


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


class SparkSession(SparkConversionMixin):
    """The entry point to programming Spark with the Dataset and DataFrame API.

    A SparkSession can be used create :class:`DataFrame`, register :class:`DataFrame` as
    tables, execute SQL over tables, cache tables, and read parquet files.
    To create a :class:`SparkSession`, use the following builder pattern:

    .. autoattribute:: builder
       :annotation:

    Examples
    --------
    Create a Spark session.

    >>> spark = (
    ...     SparkSession.builder
    ...         .master("local")
    ...         .appName("Word Count")
    ...         .config("spark.some.config.option", "some-value")
    ...         .getOrCreate()
    ... )

    Create a Spark session from a Spark context.

    >>> sc = spark.sparkContext
    >>> spark = SparkSession(sc)
    """

    class Builder:
        """Builder for :class:`SparkSession`."""

        _lock = RLock()

        def __init__(self) -> None:
            self._options: Dict[str, Any] = {}

        @overload
        def config(self, *, conf: SparkConf) -> "SparkSession.Builder":
            ...

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
            conf: Optional[SparkConf] = None,
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
            conf : :class:`SparkConf`, optional
                an instance of :class:`SparkConf`
            map: dictionary, optional
                a dictionary of configurations to set

                .. versionadded:: 3.4.0

            Returns
            -------
            :class:`SparkSession.Builder`

            Examples
            --------
            For an existing class:`SparkConf`, use `conf` parameter.

            >>> from pyspark.conf import SparkConf
            >>> SparkSession.builder.config(conf=SparkConf())
            <pyspark.sql.session.SparkSession.Builder...

            For a (key, value) pair, you can omit parameter names.

            >>> SparkSession.builder.config("spark.some.config.option", "some-value")
            <pyspark.sql.session.SparkSession.Builder...

            Additionally, you can pass a dictionary of configurations to set.

            >>> SparkSession.builder.config(
            ...     map={"spark.some.config.number": 123, "spark.some.config.float": 0.123})
            <pyspark.sql.session.SparkSession.Builder...
            """
            with self._lock:
                if conf is not None:
                    for (k, v) in conf.getAll():
                        self._options[k] = v
                elif map is not None:
                    for k, v in map.items():  # type: ignore[assignment]
                        self._options[k] = str(v)
                else:
                    self._options[cast(str, key)] = str(value)
                return self

        def master(self, master: str) -> "SparkSession.Builder":
            """Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]"
            to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone
            cluster.

            .. versionadded:: 2.0.0

            Parameters
            ----------
            master : str
                a url for spark master

            Returns
            -------
            :class:`SparkSession.Builder`

            Examples
            --------
            >>> SparkSession.builder.master("local")
            <pyspark.sql.session.SparkSession.Builder...
            """
            return self.config("spark.master", master)

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

        def enableHiveSupport(self) -> "SparkSession.Builder":
            """Enables Hive support, including connectivity to a persistent Hive metastore, support
            for Hive SerDes, and Hive user-defined functions.

            .. versionadded:: 2.0.0

            Returns
            -------
            :class:`SparkSession.Builder`

            Examples
            --------
            >>> SparkSession.builder.enableHiveSupport()
            <pyspark.sql.session.SparkSession.Builder...
            """
            return self.config("spark.sql.catalogImplementation", "hive")

        def getOrCreate(self) -> "SparkSession":
            """Gets an existing :class:`SparkSession` or, if there is no existing one, creates a
            new one based on the options set in this builder.

            .. versionadded:: 2.0.0


            Returns
            -------
            :class:`SparkSession`

            Examples
            --------
            This method first checks whether there is a valid global default SparkSession, and if
            yes, return that one. If no valid global default SparkSession exists, the method
            creates a new SparkSession and assigns the newly created SparkSession as the global
            default.

            >>> s1 = SparkSession.builder.config("k1", "v1").getOrCreate()
            >>> s1.conf.get("k1") == "v1"
            True

            The configuration of the SparkSession can be changed afterwards

            >>> s1.conf.set("k1", "v1_new")
            >>> s1.conf.get("k1") == "v1_new"
            True

            In case an existing SparkSession is returned, the config options specified
            in this builder will be applied to the existing SparkSession.

            >>> s2 = SparkSession.builder.config("k2", "v2").getOrCreate()
            >>> s1.conf.get("k1") == s2.conf.get("k1") == "v1_new"
            True
            >>> s1.conf.get("k2") == s2.conf.get("k2") == "v2"
            True
            """
            with self._lock:
                from pyspark.context import SparkContext
                from pyspark.conf import SparkConf

                session = SparkSession._instantiatedSession
                if session is None or session._sc._jsc is None:
                    sparkConf = SparkConf()
                    for key, value in self._options.items():
                        sparkConf.set(key, value)
                    # This SparkContext may be an existing one.
                    sc = SparkContext.getOrCreate(sparkConf)
                    # Do not update `SparkConf` for existing `SparkContext`, as it's shared
                    # by all sessions.
                    session = SparkSession(sc, options=self._options)
                else:
                    getattr(
                        getattr(session._jvm, "SparkSession$"), "MODULE$"
                    ).applyModifiableSettings(session._jsparkSession, self._options)
                return session

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

    _instantiatedSession: ClassVar[Optional["SparkSession"]] = None
    _activeSession: ClassVar[Optional["SparkSession"]] = None

    def __init__(
        self,
        sparkContext: SparkContext,
        jsparkSession: Optional[JavaObject] = None,
        options: Dict[str, Any] = {},
    ):
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm

        assert self._jvm is not None

        if jsparkSession is None:
            if (
                self._jvm.SparkSession.getDefaultSession().isDefined()
                and not self._jvm.SparkSession.getDefaultSession().get().sparkContext().isStopped()
            ):
                jsparkSession = self._jvm.SparkSession.getDefaultSession().get()
                getattr(getattr(self._jvm, "SparkSession$"), "MODULE$").applyModifiableSettings(
                    jsparkSession, options
                )
            else:
                jsparkSession = self._jvm.SparkSession(self._jsc.sc(), options)
        else:
            getattr(getattr(self._jvm, "SparkSession$"), "MODULE$").applyModifiableSettings(
                jsparkSession, options
            )
        self._jsparkSession = jsparkSession
        _monkey_patch_RDD(self)
        install_exception_handler()
        # If we had an instantiated SparkSession attached with a SparkContext
        # which is stopped now, we need to renew the instantiated SparkSession.
        # Otherwise, we will use invalid SparkSession when we call Builder.getOrCreate.
        if (
            SparkSession._instantiatedSession is None
            or SparkSession._instantiatedSession._sc._jsc is None
        ):
            SparkSession._instantiatedSession = self
            SparkSession._activeSession = self
            assert self._jvm is not None
            self._jvm.SparkSession.setDefaultSession(self._jsparkSession)
            self._jvm.SparkSession.setActiveSession(self._jsparkSession)

    def _repr_html_(self) -> str:
        return """
            <div>
                <p><b>SparkSession - {catalogImplementation}</b></p>
                {sc_HTML}
            </div>
        """.format(
            catalogImplementation=self.conf.get("spark.sql.catalogImplementation"),
            sc_HTML=self.sparkContext._repr_html_(),
        )

    @property
    def _jconf(self) -> "JavaObject":
        """Accessor for the JVM SQL-specific configurations"""
        return self._jsparkSession.sessionState().conf()

    def newSession(self) -> "SparkSession":
        """
        Returns a new :class:`SparkSession` as new session, that has separate SQLConf,
        registered temporary views and UDFs, but shared :class:`SparkContext` and
        table cache.

        .. versionadded:: 2.0.0

        Returns
        -------
        :class:`SparkSession`
            Spark session if an active session exists for the current thread

        Examples
        --------
        >>> spark.newSession()
        <...SparkSession object ...>
        """
        return self.__class__(self._sc, self._jsparkSession.newSession())

    @classmethod
    def getActiveSession(cls) -> Optional["SparkSession"]:
        """
        Returns the active :class:`SparkSession` for the current thread, returned by the builder

        .. versionadded:: 3.0.0

        Returns
        -------
        :class:`SparkSession`
            Spark session if an active session exists for the current thread

        Examples
        --------
        >>> s = SparkSession.getActiveSession()
        >>> df = s.createDataFrame([('Alice', 1)], ['name', 'age'])
        >>> df.select("age").show()
        +---+
        |age|
        +---+
        |  1|
        +---+
        """
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        if sc is None:
            return None
        else:
            assert sc._jvm is not None
            if sc._jvm.SparkSession.getActiveSession().isDefined():
                SparkSession(sc, sc._jvm.SparkSession.getActiveSession().get())
                return SparkSession._activeSession
            else:
                return None

    @property
    def sparkContext(self) -> SparkContext:
        """
        Returns the underlying :class:`SparkContext`.

        .. versionadded:: 2.0.0

        Returns
        -------
        :class:`SparkContext`

        Examples
        --------
        >>> spark.sparkContext
        <SparkContext master=... appName=...>

        Create an RDD from the Spark context

        >>> rdd = spark.sparkContext.parallelize([1, 2, 3])
        >>> rdd.collect()
        [1, 2, 3]
        """
        return self._sc

    @property
    def version(self) -> str:
        """
        The version of Spark on which this application is running.

        .. versionadded:: 2.0.0

        Returns
        -------
        str
            the version of Spark in string.

        Examples
        --------
        >>> _ = spark.version
        """
        return self._jsparkSession.version()

    @property
    def conf(self) -> RuntimeConfig:
        """Runtime configuration interface for Spark.

        This is the interface through which the user can get and set all Spark and Hadoop
        configurations that are relevant to Spark SQL. When getting the value of a config,
        this defaults to the value set in the underlying :class:`SparkContext`, if any.

        .. versionadded:: 2.0.0

        Returns
        -------
        :class:`pyspark.sql.conf.RuntimeConfig`

        Examples
        --------
        >>> spark.conf
        <pyspark.sql.conf.RuntimeConfig object ...>

        Set a runtime configuration for the session

        >>> spark.conf.set("key", "value")
        >>> spark.conf.get("key")
        'value'
        """
        if not hasattr(self, "_conf"):
            self._conf = RuntimeConfig(self._jsparkSession.conf())
        return self._conf

    @property
    def catalog(self) -> "Catalog":
        """Interface through which the user may create, drop, alter or query underlying
        databases, tables, functions, etc.

        .. versionadded:: 2.0.0

        Returns
        -------
        :class:`Catalog`

        Examples
        --------
        >>> spark.catalog
        <pyspark.sql.catalog.Catalog object ...>

        Create a temp view, show the list, and drop it.

        >>> spark.range(1).createTempView("test_view")
        >>> spark.catalog.listTables()
        [Table(name='test_view', catalog=None, namespace=[], description=None, ...
        >>> _ = spark.catalog.dropTempView("test_view")
        """
        from pyspark.sql.catalog import Catalog

        if not hasattr(self, "_catalog"):
            self._catalog = Catalog(self)
        return self._catalog

    @property
    def udf(self) -> "UDFRegistration":
        """Returns a :class:`UDFRegistration` for UDF registration.

        .. versionadded:: 2.0.0

        Returns
        -------
        :class:`UDFRegistration`

        Examples
        --------
        >>> spark.udf
        <pyspark.sql.udf.UDFRegistration object ...>

        Register a Python UDF, and use it in SQL.

        >>> strlen = spark.udf.register("strlen", lambda x: len(x))
        >>> spark.sql("SELECT strlen('test')").show()
        +------------+
        |strlen(test)|
        +------------+
        |           4|
        +------------+
        """
        from pyspark.sql.udf import UDFRegistration

        return UDFRegistration(self)

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numPartitions: Optional[int] = None,
    ) -> DataFrame:
        """
        Create a :class:`DataFrame` with single :class:`pyspark.sql.types.LongType` column named
        ``id``, containing elements in a range from ``start`` to ``end`` (exclusive) with
        step value ``step``.

        .. versionadded:: 2.0.0

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

        Examples
        --------
        >>> spark.range(1, 7, 2).show()
        +---+
        | id|
        +---+
        |  1|
        |  3|
        |  5|
        +---+

        If only one argument is specified, it will be used as the end value.

        >>> spark.range(3).show()
        +---+
        | id|
        +---+
        |  0|
        |  1|
        |  2|
        +---+
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism

        if end is None:
            jdf = self._jsparkSession.range(0, int(start), int(step), int(numPartitions))
        else:
            jdf = self._jsparkSession.range(int(start), int(end), int(step), int(numPartitions))

        return DataFrame(jdf, self)

    def _inferSchemaFromList(
        self, data: Iterable[Any], names: Optional[List[str]] = None
    ) -> StructType:
        """
        Infer schema from list of Row, dict, or tuple.

        Parameters
        ----------
        data : iterable
            list of Row, dict, or tuple
        names : list, optional
            list of column names

        Returns
        -------
        :class:`pyspark.sql.types.StructType`
        """
        if not data:
            raise ValueError("can not infer schema from empty dataset")
        infer_dict_as_struct = self._jconf.inferDictAsStruct()
        infer_array_from_first_element = self._jconf.legacyInferArrayTypeFromFirstElement()
        prefer_timestamp_ntz = is_timestamp_ntz_preferred()
        schema = reduce(
            _merge_type,
            (
                _infer_schema(
                    row,
                    names,
                    infer_dict_as_struct=infer_dict_as_struct,
                    infer_array_from_first_element=infer_array_from_first_element,
                    prefer_timestamp_ntz=prefer_timestamp_ntz,
                )
                for row in data
            ),
        )
        if _has_nulltype(schema):
            raise ValueError("Some of types cannot be determined after inferring")
        return schema

    def _inferSchema(
        self,
        rdd: RDD[Any],
        samplingRatio: Optional[float] = None,
        names: Optional[List[str]] = None,
    ) -> StructType:
        """
        Infer schema from an RDD of Row, dict, or tuple.

        Parameters
        ----------
        rdd : :class:`RDD`
            an RDD of Row, dict, or tuple
        samplingRatio : float, optional
            sampling ratio, or no sampling (default)
        names : list, optional

        Returns
        -------
        :class:`pyspark.sql.types.StructType`
        """
        first = rdd.first()
        if isinstance(first, Sized) and len(first) == 0:
            raise ValueError("The first row in RDD is empty, can not infer schema")

        infer_dict_as_struct = self._jconf.inferDictAsStruct()
        infer_array_from_first_element = self._jconf.legacyInferArrayTypeFromFirstElement()
        prefer_timestamp_ntz = is_timestamp_ntz_preferred()
        if samplingRatio is None:
            schema = _infer_schema(
                first,
                names=names,
                infer_dict_as_struct=infer_dict_as_struct,
                prefer_timestamp_ntz=prefer_timestamp_ntz,
            )
            if _has_nulltype(schema):
                for row in rdd.take(100)[1:]:
                    schema = _merge_type(
                        schema,
                        _infer_schema(
                            row,
                            names=names,
                            infer_dict_as_struct=infer_dict_as_struct,
                            infer_array_from_first_element=infer_array_from_first_element,
                            prefer_timestamp_ntz=prefer_timestamp_ntz,
                        ),
                    )
                    if not _has_nulltype(schema):
                        break
                else:
                    raise ValueError(
                        "Some of types cannot be determined by the "
                        "first 100 rows, please try again with sampling"
                    )
        else:
            if samplingRatio < 0.99:
                rdd = rdd.sample(False, float(samplingRatio))
            schema = rdd.map(
                lambda row: _infer_schema(
                    row,
                    names,
                    infer_dict_as_struct=infer_dict_as_struct,
                    infer_array_from_first_element=infer_array_from_first_element,
                    prefer_timestamp_ntz=prefer_timestamp_ntz,
                )
            ).reduce(_merge_type)
        return schema

    def _createFromRDD(
        self,
        rdd: RDD[Any],
        schema: Optional[Union[DataType, List[str]]],
        samplingRatio: Optional[float],
    ) -> Tuple[RDD[Tuple], StructType]:
        """
        Create an RDD for DataFrame from an existing RDD, returns the RDD and schema.
        """
        if schema is None or isinstance(schema, (list, tuple)):
            struct = self._inferSchema(rdd, samplingRatio, names=schema)
            converter = _create_converter(struct)
            tupled_rdd = rdd.map(converter)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name

        elif isinstance(schema, StructType):
            struct = schema
            tupled_rdd = rdd

        else:
            raise TypeError("schema should be StructType or list or None, but got: %s" % schema)

        # convert python objects to sql data
        internal_rdd = tupled_rdd.map(struct.toInternal)
        return internal_rdd, struct

    def _createFromLocal(
        self, data: Iterable[Any], schema: Optional[Union[DataType, List[str]]]
    ) -> Tuple[RDD[Tuple], StructType]:
        """
        Create an RDD for DataFrame from a list or pandas.DataFrame, returns
        the RDD and schema.
        """
        # make sure data could consumed multiple times
        if not isinstance(data, list):
            data = list(data)

        if schema is None or isinstance(schema, (list, tuple)):
            struct = self._inferSchemaFromList(data, names=schema)
            converter = _create_converter(struct)
            tupled_data: Iterable[Tuple] = map(converter, data)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name

        elif isinstance(schema, StructType):
            struct = schema
            tupled_data = data

        else:
            raise TypeError("schema should be StructType or list or None, but got: %s" % schema)

        # convert python objects to sql data
        internal_data = [struct.toInternal(row) for row in tupled_data]
        return self._sc.parallelize(internal_data), struct

    @staticmethod
    def _create_shell_session() -> "SparkSession":
        """
        Initialize a :class:`SparkSession` for a pyspark shell session. This is called from
        shell.py to make error handling simpler without needing to declare local variables in
        that script, which would expose those to users.
        """
        import py4j
        from pyspark.conf import SparkConf
        from pyspark.context import SparkContext

        try:
            # Try to access HiveConf, it will raise exception if Hive is not added
            conf = SparkConf()
            assert SparkContext._jvm is not None
            if conf.get("spark.sql.catalogImplementation", "hive").lower() == "hive":
                SparkContext._jvm.org.apache.hadoop.hive.conf.HiveConf()
                return SparkSession.builder.enableHiveSupport().getOrCreate()
            else:
                return SparkSession._getActiveSessionOrCreate()
        except (py4j.protocol.Py4JError, TypeError):
            if conf.get("spark.sql.catalogImplementation", "").lower() == "hive":
                warnings.warn(
                    "Fall back to non-hive support because failing to access HiveConf, "
                    "please make sure you build spark with hive"
                )

        return SparkSession._getActiveSessionOrCreate()

    @staticmethod
    def _getActiveSessionOrCreate(**static_conf: Any) -> "SparkSession":
        """
        Returns the active :class:`SparkSession` for the current thread, returned by the builder,
        or if there is no existing one, creates a new one based on the options set in the builder.

        NOTE that 'static_conf' might not be set if there's an active or default Spark session
        running.
        """
        spark = SparkSession.getActiveSession()
        if spark is None:
            builder = SparkSession.builder
            for k, v in static_conf.items():
                builder = builder.config(k, v)
            spark = builder.getOrCreate()
        return spark

    @overload
    def createDataFrame(
        self,
        data: Iterable["RowLike"],
        schema: Union[List[str], Tuple[str, ...]] = ...,
        samplingRatio: Optional[float] = ...,
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self,
        data: "RDD[RowLike]",
        schema: Union[List[str], Tuple[str, ...]] = ...,
        samplingRatio: Optional[float] = ...,
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self,
        data: Iterable["RowLike"],
        schema: Union[StructType, str],
        *,
        verifySchema: bool = ...,
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self,
        data: "RDD[RowLike]",
        schema: Union[StructType, str],
        *,
        verifySchema: bool = ...,
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self,
        data: "RDD[AtomicValue]",
        schema: Union[AtomicType, str],
        verifySchema: bool = ...,
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self,
        data: Iterable["AtomicValue"],
        schema: Union[AtomicType, str],
        verifySchema: bool = ...,
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self, data: "PandasDataFrameLike", samplingRatio: Optional[float] = ...
    ) -> DataFrame:
        ...

    @overload
    def createDataFrame(
        self,
        data: "PandasDataFrameLike",
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> DataFrame:
        ...

    def createDataFrame(  # type: ignore[misc]
        self,
        data: Union[RDD[Any], Iterable[Any], "PandasDataFrameLike", "ArrayLike"],
        schema: Optional[Union[AtomicType, StructType, str]] = None,
        samplingRatio: Optional[float] = None,
        verifySchema: bool = True,
    ) -> DataFrame:
        """
        Creates a :class:`DataFrame` from an :class:`RDD`, a list, a :class:`pandas.DataFrame`
        or a :class:`numpy.ndarray`.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        data : :class:`RDD` or iterable
            an RDD of any kind of SQL data representation (:class:`Row`,
            :class:`tuple`, ``int``, ``boolean``, etc.), or :class:`list`,
            :class:`pandas.DataFrame` or :class:`numpy.ndarray`.
        schema : :class:`pyspark.sql.types.DataType`, str or list, optional
            a :class:`pyspark.sql.types.DataType` or a datatype string or a list of
            column names, default is None.  The data type string format equals to
            :class:`pyspark.sql.types.DataType.simpleString`, except that top level struct type can
            omit the ``struct<>``.

            When ``schema`` is a list of column names, the type of each column
            will be inferred from ``data``.

            When ``schema`` is ``None``, it will try to infer the schema (column names and types)
            from ``data``, which should be an RDD of either :class:`Row`,
            :class:`namedtuple`, or :class:`dict`.

            When ``schema`` is :class:`pyspark.sql.types.DataType` or a datatype string, it must
            match the real data, or an exception will be thrown at runtime. If the given schema is
            not :class:`pyspark.sql.types.StructType`, it will be wrapped into a
            :class:`pyspark.sql.types.StructType` as its only field, and the field name will be
            "value". Each record will also be wrapped into a tuple, which can be converted to row
            later.
        samplingRatio : float, optional
            the sample ratio of rows used for inferring. The first few rows will be used
            if ``samplingRatio`` is ``None``.
        verifySchema : bool, optional
            verify data types of every row against schema. Enabled by default.

            .. versionadded:: 2.1.0

        Returns
        -------
        :class:`DataFrame`

        Notes
        -----
        Usage with `spark.sql.execution.arrow.pyspark.enabled=True` is experimental.

        Examples
        --------
        Create a DataFrame from a list of tuples.

        >>> spark.createDataFrame([('Alice', 1)]).collect()
        [Row(_1='Alice', _2=1)]
        >>> spark.createDataFrame([('Alice', 1)], ['name', 'age']).collect()
        [Row(name='Alice', age=1)]

        Create a DataFrame from a list of dictionaries

        >>> d = [{'name': 'Alice', 'age': 1}]
        >>> spark.createDataFrame(d).collect()
        [Row(age=1, name='Alice')]

        Create a DataFrame from an RDD.

        >>> rdd = spark.sparkContext.parallelize([('Alice', 1)])
        >>> spark.createDataFrame(rdd).collect()
        [Row(_1='Alice', _2=1)]
        >>> df = spark.createDataFrame(rdd, ['name', 'age'])
        >>> df.collect()
        [Row(name='Alice', age=1)]

        Create a DataFrame from Row instances.

        >>> from pyspark.sql import Row
        >>> Person = Row('name', 'age')
        >>> person = rdd.map(lambda r: Person(*r))
        >>> df2 = spark.createDataFrame(person)
        >>> df2.collect()
        [Row(name='Alice', age=1)]

        Create a DataFrame with the explicit schema specified.

        >>> from pyspark.sql.types import *
        >>> schema = StructType([
        ...    StructField("name", StringType(), True),
        ...    StructField("age", IntegerType(), True)])
        >>> df3 = spark.createDataFrame(rdd, schema)
        >>> df3.collect()
        [Row(name='Alice', age=1)]

        Create a DataFrame from a pandas DataFrame.

        >>> spark.createDataFrame(df.toPandas()).collect()  # doctest: +SKIP
        [Row(name='Alice', age=1)]
        >>> spark.createDataFrame(pandas.DataFrame([[1, 2]])).collect()  # doctest: +SKIP
        [Row(0=1, 1=2)]

        Create  a DataFrame from an RDD with the schema in DDL formatted string.

        >>> spark.createDataFrame(rdd, "a: string, b: int").collect()
        [Row(a='Alice', b=1)]
        >>> rdd = rdd.map(lambda row: row[1])
        >>> spark.createDataFrame(rdd, "int").collect()
        [Row(value=1)]

        When the type is unmatched, it throws an exception.

        >>> spark.createDataFrame(rdd, "boolean").collect() # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        Py4JJavaError: ...
        """
        SparkSession._activeSession = self
        assert self._jvm is not None
        self._jvm.SparkSession.setActiveSession(self._jsparkSession)
        if isinstance(data, DataFrame):
            raise TypeError("data is already a DataFrame")

        if isinstance(schema, str):
            schema = cast(Union[AtomicType, StructType, str], _parse_datatype_string(schema))
        elif isinstance(schema, (list, tuple)):
            # Must re-encode any unicode strings to be consistent with StructField names
            schema = [x.encode("utf-8") if not isinstance(x, str) else x for x in schema]

        try:
            import pandas as pd

            has_pandas = True
        except Exception:
            has_pandas = False

        try:
            import numpy as np

            has_numpy = True
        except Exception:
            has_numpy = False

        if has_numpy and isinstance(data, np.ndarray):
            # `data` of numpy.ndarray type will be converted to a pandas DataFrame,
            # so pandas is required.
            from pyspark.sql.pandas.utils import require_minimum_pandas_version

            require_minimum_pandas_version()
            if data.ndim not in [1, 2]:
                raise ValueError("NumPy array input should be of 1 or 2 dimensions.")
            column_names = ["value"] if data.ndim == 1 else ["_1", "_2"]
            if schema is None and not self._jconf.arrowPySparkEnabled():
                # Construct `schema` from `np.dtype` of the input NumPy array
                # TODO: Apply the logic below when self._jconf.arrowPySparkEnabled() is True
                spark_type = _from_numpy_type(data.dtype)
                if spark_type is not None:
                    schema = StructType(
                        [StructField(name, spark_type, nullable=True) for name in column_names]
                    )

            data = pd.DataFrame(data, columns=column_names)

        if has_pandas and isinstance(data, pd.DataFrame):
            # Create a DataFrame from pandas DataFrame.
            return super(SparkSession, self).createDataFrame(  # type: ignore[call-overload]
                data, schema, samplingRatio, verifySchema
            )
        return self._create_dataframe(
            data, schema, samplingRatio, verifySchema  # type: ignore[arg-type]
        )

    def _create_dataframe(
        self,
        data: Union[RDD[Any], Iterable[Any]],
        schema: Optional[Union[DataType, List[str]]],
        samplingRatio: Optional[float],
        verifySchema: bool,
    ) -> DataFrame:
        if isinstance(schema, StructType):
            verify_func = _make_type_verifier(schema) if verifySchema else lambda _: True

            @no_type_check
            def prepare(obj):
                verify_func(obj)
                return obj

        elif isinstance(schema, DataType):
            dataType = schema
            schema = StructType().add("value", schema)

            verify_func = (
                _make_type_verifier(dataType, name="field value")
                if verifySchema
                else lambda _: True
            )

            @no_type_check
            def prepare(obj):
                verify_func(obj)
                return (obj,)

        else:

            def prepare(obj: Any) -> Any:
                return obj

        if isinstance(data, RDD):
            rdd, struct = self._createFromRDD(data.map(prepare), schema, samplingRatio)
        else:
            rdd, struct = self._createFromLocal(map(prepare, data), schema)
        assert self._jvm is not None
        jrdd = self._jvm.SerDeUtil.toJavaArray(rdd._to_java_object_rdd())
        jdf = self._jsparkSession.applySchemaToPythonRDD(jrdd.rdd(), struct.json())
        df = DataFrame(jdf, self)
        df._schema = struct
        return df

    def sql(self, sqlQuery: str, **kwargs: Any) -> DataFrame:
        """Returns a :class:`DataFrame` representing the result of the given query.
        When ``kwargs`` is specified, this method formats the given string by using the Python
        standard formatter.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        sqlQuery : str
            SQL query string.
        kwargs : dict
            Other variables that the user wants to set that can be referenced in the query

            .. versionchanged:: 3.3.0
               Added optional argument ``kwargs`` to specify the mapping of variables in the query.
               This feature is experimental and unstable.

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        Executing a SQL query.

        >>> spark.sql("SELECT * FROM range(10) where id > 7").show()
        +---+
        | id|
        +---+
        |  8|
        |  9|
        +---+

        Executing a SQL query with variables as Python formatter standard.

        >>> spark.sql(
        ...     "SELECT * FROM range(10) WHERE id > {bound1} AND id < {bound2}", bound1=7, bound2=9
        ... ).show()
        +---+
        | id|
        +---+
        |  8|
        +---+

        >>> mydf = spark.range(10)
        >>> spark.sql(
        ...     "SELECT {col} FROM {mydf} WHERE id IN {x}",
        ...     col=mydf.id, mydf=mydf, x=tuple(range(4))).show()
        +---+
        | id|
        +---+
        |  0|
        |  1|
        |  2|
        |  3|
        +---+

        >>> spark.sql('''
        ...   SELECT m1.a, m2.b
        ...   FROM {table1} m1 INNER JOIN {table2} m2
        ...   ON m1.key = m2.key
        ...   ORDER BY m1.a, m2.b''',
        ...   table1=spark.createDataFrame([(1, "a"), (2, "b")], ["a", "key"]),
        ...   table2=spark.createDataFrame([(3, "a"), (4, "b"), (5, "b")], ["b", "key"])).show()
        +---+---+
        |  a|  b|
        +---+---+
        |  1|  3|
        |  2|  4|
        |  2|  5|
        +---+---+

        Also, it is possible to query using class:`Column` from :class:`DataFrame`.

        >>> mydf = spark.createDataFrame([(1, 4), (2, 4), (3, 6)], ["A", "B"])
        >>> spark.sql("SELECT {df.A}, {df[B]} FROM {df}", df=mydf).show()
        +---+---+
        |  A|  B|
        +---+---+
        |  1|  4|
        |  2|  4|
        |  3|  6|
        +---+---+
        """

        formatter = SQLStringFormatter(self)
        if len(kwargs) > 0:
            sqlQuery = formatter.format(sqlQuery, **kwargs)
        try:
            return DataFrame(self._jsparkSession.sql(sqlQuery), self)
        finally:
            if len(kwargs) > 0:
                formatter.clear()

    def table(self, tableName: str) -> DataFrame:
        """Returns the specified table as a :class:`DataFrame`.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            the table name to retrieve.

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> spark.range(5).createOrReplaceTempView("table1")
        >>> spark.table("table1").sort("id").show()
        +---+
        | id|
        +---+
        |  0|
        |  1|
        |  2|
        |  3|
        |  4|
        +---+
        """
        return DataFrame(self._jsparkSession.table(tableName), self)

    @property
    def read(self) -> DataFrameReader:
        """
        Returns a :class:`DataFrameReader` that can be used to read data
        in as a :class:`DataFrame`.

        .. versionadded:: 2.0.0

        Returns
        -------
        :class:`DataFrameReader`

        Examples
        --------
        >>> spark.read
        <pyspark.sql.readwriter.DataFrameReader object ...>

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

    @property
    def readStream(self) -> DataStreamReader:
        """
        Returns a :class:`DataStreamReader` that can be used to read data streams
        as a streaming :class:`DataFrame`.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.

        Returns
        -------
        :class:`DataStreamReader`

        Examples
        --------
        >>> spark.readStream
        <pyspark.sql.streaming.readwriter.DataStreamReader object ...>

        The example below uses Rate source that generates rows continously.
        After that, we operate a modulo by 3, and then write the stream out to the console.
        The streaming query stops in 3 seconds.

        >>> import time
        >>> df = spark.readStream.format("rate").load()
        >>> df = df.selectExpr("value % 3 as v")
        >>> q = df.writeStream.format("console").start()
        >>> time.sleep(3)
        >>> q.stop()
        """
        return DataStreamReader(self)

    @property
    def streams(self) -> "StreamingQueryManager":
        """Returns a :class:`StreamingQueryManager` that allows managing all the
        :class:`StreamingQuery` instances active on `this` context.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.

        Returns
        -------
        :class:`StreamingQueryManager`

        Examples
        --------
        >>> spark.streams
        <pyspark.sql.streaming.query.StreamingQueryManager object ...>

        Get the list of active streaming queries

        >>> sq = spark.readStream.format(
        ...     "rate").load().writeStream.format('memory').queryName('this_query').start()
        >>> sqm = spark.streams
        >>> [q.name for q in sqm.active]
        ['this_query']
        >>> sq.stop()
        """
        from pyspark.sql.streaming import StreamingQueryManager

        return StreamingQueryManager(self._jsparkSession.streams())

    def stop(self) -> None:
        """
        Stop the underlying :class:`SparkContext`.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.stop()  # doctest: +SKIP
        """
        from pyspark.sql.context import SQLContext

        self._sc.stop()
        # We should clean the default session up. See SPARK-23228.
        assert self._jvm is not None
        self._jvm.SparkSession.clearDefaultSession()
        self._jvm.SparkSession.clearActiveSession()
        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None
        SQLContext._instantiatedContext = None

    def __enter__(self) -> "SparkSession":
        """
        Enable 'with SparkSession.builder.(...).getOrCreate() as session: app' syntax.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> with SparkSession.builder.master("local").getOrCreate() as session:
        ...     session.range(5).show()  # doctest: +SKIP
        +---+
        | id|
        +---+
        |  0|
        |  1|
        |  2|
        |  3|
        |  4|
        +---+
        """
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """
        Enable 'with SparkSession.builder.(...).getOrCreate() as session: app' syntax.

        Specifically stop the SparkSession on exit of the with block.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> with SparkSession.builder.master("local").getOrCreate() as session:
        ...     session.range(5).show()  # doctest: +SKIP
        +---+
        | id|
        +---+
        |  0|
        |  1|
        |  2|
        |  3|
        |  4|
        +---+
        """
        self.stop()


def _test() -> None:
    import os
    import doctest
    import pyspark.sql.session

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.session.__dict__.copy()
    globs["spark"] = (
        SparkSession.builder.master("local[4]").appName("sql.session tests").getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.session,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    globs["spark"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
