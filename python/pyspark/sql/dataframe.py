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
import sys
import random
import warnings
from collections.abc import Iterable
from functools import reduce
from html import escape as html_escape
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    overload,
    TYPE_CHECKING,
)

from py4j.java_gateway import JavaObject  # type: ignore[import]

from pyspark import copy_func, since, _NoValue  # type: ignore[attr-defined]
from pyspark.context import SparkContext
from pyspark.rdd import (  # type: ignore[attr-defined]
    RDD,
    _load_from_socket,
    _local_iterator_from_socket,
)
from pyspark.serializers import BatchedSerializer, PickleSerializer, UTF8Deserializer
from pyspark.storagelevel import StorageLevel
from pyspark.traceback_utils import SCCallSiteSync
from pyspark.sql.column import Column, _to_seq, _to_list, _to_java_column
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    Row,
    _parse_datatype_json_string,
)
from pyspark.sql.pandas.conversion import PandasConversionMixin
from pyspark.sql.pandas.map_ops import PandasMapOpsMixin

if TYPE_CHECKING:
    from pyspark._typing import PrimitiveType
    from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
    from pyspark.sql._typing import ColumnOrName, LiteralType, OptionalPrimitiveType
    from pyspark.sql.context import SQLContext
    from pyspark.sql.group import GroupedData
    from pyspark.sql.observation import Observation


__all__ = ["DataFrame", "DataFrameNaFunctions", "DataFrameStatFunctions"]


class DataFrame(PandasMapOpsMixin, PandasConversionMixin):
    """A distributed collection of data grouped into named columns.

    A :class:`DataFrame` is equivalent to a relational table in Spark SQL,
    and can be created using various functions in :class:`SparkSession`::

        people = spark.read.parquet("...")

    Once created, it can be manipulated using the various domain-specific-language
    (DSL) functions defined in: :class:`DataFrame`, :class:`Column`.

    To select a column from the :class:`DataFrame`, use the apply method::

        ageCol = people.age

    A more concrete example::

        # To create DataFrame using SparkSession
        people = spark.read.parquet("...")
        department = spark.read.parquet("...")

        people.filter(people.age > 30).join(department, people.deptId == department.id) \\
          .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})

    .. versionadded:: 1.3.0
    """

    def __init__(self, jdf: JavaObject, sql_ctx: "SQLContext"):
        self._jdf = jdf
        self.sql_ctx = sql_ctx
        self._sc: SparkContext = cast(SparkContext, sql_ctx and sql_ctx._sc)
        self.is_cached = False
        # initialized lazily
        self._schema: Optional[StructType] = None
        self._lazy_rdd: Optional[RDD[Row]] = None
        # Check whether _repr_html is supported or not, we use it to avoid calling _jdf twice
        # by __repr__ and _repr_html_ while eager evaluation opened.
        self._support_repr_html = False

    @property  # type: ignore[misc]
    @since(1.3)
    def rdd(self) -> "RDD[Row]":
        """Returns the content as an :class:`pyspark.RDD` of :class:`Row`."""
        if self._lazy_rdd is None:
            jrdd = self._jdf.javaToPython()
            self._lazy_rdd = RDD(jrdd, self.sql_ctx._sc, BatchedSerializer(PickleSerializer()))
        return self._lazy_rdd

    @property  # type: ignore[misc]
    @since("1.3.1")
    def na(self) -> "DataFrameNaFunctions":
        """Returns a :class:`DataFrameNaFunctions` for handling missing values."""
        return DataFrameNaFunctions(self)

    @property  # type: ignore[misc]
    @since(1.4)
    def stat(self) -> "DataFrameStatFunctions":
        """Returns a :class:`DataFrameStatFunctions` for statistic functions."""
        return DataFrameStatFunctions(self)

    def toJSON(self, use_unicode: bool = True) -> "RDD[str]":
        """Converts a :class:`DataFrame` into a :class:`RDD` of string.

        Each row is turned into a JSON document as one element in the returned RDD.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.toJSON().first()
        '{"age":2,"name":"Alice"}'
        """
        rdd = self._jdf.toJSON()
        return RDD(rdd.toJavaRDD(), self._sc, UTF8Deserializer(use_unicode))

    def registerTempTable(self, name: str) -> None:
        """Registers this :class:`DataFrame` as a temporary table using the given name.

        The lifetime of this temporary table is tied to the :class:`SparkSession`
        that was used to create this :class:`DataFrame`.

        .. versionadded:: 1.3.0

        .. deprecated:: 2.0.0
            Use :meth:`DataFrame.createOrReplaceTempView` instead.

        Examples
        --------
        >>> df.registerTempTable("people")
        >>> df2 = spark.sql("select * from people")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        >>> spark.catalog.dropTempView("people")
        True

        """
        warnings.warn("Deprecated in 2.0, use createOrReplaceTempView instead.", FutureWarning)
        self._jdf.createOrReplaceTempView(name)

    def createTempView(self, name: str) -> None:
        """Creates a local temporary view with this :class:`DataFrame`.

        The lifetime of this temporary table is tied to the :class:`SparkSession`
        that was used to create this :class:`DataFrame`.
        throws :class:`TempTableAlreadyExistsException`, if the view name already exists in the
        catalog.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> df.createTempView("people")
        >>> df2 = spark.sql("select * from people")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        >>> df.createTempView("people")  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        AnalysisException: u"Temporary table 'people' already exists;"
        >>> spark.catalog.dropTempView("people")
        True

        """
        self._jdf.createTempView(name)

    def createOrReplaceTempView(self, name: str) -> None:
        """Creates or replaces a local temporary view with this :class:`DataFrame`.

        The lifetime of this temporary table is tied to the :class:`SparkSession`
        that was used to create this :class:`DataFrame`.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> df.createOrReplaceTempView("people")
        >>> df2 = df.filter(df.age > 3)
        >>> df2.createOrReplaceTempView("people")
        >>> df3 = spark.sql("select * from people")
        >>> sorted(df3.collect()) == sorted(df2.collect())
        True
        >>> spark.catalog.dropTempView("people")
        True

        """
        self._jdf.createOrReplaceTempView(name)

    def createGlobalTempView(self, name: str) -> None:
        """Creates a global temporary view with this :class:`DataFrame`.

        The lifetime of this temporary view is tied to this Spark application.
        throws :class:`TempTableAlreadyExistsException`, if the view name already exists in the
        catalog.

        .. versionadded:: 2.1.0

        Examples
        --------
        >>> df.createGlobalTempView("people")
        >>> df2 = spark.sql("select * from global_temp.people")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        >>> df.createGlobalTempView("people")  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        AnalysisException: u"Temporary table 'people' already exists;"
        >>> spark.catalog.dropGlobalTempView("people")
        True

        """
        self._jdf.createGlobalTempView(name)

    def createOrReplaceGlobalTempView(self, name: str) -> None:
        """Creates or replaces a global temporary view using the given name.

        The lifetime of this temporary view is tied to this Spark application.

        .. versionadded:: 2.2.0

        Examples
        --------
        >>> df.createOrReplaceGlobalTempView("people")
        >>> df2 = df.filter(df.age > 3)
        >>> df2.createOrReplaceGlobalTempView("people")
        >>> df3 = spark.sql("select * from global_temp.people")
        >>> sorted(df3.collect()) == sorted(df2.collect())
        True
        >>> spark.catalog.dropGlobalTempView("people")
        True

        """
        self._jdf.createOrReplaceGlobalTempView(name)

    @property
    def write(self) -> DataFrameWriter:
        """
        Interface for saving the content of the non-streaming :class:`DataFrame` out into external
        storage.

        .. versionadded:: 1.4.0

        Returns
        -------
        :class:`DataFrameWriter`
        """
        return DataFrameWriter(self)

    @property
    def writeStream(self) -> DataStreamWriter:
        """
        Interface for saving the content of the streaming :class:`DataFrame` out into external
        storage.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.

        Returns
        -------
        :class:`DataStreamWriter`
        """
        return DataStreamWriter(self)

    @property
    def schema(self) -> StructType:
        """Returns the schema of this :class:`DataFrame` as a :class:`pyspark.sql.types.StructType`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.schema
        StructType(List(StructField(age,IntegerType,true),StructField(name,StringType,true)))
        """
        if self._schema is None:
            try:
                self._schema = cast(
                    StructType, _parse_datatype_json_string(self._jdf.schema().json())
                )
            except Exception as e:
                raise ValueError("Unable to parse datatype from schema. %s" % e) from e
        return self._schema

    def printSchema(self) -> None:
        """Prints out the schema in the tree format.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.printSchema()
        root
         |-- age: integer (nullable = true)
         |-- name: string (nullable = true)
        <BLANKLINE>
        """
        print(self._jdf.schema().treeString())

    def explain(
        self, extended: Optional[Union[bool, str]] = None, mode: Optional[str] = None
    ) -> None:
        """Prints the (logical and physical) plans to the console for debugging purpose.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        extended : bool, optional
            default ``False``. If ``False``, prints only the physical plan.
            When this is a string without specifying the ``mode``, it works as the mode is
            specified.
        mode : str, optional
            specifies the expected output format of plans.

            * ``simple``: Print only a physical plan.
            * ``extended``: Print both logical and physical plans.
            * ``codegen``: Print a physical plan and generated codes if they are available.
            * ``cost``: Print a logical plan and statistics if they are available.
            * ``formatted``: Split explain output into two sections: a physical plan outline \
                and node details.

            .. versionchanged:: 3.0.0
               Added optional argument `mode` to specify the expected output format of plans.

        Examples
        --------
        >>> df.explain()
        == Physical Plan ==
        *(1) Scan ExistingRDD[age#0,name#1]

        >>> df.explain(True)
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...

        >>> df.explain(mode="formatted")
        == Physical Plan ==
        * Scan ExistingRDD (1)
        (1) Scan ExistingRDD [codegen id : 1]
        Output [2]: [age#0, name#1]
        ...

        >>> df.explain("cost")
        == Optimized Logical Plan ==
        ...Statistics...
        ...
        """

        if extended is not None and mode is not None:
            raise ValueError("extended and mode should not be set together.")

        # For the no argument case: df.explain()
        is_no_argument = extended is None and mode is None

        # For the cases below:
        #   explain(True)
        #   explain(extended=False)
        is_extended_case = isinstance(extended, bool) and mode is None

        # For the case when extended is mode:
        #   df.explain("formatted")
        is_extended_as_mode = isinstance(extended, str) and mode is None

        # For the mode specified:
        #   df.explain(mode="formatted")
        is_mode_case = extended is None and isinstance(mode, str)

        if not (is_no_argument or is_extended_case or is_extended_as_mode or is_mode_case):
            argtypes = [str(type(arg)) for arg in [extended, mode] if arg is not None]
            raise TypeError(
                "extended (optional) and mode (optional) should be a string "
                "and bool; however, got [%s]." % ", ".join(argtypes)
            )

        # Sets an explain mode depending on a given argument
        if is_no_argument:
            explain_mode = "simple"
        elif is_extended_case:
            explain_mode = "extended" if extended else "simple"
        elif is_mode_case:
            explain_mode = cast(str, mode)
        elif is_extended_as_mode:
            explain_mode = cast(str, extended)

        print(
            self._sc._jvm.PythonSQLUtils.explainString(  # type: ignore[attr-defined]
                self._jdf.queryExecution(), explain_mode
            )
        )

    def exceptAll(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows in this :class:`DataFrame` but
        not in another :class:`DataFrame` while preserving duplicates.

        This is equivalent to `EXCEPT ALL` in SQL.
        As standard in SQL, this function resolves columns by position (not by name).

        .. versionadded:: 2.4.0

        Examples
        --------
        >>> df1 = spark.createDataFrame(
        ...         [("a", 1), ("a", 1), ("a", 1), ("a", 2), ("b",  3), ("c", 4)], ["C1", "C2"])
        >>> df2 = spark.createDataFrame([("a", 1), ("b", 3)], ["C1", "C2"])

        >>> df1.exceptAll(df2).show()
        +---+---+
        | C1| C2|
        +---+---+
        |  a|  1|
        |  a|  1|
        |  a|  2|
        |  c|  4|
        +---+---+

        """
        return DataFrame(self._jdf.exceptAll(other._jdf), self.sql_ctx)

    @since(1.3)
    def isLocal(self) -> bool:
        """Returns ``True`` if the :func:`collect` and :func:`take` methods can be run locally
        (without any Spark executors).
        """
        return self._jdf.isLocal()

    @property
    def isStreaming(self) -> bool:
        """Returns ``True`` if this :class:`DataFrame` contains one or more sources that
        continuously return data as it arrives. A :class:`DataFrame` that reads data from a
        streaming source must be executed as a :class:`StreamingQuery` using the :func:`start`
        method in :class:`DataStreamWriter`.  Methods that return a single answer, (e.g.,
        :func:`count` or :func:`collect`) will throw an :class:`AnalysisException` when there
        is a streaming source present.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.
        """
        return self._jdf.isStreaming()

    def isEmpty(self) -> bool:
        """Returns ``True`` if this :class:`DataFrame` is empty.

        .. versionadded:: 3.3.0

        Examples
        --------
        >>> df_empty = spark.createDataFrame([], 'a STRING')
        >>> df_non_empty = spark.createDataFrame([("a")], 'STRING')
        >>> df_empty.isEmpty()
        True
        >>> df_non_empty.isEmpty()
        False
        """
        return self._jdf.isEmpty()

    def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:
        """Prints the first ``n`` rows to the console.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        n : int, optional
            Number of rows to show.
        truncate : bool or int, optional
            If set to ``True``, truncate strings longer than 20 chars by default.
            If set to a number greater than one, truncates long strings to length ``truncate``
            and align cells right.
        vertical : bool, optional
            If set to ``True``, print output rows vertically (one line
            per column value).

        Examples
        --------
        >>> df
        DataFrame[age: int, name: string]
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        >>> df.show(truncate=3)
        +---+----+
        |age|name|
        +---+----+
        |  2| Ali|
        |  5| Bob|
        +---+----+
        >>> df.show(vertical=True)
        -RECORD 0-----
         age  | 2
         name | Alice
        -RECORD 1-----
         age  | 5
         name | Bob
        """

        if not isinstance(n, int) or isinstance(n, bool):
            raise TypeError("Parameter 'n' (number of rows) must be an int")

        if not isinstance(vertical, bool):
            raise TypeError("Parameter 'vertical' must be a bool")

        if isinstance(truncate, bool) and truncate:
            print(self._jdf.showString(n, 20, vertical))
        else:
            try:
                int_truncate = int(truncate)
            except ValueError:
                raise TypeError(
                    "Parameter 'truncate={}' should be either bool or int.".format(truncate)
                )

            print(self._jdf.showString(n, int_truncate, vertical))

    def __repr__(self) -> str:
        if (
            not self._support_repr_html
            and self.sql_ctx._conf.isReplEagerEvalEnabled()  # type: ignore[attr-defined]
        ):
            vertical = False
            return self._jdf.showString(
                self.sql_ctx._conf.replEagerEvalMaxNumRows(),  # type: ignore[attr-defined]
                self.sql_ctx._conf.replEagerEvalTruncate(),  # type: ignore[attr-defined]
                vertical,
            )  # type: ignore[attr-defined]
        else:
            return "DataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    def _repr_html_(self) -> Optional[str]:
        """Returns a :class:`DataFrame` with html code when you enabled eager evaluation
        by 'spark.sql.repl.eagerEval.enabled', this only called by REPL you are
        using support eager evaluation with HTML.
        """
        if not self._support_repr_html:
            self._support_repr_html = True
        if self.sql_ctx._conf.isReplEagerEvalEnabled():  # type: ignore[attr-defined]
            max_num_rows = max(
                self.sql_ctx._conf.replEagerEvalMaxNumRows(), 0  # type: ignore[attr-defined]
            )
            sock_info = self._jdf.getRowsToPython(
                max_num_rows,
                self.sql_ctx._conf.replEagerEvalTruncate(),  # type: ignore[attr-defined]
            )
            rows = list(_load_from_socket(sock_info, BatchedSerializer(PickleSerializer())))
            head = rows[0]
            row_data = rows[1:]
            has_more_data = len(row_data) > max_num_rows
            row_data = row_data[:max_num_rows]

            html = "<table border='1'>\n"
            # generate table head
            html += "<tr><th>%s</th></tr>\n" % "</th><th>".join(map(lambda x: html_escape(x), head))
            # generate table rows
            for row in row_data:
                html += "<tr><td>%s</td></tr>\n" % "</td><td>".join(
                    map(lambda x: html_escape(x), row)
                )
            html += "</table>\n"
            if has_more_data:
                html += "only showing top %d %s\n" % (
                    max_num_rows,
                    "row" if max_num_rows == 1 else "rows",
                )
            return html
        else:
            return None

    def checkpoint(self, eager: bool = True) -> "DataFrame":
        """Returns a checkpointed version of this :class:`DataFrame`. Checkpointing can be used to
        truncate the logical plan of this :class:`DataFrame`, which is especially useful in
        iterative algorithms where the plan may grow exponentially. It will be saved to files
        inside the checkpoint directory set with :meth:`SparkContext.setCheckpointDir`.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        eager : bool, optional
            Whether to checkpoint this :class:`DataFrame` immediately

        Notes
        -----
        This API is experimental.
        """
        jdf = self._jdf.checkpoint(eager)
        return DataFrame(jdf, self.sql_ctx)

    def localCheckpoint(self, eager: bool = True) -> "DataFrame":
        """Returns a locally checkpointed version of this :class:`DataFrame`. Checkpointing can be
        used to truncate the logical plan of this :class:`DataFrame`, which is especially useful in
        iterative algorithms where the plan may grow exponentially. Local checkpoints are
        stored in the executors using the caching subsystem and therefore they are not reliable.

        .. versionadded:: 2.3.0

        Parameters
        ----------
        eager : bool, optional
            Whether to checkpoint this :class:`DataFrame` immediately

        Notes
        -----
        This API is experimental.
        """
        jdf = self._jdf.localCheckpoint(eager)
        return DataFrame(jdf, self.sql_ctx)

    def withWatermark(self, eventTime: str, delayThreshold: str) -> "DataFrame":
        """Defines an event time watermark for this :class:`DataFrame`. A watermark tracks a point
        in time before which we assume no more late data is going to arrive.

        Spark will use this watermark for several purposes:
          - To know when a given time window aggregation can be finalized and thus can be emitted
            when using output modes that do not allow updates.

          - To minimize the amount of state that we need to keep for on-going aggregations.

        The current watermark is computed by looking at the `MAX(eventTime)` seen across
        all of the partitions in the query minus a user specified `delayThreshold`.  Due to the cost
        of coordinating this value across partitions, the actual watermark used is only guaranteed
        to be at least `delayThreshold` behind the actual event time.  In some cases we may still
        process records that arrive more than `delayThreshold` late.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        eventTime : str
            the name of the column that contains the event time of the row.
        delayThreshold : str
            the minimum delay to wait to data to arrive late, relative to the
            latest record that has been processed in the form of an interval
            (e.g. "1 minute" or "5 hours").

        Notes
        -----
        This API is evolving.

        >>> from pyspark.sql.functions import timestamp_seconds
        >>> sdf.select(
        ...    'name',
        ...    timestamp_seconds(sdf.time).alias('time')).withWatermark('time', '10 minutes')
        DataFrame[name: string, time: timestamp]
        """
        if not eventTime or type(eventTime) is not str:
            raise TypeError("eventTime should be provided as a string")
        if not delayThreshold or type(delayThreshold) is not str:
            raise TypeError("delayThreshold should be provided as a string interval")
        jdf = self._jdf.withWatermark(eventTime, delayThreshold)
        return DataFrame(jdf, self.sql_ctx)

    def hint(
        self, name: str, *parameters: Union["PrimitiveType", List["PrimitiveType"]]
    ) -> "DataFrame":
        """Specifies some hint on the current :class:`DataFrame`.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        name : str
            A name of the hint.
        parameters : str, list, float or int
            Optional parameters.

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> df.join(df2.hint("broadcast"), "name").show()
        +----+---+------+
        |name|age|height|
        +----+---+------+
        | Bob|  5|    85|
        +----+---+------+
        """
        if len(parameters) == 1 and isinstance(parameters[0], list):
            parameters = parameters[0]  # type: ignore[assignment]

        if not isinstance(name, str):
            raise TypeError("name should be provided as str, got {0}".format(type(name)))

        allowed_types = (str, list, float, int)
        for p in parameters:
            if not isinstance(p, allowed_types):
                raise TypeError(
                    "all parameters should be in {0}, got {1} of type {2}".format(
                        allowed_types, p, type(p)
                    )
                )

        jdf = self._jdf.hint(name, self._jseq(parameters))
        return DataFrame(jdf, self.sql_ctx)

    def count(self) -> int:
        """Returns the number of rows in this :class:`DataFrame`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.count()
        2
        """
        return int(self._jdf.count())

    def collect(self) -> List[Row]:
        """Returns all the records as a list of :class:`Row`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.collect()
        [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        """
        with SCCallSiteSync(self._sc) as css:
            sock_info = self._jdf.collectToPython()
        return list(_load_from_socket(sock_info, BatchedSerializer(PickleSerializer())))

    def toLocalIterator(self, prefetchPartitions: bool = False) -> Iterator[Row]:
        """
        Returns an iterator that contains all of the rows in this :class:`DataFrame`.
        The iterator will consume as much memory as the largest partition in this
        :class:`DataFrame`. With prefetch it may consume up to the memory of the 2 largest
        partitions.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        prefetchPartitions : bool, optional
            If Spark should pre-fetch the next partition  before it is needed.

        Examples
        --------
        >>> list(df.toLocalIterator())
        [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        """
        with SCCallSiteSync(self._sc) as css:
            sock_info = self._jdf.toPythonIterator(prefetchPartitions)
        return _local_iterator_from_socket(sock_info, BatchedSerializer(PickleSerializer()))

    def limit(self, num: int) -> "DataFrame":
        """Limits the result count to the number specified.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.limit(1).collect()
        [Row(age=2, name='Alice')]
        >>> df.limit(0).collect()
        []
        """
        jdf = self._jdf.limit(num)
        return DataFrame(jdf, self.sql_ctx)

    def take(self, num: int) -> List[Row]:
        """Returns the first ``num`` rows as a :class:`list` of :class:`Row`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.take(2)
        [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        """
        return self.limit(num).collect()

    def tail(self, num: int) -> List[Row]:
        """
        Returns the last ``num`` rows as a :class:`list` of :class:`Row`.

        Running tail requires moving data into the application's driver process, and doing so with
        a very large ``num`` can crash the driver process with OutOfMemoryError.

        .. versionadded:: 3.0.0

        Examples
        --------
        >>> df.tail(1)
        [Row(age=5, name='Bob')]
        """
        with SCCallSiteSync(self._sc):
            sock_info = self._jdf.tailToPython(num)
        return list(_load_from_socket(sock_info, BatchedSerializer(PickleSerializer())))

    def foreach(self, f: Callable[[Row], None]) -> None:
        """Applies the ``f`` function to all :class:`Row` of this :class:`DataFrame`.

        This is a shorthand for ``df.rdd.foreach()``.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> def f(person):
        ...     print(person.name)
        >>> df.foreach(f)
        """
        self.rdd.foreach(f)

    def foreachPartition(self, f: Callable[[Iterator[Row]], None]) -> None:
        """Applies the ``f`` function to each partition of this :class:`DataFrame`.

        This a shorthand for ``df.rdd.foreachPartition()``.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> def f(people):
        ...     for person in people:
        ...         print(person.name)
        >>> df.foreachPartition(f)
        """
        self.rdd.foreachPartition(f)  # type: ignore[arg-type]

    def cache(self) -> "DataFrame":
        """Persists the :class:`DataFrame` with the default storage level (`MEMORY_AND_DISK`).

        .. versionadded:: 1.3.0

        Notes
        -----
        The default storage level has changed to `MEMORY_AND_DISK` to match Scala in 2.0.
        """
        self.is_cached = True
        self._jdf.cache()
        return self

    def persist(
        self,
        storageLevel: StorageLevel = (
            StorageLevel.MEMORY_AND_DISK_DESER  # type: ignore[attr-defined]
        ),
    ) -> "DataFrame":
        """Sets the storage level to persist the contents of the :class:`DataFrame` across
        operations after the first time it is computed. This can only be used to assign
        a new storage level if the :class:`DataFrame` does not have a storage level set yet.
        If no storage level is specified defaults to (`MEMORY_AND_DISK_DESER`)

        .. versionadded:: 1.3.0

        Notes
        -----
        The default storage level has changed to `MEMORY_AND_DISK_DESER` to match Scala in 3.0.
        """
        self.is_cached = True
        javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)  # type: ignore[attr-defined]
        self._jdf.persist(javaStorageLevel)
        return self

    @property
    def storageLevel(self) -> StorageLevel:
        """Get the :class:`DataFrame`'s current storage level.

        .. versionadded:: 2.1.0

        Examples
        --------
        >>> df.storageLevel
        StorageLevel(False, False, False, False, 1)
        >>> df.cache().storageLevel
        StorageLevel(True, True, False, True, 1)
        >>> df2.persist(StorageLevel.DISK_ONLY_2).storageLevel
        StorageLevel(True, False, False, False, 2)
        """
        java_storage_level = self._jdf.storageLevel()
        storage_level = StorageLevel(
            java_storage_level.useDisk(),
            java_storage_level.useMemory(),
            java_storage_level.useOffHeap(),
            java_storage_level.deserialized(),
            java_storage_level.replication(),
        )
        return storage_level

    def unpersist(self, blocking: bool = False) -> "DataFrame":
        """Marks the :class:`DataFrame` as non-persistent, and remove all blocks for it from
        memory and disk.

        .. versionadded:: 1.3.0

        Notes
        -----
        `blocking` default has changed to ``False`` to match Scala in 2.0.
        """
        self.is_cached = False
        self._jdf.unpersist(blocking)
        return self

    def coalesce(self, numPartitions: int) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` that has exactly `numPartitions` partitions.

        Similar to coalesce defined on an :class:`RDD`, this operation results in a
        narrow dependency, e.g. if you go from 1000 partitions to 100 partitions,
        there will not be a shuffle, instead each of the 100 new partitions will
        claim 10 of the current partitions. If a larger number of partitions is requested,
        it will stay at the current number of partitions.

        However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
        this may result in your computation taking place on fewer nodes than
        you like (e.g. one node in the case of numPartitions = 1). To avoid this,
        you can call repartition(). This will add a shuffle step, but means the
        current upstream partitions will be executed in parallel (per whatever
        the current partitioning is).

        .. versionadded:: 1.4.0

        Parameters
        ----------
        numPartitions : int
            specify the target number of partitions

        Examples
        --------
        >>> df.coalesce(1).rdd.getNumPartitions()
        1
        """
        return DataFrame(self._jdf.coalesce(numPartitions), self.sql_ctx)

    @overload
    def repartition(self, numPartitions: int, *cols: "ColumnOrName") -> "DataFrame":
        ...

    @overload
    def repartition(self, *cols: "ColumnOrName") -> "DataFrame":
        ...

    def repartition(  # type: ignore[misc]
        self, numPartitions: Union[int, "ColumnOrName"], *cols: "ColumnOrName"
    ) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` partitioned by the given partitioning expressions. The
        resulting :class:`DataFrame` is hash partitioned.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        numPartitions : int
            can be an int to specify the target number of partitions or a Column.
            If it is a Column, it will be used as the first partitioning column. If not specified,
            the default number of partitions is used.
        cols : str or :class:`Column`
            partitioning columns.

            .. versionchanged:: 1.6
               Added optional arguments to specify the partitioning columns. Also made numPartitions
               optional if partitioning columns are specified.

        Examples
        --------
        >>> df.repartition(10).rdd.getNumPartitions()
        10
        >>> data = df.union(df).repartition("age")
        >>> data.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        >>> data = data.repartition(7, "age")
        >>> data.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        >>> data.rdd.getNumPartitions()
        7
        >>> data = data.repartition(3, "name", "age")
        >>> data.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  5|  Bob|
        |  2|Alice|
        |  2|Alice|
        +---+-----+
        """
        if isinstance(numPartitions, int):
            if len(cols) == 0:
                return DataFrame(self._jdf.repartition(numPartitions), self.sql_ctx)
            else:
                return DataFrame(
                    self._jdf.repartition(numPartitions, self._jcols(*cols)), self.sql_ctx
                )
        elif isinstance(numPartitions, (str, Column)):
            cols = (numPartitions,) + cols
            return DataFrame(self._jdf.repartition(self._jcols(*cols)), self.sql_ctx)
        else:
            raise TypeError("numPartitions should be an int or Column")

    @overload
    def repartitionByRange(self, numPartitions: int, *cols: "ColumnOrName") -> "DataFrame":
        ...

    @overload
    def repartitionByRange(self, *cols: "ColumnOrName") -> "DataFrame":
        ...

    def repartitionByRange(  # type: ignore[misc]
        self, numPartitions: Union[int, "ColumnOrName"], *cols: "ColumnOrName"
    ) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` partitioned by the given partitioning expressions. The
        resulting :class:`DataFrame` is range partitioned.

        At least one partition-by expression must be specified.
        When no explicit sort order is specified, "ascending nulls first" is assumed.

        .. versionadded:: 2.4.0

        Parameters
        ----------
        numPartitions : int
            can be an int to specify the target number of partitions or a Column.
            If it is a Column, it will be used as the first partitioning column. If not specified,
            the default number of partitions is used.
        cols : str or :class:`Column`
            partitioning columns.

        Notes
        -----
        Due to performance reasons this method uses sampling to estimate the ranges.
        Hence, the output may not be consistent, since sampling can return different values.
        The sample size can be controlled by the config
        `spark.sql.execution.rangeExchange.sampleSizePerPartition`.

        Examples
        --------
        >>> df.repartitionByRange(2, "age").rdd.getNumPartitions()
        2
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        >>> df.repartitionByRange(1, "age").rdd.getNumPartitions()
        1
        >>> data = df.repartitionByRange("age")
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        """
        if isinstance(numPartitions, int):
            if len(cols) == 0:
                raise ValueError("At least one partition-by expression must be specified.")
            else:
                return DataFrame(
                    self._jdf.repartitionByRange(numPartitions, self._jcols(*cols)), self.sql_ctx
                )
        elif isinstance(numPartitions, (str, Column)):
            cols = (numPartitions,) + cols
            return DataFrame(self._jdf.repartitionByRange(self._jcols(*cols)), self.sql_ctx)
        else:
            raise TypeError("numPartitions should be an int, string or Column")

    def distinct(self) -> "DataFrame":
        """Returns a new :class:`DataFrame` containing the distinct rows in this :class:`DataFrame`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.distinct().count()
        2
        """
        return DataFrame(self._jdf.distinct(), self.sql_ctx)

    @overload
    def sample(self, fraction: float, seed: Optional[int] = ...) -> "DataFrame":
        ...

    @overload
    def sample(
        self,
        withReplacement: Optional[bool],
        fraction: float,
        seed: Optional[int] = ...,
    ) -> "DataFrame":
        ...

    def sample(  # type: ignore[misc]
        self,
        withReplacement: Optional[Union[float, bool]] = None,
        fraction: Optional[Union[int, float]] = None,
        seed: Optional[int] = None,
    ) -> "DataFrame":
        """Returns a sampled subset of this :class:`DataFrame`.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        withReplacement : bool, optional
            Sample with replacement or not (default ``False``).
        fraction : float, optional
            Fraction of rows to generate, range [0.0, 1.0].
        seed : int, optional
            Seed for sampling (default a random seed).

        Notes
        -----
        This is not guaranteed to provide exactly the fraction specified of the total
        count of the given :class:`DataFrame`.

        `fraction` is required and, `withReplacement` and `seed` are optional.

        Examples
        --------
        >>> df = spark.range(10)
        >>> df.sample(0.5, 3).count()
        7
        >>> df.sample(fraction=0.5, seed=3).count()
        7
        >>> df.sample(withReplacement=True, fraction=0.5, seed=3).count()
        1
        >>> df.sample(1.0).count()
        10
        >>> df.sample(fraction=1.0).count()
        10
        >>> df.sample(False, fraction=1.0).count()
        10
        """

        # For the cases below:
        #   sample(True, 0.5 [, seed])
        #   sample(True, fraction=0.5 [, seed])
        #   sample(withReplacement=False, fraction=0.5 [, seed])
        is_withReplacement_set = type(withReplacement) == bool and isinstance(fraction, float)

        # For the case below:
        #   sample(faction=0.5 [, seed])
        is_withReplacement_omitted_kwargs = withReplacement is None and isinstance(fraction, float)

        # For the case below:
        #   sample(0.5 [, seed])
        is_withReplacement_omitted_args = isinstance(withReplacement, float)

        if not (
            is_withReplacement_set
            or is_withReplacement_omitted_kwargs
            or is_withReplacement_omitted_args
        ):
            argtypes = [
                str(type(arg)) for arg in [withReplacement, fraction, seed] if arg is not None
            ]
            raise TypeError(
                "withReplacement (optional), fraction (required) and seed (optional)"
                " should be a bool, float and number; however, "
                "got [%s]." % ", ".join(argtypes)
            )

        if is_withReplacement_omitted_args:
            if fraction is not None:
                seed = cast(int, fraction)
            fraction = withReplacement
            withReplacement = None

        seed = int(seed) if seed is not None else None
        args = [arg for arg in [withReplacement, fraction, seed] if arg is not None]
        jdf = self._jdf.sample(*args)
        return DataFrame(jdf, self.sql_ctx)

    def sampleBy(
        self, col: "ColumnOrName", fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> "DataFrame":
        """
        Returns a stratified sample without replacement based on the
        fraction given on each stratum.

        .. versionadded:: 1.5.0

        Parameters
        ----------
        col : :class:`Column` or str
            column that defines strata

            .. versionchanged:: 3.0
               Added sampling by a column of :class:`Column`
        fractions : dict
            sampling fraction for each stratum. If a stratum is not
            specified, we treat its fraction as zero.
        seed : int, optional
            random seed

        Returns
        -------
        a new :class:`DataFrame` that represents the stratified sample

        Examples
        --------
        >>> from pyspark.sql.functions import col
        >>> dataset = sqlContext.range(0, 100).select((col("id") % 3).alias("key"))
        >>> sampled = dataset.sampleBy("key", fractions={0: 0.1, 1: 0.2}, seed=0)
        >>> sampled.groupBy("key").count().orderBy("key").show()
        +---+-----+
        |key|count|
        +---+-----+
        |  0|    3|
        |  1|    6|
        +---+-----+
        >>> dataset.sampleBy(col("key"), fractions={2: 1.0}, seed=0).count()
        33
        """
        if isinstance(col, str):
            col = Column(col)
        elif not isinstance(col, Column):
            raise TypeError("col must be a string or a column, but got %r" % type(col))
        if not isinstance(fractions, dict):
            raise TypeError("fractions must be a dict but got %r" % type(fractions))
        for k, v in fractions.items():
            if not isinstance(k, (float, int, str)):
                raise TypeError("key must be float, int, or string, but got %r" % type(k))
            fractions[k] = float(v)
        col = col._jc
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        return DataFrame(self._jdf.stat().sampleBy(col, self._jmap(fractions), seed), self.sql_ctx)

    def randomSplit(self, weights: List[float], seed: Optional[int] = None) -> List["DataFrame"]:
        """Randomly splits this :class:`DataFrame` with the provided weights.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        weights : list
            list of doubles as weights with which to split the :class:`DataFrame`.
            Weights will be normalized if they don't sum up to 1.0.
        seed : int, optional
            The seed for sampling.

        Examples
        --------
        >>> splits = df4.randomSplit([1.0, 2.0], 24)
        >>> splits[0].count()
        2

        >>> splits[1].count()
        2
        """
        for w in weights:
            if w < 0.0:
                raise ValueError("Weights must be positive. Found weight value: %s" % w)
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        rdd_array = self._jdf.randomSplit(
            _to_list(self.sql_ctx._sc, cast(List["ColumnOrName"], weights)), int(seed)
        )
        return [DataFrame(rdd, self.sql_ctx) for rdd in rdd_array]

    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        """Returns all column names and their data types as a list.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.dtypes
        [('age', 'int'), ('name', 'string')]
        """
        return [(str(f.name), f.dataType.simpleString()) for f in self.schema.fields]

    @property
    def columns(self) -> List[str]:
        """Returns all column names as a list.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.columns
        ['age', 'name']
        """
        return [f.name for f in self.schema.fields]

    def colRegex(self, colName: str) -> Column:
        """
        Selects column based on the column name specified as a regex and returns it
        as :class:`Column`.

        .. versionadded:: 2.3.0

        Parameters
        ----------
        colName : str
            string, column name specified as a regex.

        Examples
        --------
        >>> df = spark.createDataFrame([("a", 1), ("b", 2), ("c",  3)], ["Col1", "Col2"])
        >>> df.select(df.colRegex("`(Col1)?+.+`")).show()
        +----+
        |Col2|
        +----+
        |   1|
        |   2|
        |   3|
        +----+
        """
        if not isinstance(colName, str):
            raise TypeError("colName should be provided as string")
        jc = self._jdf.colRegex(colName)
        return Column(jc)

    def alias(self, alias: str) -> "DataFrame":
        """Returns a new :class:`DataFrame` with an alias set.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        alias : str
            an alias name to be set for the :class:`DataFrame`.

        Examples
        --------
        >>> from pyspark.sql.functions import *
        >>> df_as1 = df.alias("df_as1")
        >>> df_as2 = df.alias("df_as2")
        >>> joined_df = df_as1.join(df_as2, col("df_as1.name") == col("df_as2.name"), 'inner')
        >>> joined_df.select("df_as1.name", "df_as2.name", "df_as2.age") \
                .sort(desc("df_as1.name")).collect()
        [Row(name='Bob', name='Bob', age=5), Row(name='Alice', name='Alice', age=2)]
        """
        assert isinstance(alias, str), "alias should be a string"
        return DataFrame(getattr(self._jdf, "as")(alias), self.sql_ctx)

    def crossJoin(self, other: "DataFrame") -> "DataFrame":
        """Returns the cartesian product with another :class:`DataFrame`.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        other : :class:`DataFrame`
            Right side of the cartesian product.

        Examples
        --------
        >>> df.select("age", "name").collect()
        [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        >>> df2.select("name", "height").collect()
        [Row(name='Tom', height=80), Row(name='Bob', height=85)]
        >>> df.crossJoin(df2.select("height")).select("age", "name", "height").collect()
        [Row(age=2, name='Alice', height=80), Row(age=2, name='Alice', height=85),
         Row(age=5, name='Bob', height=80), Row(age=5, name='Bob', height=85)]
        """

        jdf = self._jdf.crossJoin(other._jdf)
        return DataFrame(jdf, self.sql_ctx)

    def join(
        self,
        other: "DataFrame",
        on: Optional[Union[str, List[str], Column, List[Column]]] = None,
        how: Optional[str] = None,
    ) -> "DataFrame":
        """Joins with another :class:`DataFrame`, using the given join expression.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        other : :class:`DataFrame`
            Right side of the join
        on : str, list or :class:`Column`, optional
            a string for the join column name, a list of column names,
            a join expression (Column), or a list of Columns.
            If `on` is a string or a list of strings indicating the name of the join column(s),
            the column(s) must exist on both sides, and this performs an equi-join.
        how : str, optional
            default ``inner``. Must be one of: ``inner``, ``cross``, ``outer``,
            ``full``, ``fullouter``, ``full_outer``, ``left``, ``leftouter``, ``left_outer``,
            ``right``, ``rightouter``, ``right_outer``, ``semi``, ``leftsemi``, ``left_semi``,
            ``anti``, ``leftanti`` and ``left_anti``.

        Examples
        --------
        The following performs a full outer join between ``df1`` and ``df2``.

        >>> from pyspark.sql.functions import desc
        >>> df.join(df2, df.name == df2.name, 'outer').select(df.name, df2.height) \
                .sort(desc("name")).collect()
        [Row(name='Bob', height=85), Row(name='Alice', height=None), Row(name=None, height=80)]

        >>> df.join(df2, 'name', 'outer').select('name', 'height').sort(desc("name")).collect()
        [Row(name='Tom', height=80), Row(name='Bob', height=85), Row(name='Alice', height=None)]

        >>> cond = [df.name == df3.name, df.age == df3.age]
        >>> df.join(df3, cond, 'outer').select(df.name, df3.age).collect()
        [Row(name='Alice', age=2), Row(name='Bob', age=5)]

        >>> df.join(df2, 'name').select(df.name, df2.height).collect()
        [Row(name='Bob', height=85)]

        >>> df.join(df4, ['name', 'age']).select(df.name, df.age).collect()
        [Row(name='Bob', age=5)]
        """

        if on is not None and not isinstance(on, list):
            on = [on]  # type: ignore[assignment]

        if on is not None:
            if isinstance(on[0], str):
                on = self._jseq(cast(List[str], on))
            else:
                assert isinstance(on[0], Column), "on should be Column or list of Column"
                on = reduce(lambda x, y: x.__and__(y), cast(List[Column], on))
                on = on._jc

        if on is None and how is None:
            jdf = self._jdf.join(other._jdf)
        else:
            if how is None:
                how = "inner"
            if on is None:
                on = self._jseq([])
            assert isinstance(how, str), "how should be a string"
            jdf = self._jdf.join(other._jdf, on, how)
        return DataFrame(jdf, self.sql_ctx)

    # TODO(SPARK-22947): Fix the DataFrame API.
    def _joinAsOf(
        self,
        other: "DataFrame",
        leftAsOfColumn: Union[str, Column],
        rightAsOfColumn: Union[str, Column],
        on: Optional[Union[str, List[str], Column, List[Column]]] = None,
        how: Optional[str] = None,
        *,
        tolerance: Optional[Column] = None,
        allowExactMatches: bool = True,
        direction: str = "backward",
    ) -> "DataFrame":
        """
        Perform an as-of join.

        This is similar to a left-join except that we match on nearest
        key rather than equal keys.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        other : :class:`DataFrame`
            Right side of the join
        leftAsOfColumn : str or :class:`Column`
            a string for the as-of join column name, or a Column
        rightAsOfColumn : str or :class:`Column`
            a string for the as-of join column name, or a Column
        on : str, list or :class:`Column`, optional
            a string for the join column name, a list of column names,
            a join expression (Column), or a list of Columns.
            If `on` is a string or a list of strings indicating the name of the join column(s),
            the column(s) must exist on both sides, and this performs an equi-join.
        how : str, optional
            default ``inner``. Must be one of: ``inner`` and ``left``.
        tolerance : :class:`Column`, optional
            an asof tolerance within this range; must be compatible
            with the merge index.
        allowExactMatches : bool, optional
            default ``True``.
        direction : str, optional
            default ``backward``. Must be one of: ``backward``, ``forward``, and ``nearest``.

        Examples
        --------
        The following performs an as-of join between ``left`` and ``right``.

        >>> left = spark.createDataFrame([(1, "a"), (5, "b"), (10,  "c")], ["a", "left_val"])
        >>> right = spark.createDataFrame([(1, 1), (2, 2), (3, 3), (6, 6), (7, 7)],
        ...                               ["a", "right_val"])
        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a"
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1),
         Row(a=5, left_val='b', right_val=3),
         Row(a=10, left_val='c', right_val=7)]

        >>> from pyspark.sql import functions as F
        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", tolerance=F.lit(1)
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1)]

        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", how="left", tolerance=F.lit(1)
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1),
         Row(a=5, left_val='b', right_val=None),
         Row(a=10, left_val='c', right_val=None)]

        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", allowExactMatches=False
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=5, left_val='b', right_val=3),
         Row(a=10, left_val='c', right_val=7)]

        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", direction="forward"
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1),
         Row(a=5, left_val='b', right_val=6)]
        """
        if isinstance(leftAsOfColumn, str):
            leftAsOfColumn = self[leftAsOfColumn]
        left_as_of_jcol = cast(Column, leftAsOfColumn)._jc
        if isinstance(rightAsOfColumn, str):
            rightAsOfColumn = other[rightAsOfColumn]
        right_as_of_jcol = cast(Column, rightAsOfColumn)._jc

        if on is not None and not isinstance(on, list):
            on = [on]  # type: ignore[assignment]

        if on is not None:
            if isinstance(on[0], str):
                on = self._jseq(cast(List[str], on))
            else:
                assert isinstance(on[0], Column), "on should be Column or list of Column"
                on = reduce(lambda x, y: x.__and__(y), cast(List[Column], on))
                on = on._jc

        if how is None:
            how = "inner"
        assert isinstance(how, str), "how should be a string"

        if tolerance is not None:
            assert isinstance(tolerance, Column), "tolerance should be Column"
            tolerance = tolerance._jc

        jdf = self._jdf.joinAsOf(
            other._jdf,
            left_as_of_jcol,
            right_as_of_jcol,
            on,
            how,
            tolerance,
            allowExactMatches,
            direction,
        )
        return DataFrame(jdf, self.sql_ctx)

    def sortWithinPartitions(
        self, *cols: Union[str, Column, List[Union[str, Column]]], **kwargs: Any
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` with each partition sorted by the specified column(s).

        .. versionadded:: 1.6.0

        Parameters
        ----------
        cols : str, list or :class:`Column`, optional
            list of :class:`Column` or column names to sort by.

        Other Parameters
        ----------------
        ascending : bool or list, optional
            boolean or list of boolean (default ``True``).
            Sort ascending vs. descending. Specify list for multiple sort orders.
            If a list is specified, length of the list must equal length of the `cols`.

        Examples
        --------
        >>> df.sortWithinPartitions("age", ascending=False).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        """
        jdf = self._jdf.sortWithinPartitions(self._sort_cols(cols, kwargs))
        return DataFrame(jdf, self.sql_ctx)

    def sort(
        self, *cols: Union[str, Column, List[Union[str, Column]]], **kwargs: Any
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` sorted by the specified column(s).

        .. versionadded:: 1.3.0

        Parameters
        ----------
        cols : str, list, or :class:`Column`, optional
             list of :class:`Column` or column names to sort by.

        Other Parameters
        ----------------
        ascending : bool or list, optional
            boolean or list of boolean (default ``True``).
            Sort ascending vs. descending. Specify list for multiple sort orders.
            If a list is specified, length of the list must equal length of the `cols`.

        Examples
        --------
        >>> df.sort(df.age.desc()).collect()
        [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        >>> df.sort("age", ascending=False).collect()
        [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        >>> df.orderBy(df.age.desc()).collect()
        [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        >>> from pyspark.sql.functions import *
        >>> df.sort(asc("age")).collect()
        [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        >>> df.orderBy(desc("age"), "name").collect()
        [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        >>> df.orderBy(["age", "name"], ascending=[0, 1]).collect()
        [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        """
        jdf = self._jdf.sort(self._sort_cols(cols, kwargs))
        return DataFrame(jdf, self.sql_ctx)

    orderBy = sort

    def _jseq(
        self,
        cols: Sequence,
        converter: Optional[Callable[..., Union["PrimitiveType", JavaObject]]] = None,
    ) -> JavaObject:
        """Return a JVM Seq of Columns from a list of Column or names"""
        return _to_seq(self.sql_ctx._sc, cols, converter)

    def _jmap(self, jm: Dict) -> JavaObject:
        """Return a JVM Scala Map from a dict"""
        return _to_scala_map(self.sql_ctx._sc, jm)

    def _jcols(self, *cols: "ColumnOrName") -> JavaObject:
        """Return a JVM Seq of Columns from a list of Column or column names

        If `cols` has only one list in it, cols[0] will be used as the list.
        """
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        return self._jseq(cols, _to_java_column)

    def _sort_cols(
        self, cols: Sequence[Union[str, Column, List[Union[str, Column]]]], kwargs: Dict[str, Any]
    ) -> JavaObject:
        """Return a JVM Seq of Columns that describes the sort order"""
        if not cols:
            raise ValueError("should sort by at least one column")
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        jcols = [_to_java_column(cast("ColumnOrName", c)) for c in cols]
        ascending = kwargs.get("ascending", True)
        if isinstance(ascending, (bool, int)):
            if not ascending:
                jcols = [jc.desc() for jc in jcols]
        elif isinstance(ascending, list):
            jcols = [jc if asc else jc.desc() for asc, jc in zip(ascending, jcols)]
        else:
            raise TypeError("ascending can only be boolean or list, but got %s" % type(ascending))
        return self._jseq(jcols)

    def describe(self, *cols: Union[str, List[str]]) -> "DataFrame":
        """Computes basic statistics for numeric and string columns.

        .. versionadded:: 1.3.1

        This include count, mean, stddev, min, and max. If no columns are
        given, this function computes statistics for all numerical or string columns.

        Notes
        -----
        This function is meant for exploratory data analysis, as we make no
        guarantee about the backward compatibility of the schema of the resulting
        :class:`DataFrame`.

        Use summary for expanded statistics and control over which statistics to compute.

        Examples
        --------
        >>> df.describe(['age']).show()
        +-------+------------------+
        |summary|               age|
        +-------+------------------+
        |  count|                 2|
        |   mean|               3.5|
        | stddev|2.1213203435596424|
        |    min|                 2|
        |    max|                 5|
        +-------+------------------+
        >>> df.describe().show()
        +-------+------------------+-----+
        |summary|               age| name|
        +-------+------------------+-----+
        |  count|                 2|    2|
        |   mean|               3.5| null|
        | stddev|2.1213203435596424| null|
        |    min|                 2|Alice|
        |    max|                 5|  Bob|
        +-------+------------------+-----+

        See Also
        --------
        DataFrame.summary
        """
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]  # type: ignore[assignment]
        jdf = self._jdf.describe(self._jseq(cols))
        return DataFrame(jdf, self.sql_ctx)

    def summary(self, *statistics: str) -> "DataFrame":
        """Computes specified statistics for numeric and string columns. Available statistics are:
        - count
        - mean
        - stddev
        - min
        - max
        - arbitrary approximate percentiles specified as a percentage (e.g., 75%)

        If no statistics are given, this function computes count, mean, stddev, min,
        approximate quartiles (percentiles at 25%, 50%, and 75%), and max.

        .. versionadded:: 2.3.0

        Notes
        -----
        This function is meant for exploratory data analysis, as we make no
        guarantee about the backward compatibility of the schema of the resulting
        :class:`DataFrame`.

        Examples
        --------
        >>> df.summary().show()
        +-------+------------------+-----+
        |summary|               age| name|
        +-------+------------------+-----+
        |  count|                 2|    2|
        |   mean|               3.5| null|
        | stddev|2.1213203435596424| null|
        |    min|                 2|Alice|
        |    25%|                 2| null|
        |    50%|                 2| null|
        |    75%|                 5| null|
        |    max|                 5|  Bob|
        +-------+------------------+-----+

        >>> df.summary("count", "min", "25%", "75%", "max").show()
        +-------+---+-----+
        |summary|age| name|
        +-------+---+-----+
        |  count|  2|    2|
        |    min|  2|Alice|
        |    25%|  2| null|
        |    75%|  5| null|
        |    max|  5|  Bob|
        +-------+---+-----+

        To do a summary for specific columns first select them:

        >>> df.select("age", "name").summary("count").show()
        +-------+---+----+
        |summary|age|name|
        +-------+---+----+
        |  count|  2|   2|
        +-------+---+----+

        See Also
        --------
        DataFrame.display
        """
        if len(statistics) == 1 and isinstance(statistics[0], list):
            statistics = statistics[0]
        jdf = self._jdf.summary(self._jseq(statistics))
        return DataFrame(jdf, self.sql_ctx)

    @overload
    def head(self) -> Optional[Row]:
        ...

    @overload
    def head(self, n: int) -> List[Row]:
        ...

    def head(self, n: Optional[int] = None) -> Union[Optional[Row], List[Row]]:
        """Returns the first ``n`` rows.

        .. versionadded:: 1.3.0

        Notes
        -----
        This method should only be used if the resulting array is expected
        to be small, as all the data is loaded into the driver's memory.

        Parameters
        ----------
        n : int, optional
            default 1. Number of rows to return.

        Returns
        -------
        If n is greater than 1, return a list of :class:`Row`.
        If n is 1, return a single Row.

        Examples
        --------
        >>> df.head()
        Row(age=2, name='Alice')
        >>> df.head(1)
        [Row(age=2, name='Alice')]
        """
        if n is None:
            rs = self.head(1)
            return rs[0] if rs else None
        return self.take(n)

    def first(self) -> Optional[Row]:
        """Returns the first row as a :class:`Row`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.first()
        Row(age=2, name='Alice')
        """
        return self.head()

    @overload
    def __getitem__(self, item: Union[int, str]) -> Column:
        ...

    @overload
    def __getitem__(self, item: Union[Column, List, Tuple]) -> "DataFrame":
        ...

    def __getitem__(self, item: Union[int, str, Column, List, Tuple]) -> Union[Column, "DataFrame"]:
        """Returns the column as a :class:`Column`.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.select(df['age']).collect()
        [Row(age=2), Row(age=5)]
        >>> df[ ["name", "age"]].collect()
        [Row(name='Alice', age=2), Row(name='Bob', age=5)]
        >>> df[ df.age > 3 ].collect()
        [Row(age=5, name='Bob')]
        >>> df[df[0] > 3].collect()
        [Row(age=5, name='Bob')]
        """
        if isinstance(item, str):
            jc = self._jdf.apply(item)
            return Column(jc)
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, (list, tuple)):
            return self.select(*item)
        elif isinstance(item, int):
            jc = self._jdf.apply(self.columns[item])
            return Column(jc)
        else:
            raise TypeError("unexpected item type: %s" % type(item))

    def __getattr__(self, name: str) -> Column:
        """Returns the :class:`Column` denoted by ``name``.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.select(df.age).collect()
        [Row(age=2), Row(age=5)]
        """
        if name not in self.columns:
            raise AttributeError(
                "'%s' object has no attribute '%s'" % (self.__class__.__name__, name)
            )
        jc = self._jdf.apply(name)
        return Column(jc)

    @overload
    def select(self, *cols: "ColumnOrName") -> "DataFrame":
        ...

    @overload
    def select(self, __cols: Union[List[Column], List[str]]) -> "DataFrame":
        ...

    def select(self, *cols: "ColumnOrName") -> "DataFrame":  # type: ignore[misc]
        """Projects a set of expressions and returns a new :class:`DataFrame`.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        cols : str, :class:`Column`, or list
            column names (string) or expressions (:class:`Column`).
            If one of the column names is '*', that column is expanded to include all columns
            in the current :class:`DataFrame`.

        Examples
        --------
        >>> df.select('*').collect()
        [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        >>> df.select('name', 'age').collect()
        [Row(name='Alice', age=2), Row(name='Bob', age=5)]
        >>> df.select(df.name, (df.age + 10).alias('age')).collect()
        [Row(name='Alice', age=12), Row(name='Bob', age=15)]
        """
        jdf = self._jdf.select(self._jcols(*cols))
        return DataFrame(jdf, self.sql_ctx)

    @overload
    def selectExpr(self, *expr: str) -> "DataFrame":
        ...

    @overload
    def selectExpr(self, *expr: List[str]) -> "DataFrame":
        ...

    def selectExpr(self, *expr: Union[str, List[str]]) -> "DataFrame":
        """Projects a set of SQL expressions and returns a new :class:`DataFrame`.

        This is a variant of :func:`select` that accepts SQL expressions.

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.selectExpr("age * 2", "abs(age)").collect()
        [Row((age * 2)=4, abs(age)=2), Row((age * 2)=10, abs(age)=5)]
        """
        if len(expr) == 1 and isinstance(expr[0], list):
            expr = expr[0]  # type: ignore[assignment]
        jdf = self._jdf.selectExpr(self._jseq(expr))
        return DataFrame(jdf, self.sql_ctx)

    def filter(self, condition: "ColumnOrName") -> "DataFrame":
        """Filters rows using the given condition.

        :func:`where` is an alias for :func:`filter`.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        condition : :class:`Column` or str
            a :class:`Column` of :class:`types.BooleanType`
            or a string of SQL expression.

        Examples
        --------
        >>> df.filter(df.age > 3).collect()
        [Row(age=5, name='Bob')]
        >>> df.where(df.age == 2).collect()
        [Row(age=2, name='Alice')]

        >>> df.filter("age > 3").collect()
        [Row(age=5, name='Bob')]
        >>> df.where("age = 2").collect()
        [Row(age=2, name='Alice')]
        """
        if isinstance(condition, str):
            jdf = self._jdf.filter(condition)
        elif isinstance(condition, Column):
            jdf = self._jdf.filter(condition._jc)
        else:
            raise TypeError("condition should be string or Column")
        return DataFrame(jdf, self.sql_ctx)

    @overload
    def groupBy(self, *cols: "ColumnOrName") -> "GroupedData":
        ...

    @overload
    def groupBy(self, __cols: Union[List[Column], List[str]]) -> "GroupedData":
        ...

    def groupBy(self, *cols: "ColumnOrName") -> "GroupedData":  # type: ignore[misc]
        """Groups the :class:`DataFrame` using the specified columns,
        so we can run aggregation on them. See :class:`GroupedData`
        for all the available aggregate functions.

        :func:`groupby` is an alias for :func:`groupBy`.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        cols : list, str or :class:`Column`
            columns to group by.
            Each element should be a column name (string) or an expression (:class:`Column`).

        Examples
        --------
        >>> df.groupBy().avg().collect()
        [Row(avg(age)=3.5)]
        >>> sorted(df.groupBy('name').agg({'age': 'mean'}).collect())
        [Row(name='Alice', avg(age)=2.0), Row(name='Bob', avg(age)=5.0)]
        >>> sorted(df.groupBy(df.name).avg().collect())
        [Row(name='Alice', avg(age)=2.0), Row(name='Bob', avg(age)=5.0)]
        >>> sorted(df.groupBy(['name', df.age]).count().collect())
        [Row(name='Alice', age=2, count=1), Row(name='Bob', age=5, count=1)]
        """
        jgd = self._jdf.groupBy(self._jcols(*cols))
        from pyspark.sql.group import GroupedData

        return GroupedData(jgd, self)

    @overload
    def rollup(self, *cols: "ColumnOrName") -> "GroupedData":
        ...

    @overload
    def rollup(self, __cols: Union[List[Column], List[str]]) -> "GroupedData":
        ...

    def rollup(self, *cols: "ColumnOrName") -> "GroupedData":  # type: ignore[misc]
        """
        Create a multi-dimensional rollup for the current :class:`DataFrame` using
        the specified columns, so we can run aggregation on them.

        .. versionadded:: 1.4.0

        Examples
        --------
        >>> df.rollup("name", df.age).count().orderBy("name", "age").show()
        +-----+----+-----+
        | name| age|count|
        +-----+----+-----+
        | null|null|    2|
        |Alice|null|    1|
        |Alice|   2|    1|
        |  Bob|null|    1|
        |  Bob|   5|    1|
        +-----+----+-----+
        """
        jgd = self._jdf.rollup(self._jcols(*cols))
        from pyspark.sql.group import GroupedData

        return GroupedData(jgd, self)

    @overload
    def cube(self, *cols: "ColumnOrName") -> "GroupedData":
        ...

    @overload
    def cube(self, __cols: Union[List[Column], List[str]]) -> "GroupedData":
        ...

    def cube(self, *cols: "ColumnOrName") -> "GroupedData":  # type: ignore[misc]
        """
        Create a multi-dimensional cube for the current :class:`DataFrame` using
        the specified columns, so we can run aggregations on them.

        .. versionadded:: 1.4.0

        Examples
        --------
        >>> df.cube("name", df.age).count().orderBy("name", "age").show()
        +-----+----+-----+
        | name| age|count|
        +-----+----+-----+
        | null|null|    2|
        | null|   2|    1|
        | null|   5|    1|
        |Alice|null|    1|
        |Alice|   2|    1|
        |  Bob|null|    1|
        |  Bob|   5|    1|
        +-----+----+-----+
        """
        jgd = self._jdf.cube(self._jcols(*cols))
        from pyspark.sql.group import GroupedData

        return GroupedData(jgd, self)

    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> "DataFrame":
        """Aggregate on the entire :class:`DataFrame` without groups
        (shorthand for ``df.groupBy().agg()``).

        .. versionadded:: 1.3.0

        Examples
        --------
        >>> df.agg({"age": "max"}).collect()
        [Row(max(age)=5)]
        >>> from pyspark.sql import functions as F
        >>> df.agg(F.min(df.age)).collect()
        [Row(min(age)=2)]
        """
        return self.groupBy().agg(*exprs)  # type: ignore[arg-type]

    @since(3.3)
    def observe(self, observation: "Observation", *exprs: Column) -> "DataFrame":
        """Observe (named) metrics through an :class:`Observation` instance.

        A user can retrieve the metrics by accessing `Observation.get`.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        observation : :class:`Observation`
            an :class:`Observation` instance to obtain the metric.
        exprs : list of :class:`Column`
            column expressions (:class:`Column`).

        Returns
        -------
        :class:`DataFrame`
            the observed :class:`DataFrame`.

        Notes
        -----
        This method does not support streaming datasets.

        Examples
        --------
        >>> from pyspark.sql.functions import col, count, lit, max
        >>> from pyspark.sql import Observation
        >>> observation = Observation("my metrics")
        >>> observed_df = df.observe(observation, count(lit(1)).alias("count"), max(col("age")))
        >>> observed_df.count()
        2
        >>> observation.get
        {'count': 2, 'max(age)': 5}
        """
        from pyspark.sql import Observation

        assert isinstance(observation, Observation), "observation should be Observation"
        return observation._on(self, *exprs)

    @since(2.0)
    def union(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing union of rows in this and another
        :class:`DataFrame`.

        This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union
        (that does deduplication of elements), use this function followed by :func:`distinct`.

        Also as standard in SQL, this function resolves columns by position (not by name).
        """
        return DataFrame(self._jdf.union(other._jdf), self.sql_ctx)

    @since(1.3)
    def unionAll(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing union of rows in this and another
        :class:`DataFrame`.

        This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union
        (that does deduplication of elements), use this function followed by :func:`distinct`.

        Also as standard in SQL, this function resolves columns by position (not by name).
        """
        return self.union(other)

    def unionByName(self, other: "DataFrame", allowMissingColumns: bool = False) -> "DataFrame":
        """Returns a new :class:`DataFrame` containing union of rows in this and another
        :class:`DataFrame`.

        This is different from both `UNION ALL` and `UNION DISTINCT` in SQL. To do a SQL-style set
        union (that does deduplication of elements), use this function followed by :func:`distinct`.

        .. versionadded:: 2.3.0

        Examples
        --------
        The difference between this function and :func:`union` is that this function
        resolves columns by name (not by position):

        >>> df1 = spark.createDataFrame([[1, 2, 3]], ["col0", "col1", "col2"])
        >>> df2 = spark.createDataFrame([[4, 5, 6]], ["col1", "col2", "col0"])
        >>> df1.unionByName(df2).show()
        +----+----+----+
        |col0|col1|col2|
        +----+----+----+
        |   1|   2|   3|
        |   6|   4|   5|
        +----+----+----+

        When the parameter `allowMissingColumns` is ``True``, the set of column names
        in this and other :class:`DataFrame` can differ; missing columns will be filled with null.
        Further, the missing columns of this :class:`DataFrame` will be added at the end
        in the schema of the union result:

        >>> df1 = spark.createDataFrame([[1, 2, 3]], ["col0", "col1", "col2"])
        >>> df2 = spark.createDataFrame([[4, 5, 6]], ["col1", "col2", "col3"])
        >>> df1.unionByName(df2, allowMissingColumns=True).show()
        +----+----+----+----+
        |col0|col1|col2|col3|
        +----+----+----+----+
        |   1|   2|   3|null|
        |null|   4|   5|   6|
        +----+----+----+----+

        .. versionchanged:: 3.1.0
           Added optional argument `allowMissingColumns` to specify whether to allow
           missing columns.
        """
        return DataFrame(self._jdf.unionByName(other._jdf, allowMissingColumns), self.sql_ctx)

    @since(1.3)
    def intersect(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows only in
        both this :class:`DataFrame` and another :class:`DataFrame`.

        This is equivalent to `INTERSECT` in SQL.
        """
        return DataFrame(self._jdf.intersect(other._jdf), self.sql_ctx)

    def intersectAll(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows in both this :class:`DataFrame`
        and another :class:`DataFrame` while preserving duplicates.

        This is equivalent to `INTERSECT ALL` in SQL. As standard in SQL, this function
        resolves columns by position (not by name).

        .. versionadded:: 2.4.0

        Examples
        --------
        >>> df1 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3), ("c", 4)], ["C1", "C2"])
        >>> df2 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3)], ["C1", "C2"])

        >>> df1.intersectAll(df2).sort("C1", "C2").show()
        +---+---+
        | C1| C2|
        +---+---+
        |  a|  1|
        |  a|  1|
        |  b|  3|
        +---+---+

        """
        return DataFrame(self._jdf.intersectAll(other._jdf), self.sql_ctx)

    @since(1.3)
    def subtract(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows in this :class:`DataFrame`
        but not in another :class:`DataFrame`.

        This is equivalent to `EXCEPT DISTINCT` in SQL.

        """
        return DataFrame(getattr(self._jdf, "except")(other._jdf), self.sql_ctx)

    def dropDuplicates(self, subset: Optional[List[str]] = None) -> "DataFrame":
        """Return a new :class:`DataFrame` with duplicate rows removed,
        optionally only considering certain columns.

        For a static batch :class:`DataFrame`, it just drops duplicate rows. For a streaming
        :class:`DataFrame`, it will keep all data across triggers as intermediate state to drop
        duplicates rows. You can use :func:`withWatermark` to limit how late the duplicate data can
        be and system will accordingly limit the state. In addition, too late data older than
        watermark will be dropped to avoid any possibility of duplicates.

        :func:`drop_duplicates` is an alias for :func:`dropDuplicates`.

        .. versionadded:: 1.4.0

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = sc.parallelize([ \\
        ...     Row(name='Alice', age=5, height=80), \\
        ...     Row(name='Alice', age=5, height=80), \\
        ...     Row(name='Alice', age=10, height=80)]).toDF()
        >>> df.dropDuplicates().show()
        +-----+---+------+
        | name|age|height|
        +-----+---+------+
        |Alice|  5|    80|
        |Alice| 10|    80|
        +-----+---+------+

        >>> df.dropDuplicates(['name', 'height']).show()
        +-----+---+------+
        | name|age|height|
        +-----+---+------+
        |Alice|  5|    80|
        +-----+---+------+
        """
        if subset is not None and (not isinstance(subset, Iterable) or isinstance(subset, str)):
            raise TypeError("Parameter 'subset' must be a list of columns")

        if subset is None:
            jdf = self._jdf.dropDuplicates()
        else:
            jdf = self._jdf.dropDuplicates(self._jseq(subset))
        return DataFrame(jdf, self.sql_ctx)

    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` omitting rows with null values.
        :func:`DataFrame.dropna` and :func:`DataFrameNaFunctions.drop` are aliases of each other.

        .. versionadded:: 1.3.1

        Parameters
        ----------
        how : str, optional
            'any' or 'all'.
            If 'any', drop a row if it contains any nulls.
            If 'all', drop a row only if all its values are null.
        thresh: int, optional
            default None
            If specified, drop rows that have less than `thresh` non-null values.
            This overwrites the `how` parameter.
        subset : str, tuple or list, optional
            optional list of column names to consider.

        Examples
        --------
        >>> df4.na.drop().show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        | 10|    80|Alice|
        +---+------+-----+
        """
        if how is not None and how not in ["any", "all"]:
            raise ValueError("how ('" + how + "') should be 'any' or 'all'")

        if subset is None:
            subset = self.columns
        elif isinstance(subset, str):
            subset = [subset]
        elif not isinstance(subset, (list, tuple)):
            raise TypeError("subset should be a list or tuple of column names")

        if thresh is None:
            thresh = len(subset) if how == "any" else 1

        return DataFrame(self._jdf.na().drop(thresh, self._jseq(subset)), self.sql_ctx)

    @overload
    def fillna(
        self,
        value: "LiteralType",
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = ...,
    ) -> "DataFrame":
        ...

    @overload
    def fillna(self, value: Dict[str, "LiteralType"]) -> "DataFrame":
        ...

    def fillna(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> "DataFrame":
        """Replace null values, alias for ``na.fill()``.
        :func:`DataFrame.fillna` and :func:`DataFrameNaFunctions.fill` are aliases of each other.

        .. versionadded:: 1.3.1

        Parameters
        ----------
        value : int, float, string, bool or dict
            Value to replace null values with.
            If the value is a dict, then `subset` is ignored and `value` must be a mapping
            from column name (string) to replacement value. The replacement value must be
            an int, float, boolean, or string.
        subset : str, tuple or list, optional
            optional list of column names to consider.
            Columns specified in subset that do not have matching data type are ignored.
            For example, if `value` is a string, and subset contains a non-string column,
            then the non-string column is simply ignored.

        Examples
        --------
        >>> df4.na.fill(50).show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        | 10|    80|Alice|
        |  5|    50|  Bob|
        | 50|    50|  Tom|
        | 50|    50| null|
        +---+------+-----+

        >>> df5.na.fill(False).show()
        +----+-------+-----+
        | age|   name|  spy|
        +----+-------+-----+
        |  10|  Alice|false|
        |   5|    Bob|false|
        |null|Mallory| true|
        +----+-------+-----+

        >>> df4.na.fill({'age': 50, 'name': 'unknown'}).show()
        +---+------+-------+
        |age|height|   name|
        +---+------+-------+
        | 10|    80|  Alice|
        |  5|  null|    Bob|
        | 50|  null|    Tom|
        | 50|  null|unknown|
        +---+------+-------+
        """
        if not isinstance(value, (float, int, str, bool, dict)):
            raise TypeError("value should be a float, int, string, bool or dict")

        # Note that bool validates isinstance(int), but we don't want to
        # convert bools to floats

        if not isinstance(value, bool) and isinstance(value, int):
            value = float(value)

        if isinstance(value, dict):
            return DataFrame(self._jdf.na().fill(value), self.sql_ctx)
        elif subset is None:
            return DataFrame(self._jdf.na().fill(value), self.sql_ctx)
        else:
            if isinstance(subset, str):
                subset = [subset]
            elif not isinstance(subset, (list, tuple)):
                raise TypeError("subset should be a list or tuple of column names")

            return DataFrame(self._jdf.na().fill(value, self._jseq(subset)), self.sql_ctx)

    @overload
    def replace(
        self,
        to_replace: "LiteralType",
        value: "OptionalPrimitiveType",
        subset: Optional[List[str]] = ...,
    ) -> "DataFrame":
        ...

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: List["OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> "DataFrame":
        ...

    @overload
    def replace(
        self,
        to_replace: Dict["LiteralType", "OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> "DataFrame":
        ...

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: "OptionalPrimitiveType",
        subset: Optional[List[str]] = ...,
    ) -> "DataFrame":
        ...

    def replace(  # type: ignore[misc]
        self,
        to_replace: Union[
            "LiteralType", List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]
        ],
        value: Optional[Union["OptionalPrimitiveType", List["OptionalPrimitiveType"]]] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` replacing a value with another value.
        :func:`DataFrame.replace` and :func:`DataFrameNaFunctions.replace` are
        aliases of each other.
        Values to_replace and value must have the same type and can only be numerics, booleans,
        or strings. Value can have None. When replacing, the new value will be cast
        to the type of the existing column.
        For numeric replacements all values to be replaced should have unique
        floating point representation. In case of conflicts (for example with `{42: -1, 42.0: 1}`)
        and arbitrary replacement will be used.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        to_replace : bool, int, float, string, list or dict
            Value to be replaced.
            If the value is a dict, then `value` is ignored or can be omitted, and `to_replace`
            must be a mapping between a value and a replacement.
        value : bool, int, float, string or None, optional
            The replacement value must be a bool, int, float, string or None. If `value` is a
            list, `value` should be of the same length and type as `to_replace`.
            If `value` is a scalar and `to_replace` is a sequence, then `value` is
            used as a replacement for each item in `to_replace`.
        subset : list, optional
            optional list of column names to consider.
            Columns specified in subset that do not have matching data type are ignored.
            For example, if `value` is a string, and subset contains a non-string column,
            then the non-string column is simply ignored.

        Examples
        --------
        >>> df4.na.replace(10, 20).show()
        +----+------+-----+
        | age|height| name|
        +----+------+-----+
        |  20|    80|Alice|
        |   5|  null|  Bob|
        |null|  null|  Tom|
        |null|  null| null|
        +----+------+-----+

        >>> df4.na.replace('Alice', None).show()
        +----+------+----+
        | age|height|name|
        +----+------+----+
        |  10|    80|null|
        |   5|  null| Bob|
        |null|  null| Tom|
        |null|  null|null|
        +----+------+----+

        >>> df4.na.replace({'Alice': None}).show()
        +----+------+----+
        | age|height|name|
        +----+------+----+
        |  10|    80|null|
        |   5|  null| Bob|
        |null|  null| Tom|
        |null|  null|null|
        +----+------+----+

        >>> df4.na.replace(['Alice', 'Bob'], ['A', 'B'], 'name').show()
        +----+------+----+
        | age|height|name|
        +----+------+----+
        |  10|    80|   A|
        |   5|  null|   B|
        |null|  null| Tom|
        |null|  null|null|
        +----+------+----+
        """
        if value is _NoValue:
            if isinstance(to_replace, dict):
                value = None
            else:
                raise TypeError("value argument is required when to_replace is not a dictionary.")

        # Helper functions
        def all_of(types: Union[Type, Tuple[Type, ...]]) -> Callable[[Iterable], bool]:
            """Given a type or tuple of types and a sequence of xs
            check if each x is instance of type(s)

            >>> all_of(bool)([True, False])
            True
            >>> all_of(str)(["a", 1])
            False
            """

            def all_of_(xs: Iterable) -> bool:
                return all(isinstance(x, types) for x in xs)

            return all_of_

        all_of_bool = all_of(bool)
        all_of_str = all_of(str)
        all_of_numeric = all_of((float, int))

        # Validate input types
        valid_types = (bool, float, int, str, list, tuple)
        if not isinstance(to_replace, valid_types + (dict,)):
            raise TypeError(
                "to_replace should be a bool, float, int, string, list, tuple, or dict. "
                "Got {0}".format(type(to_replace))
            )

        if (
            not isinstance(value, valid_types)
            and value is not None
            and not isinstance(to_replace, dict)
        ):
            raise TypeError(
                "If to_replace is not a dict, value should be "
                "a bool, float, int, string, list, tuple or None. "
                "Got {0}".format(type(value))
            )

        if isinstance(to_replace, (list, tuple)) and isinstance(value, (list, tuple)):
            if len(to_replace) != len(value):
                raise ValueError(
                    "to_replace and value lists should be of the same length. "
                    "Got {0} and {1}".format(len(to_replace), len(value))
                )

        if not (subset is None or isinstance(subset, (list, tuple, str))):
            raise TypeError(
                "subset should be a list or tuple of column names, "
                "column name or None. Got {0}".format(type(subset))
            )

        # Reshape input arguments if necessary
        if isinstance(to_replace, (float, int, str)):
            to_replace = [to_replace]

        if isinstance(to_replace, dict):
            rep_dict = to_replace
            if value is not None:
                warnings.warn("to_replace is a dict and value is not None. value will be ignored.")
        else:
            if isinstance(value, (float, int, str)) or value is None:
                value = [value for _ in range(len(to_replace))]
            rep_dict = dict(zip(to_replace, value))

        if isinstance(subset, str):
            subset = [subset]

        # Verify we were not passed in mixed type generics.
        if not any(
            all_of_type(rep_dict.keys())
            and all_of_type(x for x in rep_dict.values() if x is not None)
            for all_of_type in [all_of_bool, all_of_str, all_of_numeric]
        ):
            raise ValueError("Mixed type replacements are not supported")

        if subset is None:
            return DataFrame(self._jdf.na().replace("*", rep_dict), self.sql_ctx)
        else:
            return DataFrame(
                self._jdf.na().replace(self._jseq(subset), self._jmap(rep_dict)), self.sql_ctx
            )

    @overload
    def approxQuantile(
        self,
        col: str,
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[float]:
        ...

    @overload
    def approxQuantile(
        self,
        col: Union[List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[List[float]]:
        ...

    def approxQuantile(
        self,
        col: Union[str, List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        """
        Calculates the approximate quantiles of numerical columns of a
        :class:`DataFrame`.

        The result of this algorithm has the following deterministic bound:
        If the :class:`DataFrame` has N elements and if we request the quantile at
        probability `p` up to error `err`, then the algorithm will return
        a sample `x` from the :class:`DataFrame` so that the *exact* rank of `x` is
        close to (p * N). More precisely,

          floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).

        This method implements a variation of the Greenwald-Khanna
        algorithm (with some speed optimizations). The algorithm was first
        present in [[https://doi.org/10.1145/375663.375670
        Space-efficient Online Computation of Quantile Summaries]]
        by Greenwald and Khanna.

        Note that null values will be ignored in numerical columns before calculation.
        For columns only containing null values, an empty list is returned.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        col: str, tuple or list
            Can be a single column name, or a list of names for multiple columns.

            .. versionchanged:: 2.2
               Added support for multiple columns.
        probabilities : list or tuple
            a list of quantile probabilities
            Each number must belong to [0, 1].
            For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
        relativeError : float
            The relative target precision to achieve
            (>= 0). If set to zero, the exact quantiles are computed, which
            could be very expensive. Note that values greater than 1 are
            accepted but give the same result as 1.

        Returns
        -------
        list
            the approximate quantiles at the given probabilities. If
            the input `col` is a string, the output is a list of floats. If the
            input `col` is a list or tuple of strings, the output is also a
            list, but each element in it is a list of floats, i.e., the output
            is a list of list of floats.
        """

        if not isinstance(col, (str, list, tuple)):
            raise TypeError("col should be a string, list or tuple, but got %r" % type(col))

        isStr = isinstance(col, str)

        if isinstance(col, tuple):
            col = list(col)
        elif isStr:
            col = [cast(str, col)]

        for c in col:
            if not isinstance(c, str):
                raise TypeError("columns should be strings, but got %r" % type(c))
        col = _to_list(self._sc, cast(List["ColumnOrName"], col))

        if not isinstance(probabilities, (list, tuple)):
            raise TypeError("probabilities should be a list or tuple")
        if isinstance(probabilities, tuple):
            probabilities = list(probabilities)
        for p in probabilities:
            if not isinstance(p, (float, int)) or p < 0 or p > 1:
                raise ValueError("probabilities should be numerical (float, int) in [0,1].")
        probabilities = _to_list(self._sc, cast(List["ColumnOrName"], probabilities))

        if not isinstance(relativeError, (float, int)):
            raise TypeError("relativeError should be numerical (float, int)")
        if relativeError < 0:
            raise ValueError("relativeError should be >= 0.")
        relativeError = float(relativeError)

        jaq = self._jdf.stat().approxQuantile(col, probabilities, relativeError)
        jaq_list = [list(j) for j in jaq]
        return jaq_list[0] if isStr else jaq_list

    def corr(self, col1: str, col2: str, method: Optional[str] = None) -> float:
        """
        Calculates the correlation of two columns of a :class:`DataFrame` as a double value.
        Currently only supports the Pearson Correlation Coefficient.
        :func:`DataFrame.corr` and :func:`DataFrameStatFunctions.corr` are aliases of each other.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        col1 : str
            The name of the first column
        col2 : str
            The name of the second column
        method : str, optional
            The correlation method. Currently only supports "pearson"
        """
        if not isinstance(col1, str):
            raise TypeError("col1 should be a string.")
        if not isinstance(col2, str):
            raise TypeError("col2 should be a string.")
        if not method:
            method = "pearson"
        if not method == "pearson":
            raise ValueError(
                "Currently only the calculation of the Pearson Correlation "
                + "coefficient is supported."
            )
        return self._jdf.stat().corr(col1, col2, method)

    def cov(self, col1: str, col2: str) -> float:
        """
        Calculate the sample covariance for the given columns, specified by their names, as a
        double value. :func:`DataFrame.cov` and :func:`DataFrameStatFunctions.cov` are aliases.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        col1 : str
            The name of the first column
        col2 : str
            The name of the second column
        """
        if not isinstance(col1, str):
            raise TypeError("col1 should be a string.")
        if not isinstance(col2, str):
            raise TypeError("col2 should be a string.")
        return self._jdf.stat().cov(col1, col2)

    def crosstab(self, col1: str, col2: str) -> "DataFrame":
        """
        Computes a pair-wise frequency table of the given columns. Also known as a contingency
        table. The number of distinct values for each column should be less than 1e4. At most 1e6
        non-zero pair frequencies will be returned.
        The first column of each row will be the distinct values of `col1` and the column names
        will be the distinct values of `col2`. The name of the first column will be `$col1_$col2`.
        Pairs that have no occurrences will have zero as their counts.
        :func:`DataFrame.crosstab` and :func:`DataFrameStatFunctions.crosstab` are aliases.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        col1 : str
            The name of the first column. Distinct items will make the first item of
            each row.
        col2 : str
            The name of the second column. Distinct items will make the column names
            of the :class:`DataFrame`.
        """
        if not isinstance(col1, str):
            raise TypeError("col1 should be a string.")
        if not isinstance(col2, str):
            raise TypeError("col2 should be a string.")
        return DataFrame(self._jdf.stat().crosstab(col1, col2), self.sql_ctx)

    def freqItems(
        self, cols: Union[List[str], Tuple[str]], support: Optional[float] = None
    ) -> "DataFrame":
        """
        Finding frequent items for columns, possibly with false positives. Using the
        frequent element count algorithm described in
        "https://doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou".
        :func:`DataFrame.freqItems` and :func:`DataFrameStatFunctions.freqItems` are aliases.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        cols : list or tuple
            Names of the columns to calculate frequent items for as a list or tuple of
            strings.
        support : float, optional
            The frequency with which to consider an item 'frequent'. Default is 1%.
            The support must be greater than 1e-4.

        Notes
        -----
        This function is meant for exploratory data analysis, as we make no
        guarantee about the backward compatibility of the schema of the resulting
        :class:`DataFrame`.
        """
        if isinstance(cols, tuple):
            cols = list(cols)
        if not isinstance(cols, list):
            raise TypeError("cols must be a list or tuple of column names as strings.")
        if not support:
            support = 0.01
        return DataFrame(self._jdf.stat().freqItems(_to_seq(self._sc, cols), support), self.sql_ctx)

    def withColumn(self, colName: str, col: Column) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` by adding a column or replacing the
        existing column that has the same name.

        The column expression must be an expression over this :class:`DataFrame`; attempting to add
        a column from some other :class:`DataFrame` will raise an error.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        colName : str
            string, name of the new column.
        col : :class:`Column`
            a :class:`Column` expression for the new column.

        Notes
        -----
        This method introduces a projection internally. Therefore, calling it multiple
        times, for instance, via loops in order to add multiple columns can generate big
        plans which can cause performance issues and even `StackOverflowException`.
        To avoid this, use :func:`select` with the multiple columns at once.

        Examples
        --------
        >>> df.withColumn('age2', df.age + 2).collect()
        [Row(age=2, name='Alice', age2=4), Row(age=5, name='Bob', age2=7)]

        """
        if not isinstance(col, Column):
            raise TypeError("col should be Column")
        return DataFrame(self._jdf.withColumn(colName, col._jc), self.sql_ctx)

    def withColumnRenamed(self, existing: str, new: str) -> "DataFrame":
        """Returns a new :class:`DataFrame` by renaming an existing column.
        This is a no-op if schema doesn't contain the given column name.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        existing : str
            string, name of the existing column to rename.
        new : str
            string, new name of the column.

        Examples
        --------
        >>> df.withColumnRenamed('age', 'age2').collect()
        [Row(age2=2, name='Alice'), Row(age2=5, name='Bob')]
        """
        return DataFrame(self._jdf.withColumnRenamed(existing, new), self.sql_ctx)

    def withMetadata(self, columnName: str, metadata: Dict[str, Any]) -> "DataFrame":
        """Returns a new :class:`DataFrame` by updating an existing column with metadata.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        columnName : str
            string, name of the existing column to update the metadata.
        metadata : dict
            dict, new metadata to be assigned to df.schema[columnName].metadata

        Examples
        --------
        >>> df_meta = df.withMetadata('age', {'foo': 'bar'})
        >>> df_meta.schema['age'].metadata
        {'foo': 'bar'}
        """
        if not isinstance(metadata, dict):
            raise TypeError("metadata should be a dict")
        sc = SparkContext._active_spark_context  # type: ignore[attr-defined]
        jmeta = sc._jvm.org.apache.spark.sql.types.Metadata.fromJson(json.dumps(metadata))
        return DataFrame(self._jdf.withMetadata(columnName, jmeta), self.sql_ctx)

    @overload
    def drop(self, cols: "ColumnOrName") -> "DataFrame":
        ...

    @overload
    def drop(self, *cols: str) -> "DataFrame":
        ...

    def drop(self, *cols: "ColumnOrName") -> "DataFrame":  # type: ignore[misc]
        """Returns a new :class:`DataFrame` that drops the specified column.
        This is a no-op if schema doesn't contain the given column name(s).

        .. versionadded:: 1.4.0

        Parameters
        ----------
        cols: str or :class:`Column`
            a name of the column, or the :class:`Column` to drop

        Examples
        --------
        >>> df.drop('age').collect()
        [Row(name='Alice'), Row(name='Bob')]

        >>> df.drop(df.age).collect()
        [Row(name='Alice'), Row(name='Bob')]

        >>> df.join(df2, df.name == df2.name, 'inner').drop(df.name).collect()
        [Row(age=5, height=85, name='Bob')]

        >>> df.join(df2, df.name == df2.name, 'inner').drop(df2.name).collect()
        [Row(age=5, name='Bob', height=85)]

        >>> df.join(df2, 'name', 'inner').drop('age', 'height').collect()
        [Row(name='Bob')]
        """
        if len(cols) == 1:
            col = cols[0]
            if isinstance(col, str):
                jdf = self._jdf.drop(col)
            elif isinstance(col, Column):
                jdf = self._jdf.drop(col._jc)
            else:
                raise TypeError("col should be a string or a Column")
        else:
            for col in cols:
                if not isinstance(col, str):
                    raise TypeError("each col in the param list should be a string")
            jdf = self._jdf.drop(self._jseq(cols))

        return DataFrame(jdf, self.sql_ctx)

    def toDF(self, *cols: "ColumnOrName") -> "DataFrame":
        """Returns a new :class:`DataFrame` that with new specified column names

        Parameters
        ----------
        cols : str
            new column names

        Examples
        --------
        >>> df.toDF('f1', 'f2').collect()
        [Row(f1=2, f2='Alice'), Row(f1=5, f2='Bob')]
        """
        jdf = self._jdf.toDF(self._jseq(cols))
        return DataFrame(jdf, self.sql_ctx)

    def transform(self, func: Callable[["DataFrame"], "DataFrame"]) -> "DataFrame":
        """Returns a new :class:`DataFrame`. Concise syntax for chaining custom transformations.

        .. versionadded:: 3.0.0

        Parameters
        ----------
        func : function
            a function that takes and returns a :class:`DataFrame`.

        Examples
        --------
        >>> from pyspark.sql.functions import col
        >>> df = spark.createDataFrame([(1, 1.0), (2, 2.0)], ["int", "float"])
        >>> def cast_all_to_int(input_df):
        ...     return input_df.select([col(col_name).cast("int") for col_name in input_df.columns])
        >>> def sort_columns_asc(input_df):
        ...     return input_df.select(*sorted(input_df.columns))
        >>> df.transform(cast_all_to_int).transform(sort_columns_asc).show()
        +-----+---+
        |float|int|
        +-----+---+
        |    1|  1|
        |    2|  2|
        +-----+---+
        """
        result = func(self)
        assert isinstance(
            result, DataFrame
        ), "Func returned an instance of type [%s], " "should have been DataFrame." % type(result)
        return result

    def sameSemantics(self, other: "DataFrame") -> bool:
        """
        Returns `True` when the logical query plans inside both :class:`DataFrame`\\s are equal and
        therefore return same results.

        .. versionadded:: 3.1.0

        Notes
        -----
        The equality comparison here is simplified by tolerating the cosmetic differences
        such as attribute names.

        This API can compare both :class:`DataFrame`\\s very fast but can still return
        `False` on the :class:`DataFrame` that return the same results, for instance, from
        different plans. Such false negative semantic can be useful when caching as an example.

        This API is a developer API.

        Examples
        --------
        >>> df1 = spark.range(10)
        >>> df2 = spark.range(10)
        >>> df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col1", df2.id * 2))
        True
        >>> df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col1", df2.id + 2))
        False
        >>> df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col0", df2.id * 2))
        True
        """
        if not isinstance(other, DataFrame):
            raise TypeError("other parameter should be of DataFrame; however, got %s" % type(other))
        return self._jdf.sameSemantics(other._jdf)

    def semanticHash(self) -> int:
        """
        Returns a hash code of the logical query plan against this :class:`DataFrame`.

        .. versionadded:: 3.1.0

        Notes
        -----
        Unlike the standard hash code, the hash is calculated against the query plan
        simplified by tolerating the cosmetic differences such as attribute names.

        This API is a developer API.

        Examples
        --------
        >>> spark.range(10).selectExpr("id as col0").semanticHash()  # doctest: +SKIP
        1855039936
        >>> spark.range(10).selectExpr("id as col1").semanticHash()  # doctest: +SKIP
        1855039936
        """
        return self._jdf.semanticHash()

    def inputFiles(self) -> List[str]:
        """
        Returns a best-effort snapshot of the files that compose this :class:`DataFrame`.
        This method simply asks each constituent BaseRelation for its respective files and
        takes the union of all results. Depending on the source relations, this may not find
        all input files. Duplicates are removed.

        .. versionadded:: 3.1.0

        Examples
        --------
        >>> df = spark.read.load("examples/src/main/resources/people.json", format="json")
        >>> len(df.inputFiles())
        1
        """
        return list(self._jdf.inputFiles())

    where = copy_func(filter, sinceversion=1.3, doc=":func:`where` is an alias for :func:`filter`.")

    # Two aliases below were added for pandas compatibility many years ago.
    # There are too many differences compared to pandas and we cannot just
    # make it "compatible" by adding aliases. Therefore, we stop adding such
    # aliases as of Spark 3.0. Two methods below remain just
    # for legacy users currently.
    groupby = copy_func(
        groupBy, sinceversion=1.4, doc=":func:`groupby` is an alias for :func:`groupBy`."
    )

    drop_duplicates = copy_func(
        dropDuplicates,
        sinceversion=1.4,
        doc=":func:`drop_duplicates` is an alias for :func:`dropDuplicates`.",
    )

    def writeTo(self, table: str) -> DataFrameWriterV2:
        """
        Create a write configuration builder for v2 sources.

        This builder is used to configure and execute write operations.

        For example, to append or create or replace existing tables.

        .. versionadded:: 3.1.0

        Examples
        --------
        >>> df.writeTo("catalog.db.table").append()  # doctest: +SKIP
        >>> df.writeTo(                              # doctest: +SKIP
        ...     "catalog.db.table"
        ... ).partitionedBy("col").createOrReplace()
        """
        return DataFrameWriterV2(self, table)

    # Keep to_pandas_on_spark for backward compatibility for now.
    def to_pandas_on_spark(
        self, index_col: Optional[Union[str, List[str]]] = None
    ) -> "PandasOnSparkDataFrame":
        warnings.warn(
            "DataFrame.to_pandas_on_spark is deprecated. Use DataFrame.pandas_api instead.",
            FutureWarning,
        )
        return self.pandas_api(index_col)

    def pandas_api(
        self, index_col: Optional[Union[str, List[str]]] = None
    ) -> "PandasOnSparkDataFrame":
        """
        Converts the existing DataFrame into a pandas-on-Spark DataFrame.

        If a pandas-on-Spark DataFrame is converted to a Spark DataFrame and then back
        to pandas-on-Spark, it will lose the index information and the original index
        will be turned into a normal column.

        This is only available if Pandas is installed and available.

        Parameters
        ----------
        index_col: str or list of str, optional, default: None
            Index column of table in Spark.

        See Also
        --------
        pyspark.pandas.frame.DataFrame.to_spark

        Examples
        --------
        >>> df.show()  # doctest: +SKIP
        +----+----+
        |Col1|Col2|
        +----+----+
        |   a|   1|
        |   b|   2|
        |   c|   3|
        +----+----+

        >>> df.pandas_api()  # doctest: +SKIP
          Col1  Col2
        0    a     1
        1    b     2
        2    c     3

        We can specify the index columns.

        >>> df.pandas_api(index_col="Col1"): # doctest: +SKIP
              Col2
        Col1
        a        1
        b        2
        c        3
        """
        from pyspark.pandas.namespace import _get_index_map
        from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
        from pyspark.pandas.internal import InternalFrame

        index_spark_columns, index_names = _get_index_map(self, index_col)
        internal = InternalFrame(
            spark_frame=self,
            index_spark_columns=index_spark_columns,
            index_names=index_names,  # type: ignore[arg-type]
        )
        return PandasOnSparkDataFrame(internal)

    # Keep to_koalas for backward compatibility for now.
    def to_koalas(
        self, index_col: Optional[Union[str, List[str]]] = None
    ) -> "PandasOnSparkDataFrame":
        return self.pandas_api(index_col)


def _to_scala_map(sc: SparkContext, jm: Dict) -> JavaObject:
    """
    Convert a dict into a JVM Map.
    """
    return sc._jvm.PythonUtils.toScalaMap(jm)  # type: ignore[attr-defined]


class DataFrameNaFunctions(object):
    """Functionality for working with missing data in :class:`DataFrame`.

    .. versionadded:: 1.4
    """

    def __init__(self, df: DataFrame):
        self.df = df

    def drop(
        self, how: str = "any", thresh: Optional[int] = None, subset: Optional[List[str]] = None
    ) -> DataFrame:
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    drop.__doc__ = DataFrame.dropna.__doc__

    @overload
    def fill(self, value: "LiteralType", subset: Optional[List[str]] = ...) -> DataFrame:
        ...

    @overload
    def fill(self, value: Dict[str, "LiteralType"]) -> DataFrame:
        ...

    def fill(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        return self.df.fillna(value=value, subset=subset)  # type: ignore[arg-type]

    fill.__doc__ = DataFrame.fillna.__doc__

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: List["OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> DataFrame:
        ...

    @overload
    def replace(
        self,
        to_replace: Dict["LiteralType", "OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> DataFrame:
        ...

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: "OptionalPrimitiveType",
        subset: Optional[List[str]] = ...,
    ) -> DataFrame:
        ...

    def replace(  # type: ignore[misc]
        self,
        to_replace: Union[List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]],
        value: Optional[Union["OptionalPrimitiveType", List["OptionalPrimitiveType"]]] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        return self.df.replace(to_replace, value, subset)  # type: ignore[arg-type]

    replace.__doc__ = DataFrame.replace.__doc__


class DataFrameStatFunctions(object):
    """Functionality for statistic functions with :class:`DataFrame`.

    .. versionadded:: 1.4
    """

    def __init__(self, df: DataFrame):
        self.df = df

    @overload
    def approxQuantile(
        self,
        col: str,
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[float]:
        ...

    @overload
    def approxQuantile(
        self,
        col: Union[List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[List[float]]:
        ...

    def approxQuantile(  # type: ignore[misc]
        self,
        col: Union[str, List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        return self.df.approxQuantile(col, probabilities, relativeError)

    approxQuantile.__doc__ = DataFrame.approxQuantile.__doc__

    def corr(self, col1: str, col2: str, method: Optional[str] = None) -> float:
        return self.df.corr(col1, col2, method)

    corr.__doc__ = DataFrame.corr.__doc__

    def cov(self, col1: str, col2: str) -> float:
        return self.df.cov(col1, col2)

    cov.__doc__ = DataFrame.cov.__doc__

    def crosstab(self, col1: str, col2: str) -> DataFrame:
        return self.df.crosstab(col1, col2)

    crosstab.__doc__ = DataFrame.crosstab.__doc__

    def freqItems(self, cols: List[str], support: Optional[float] = None) -> DataFrame:
        return self.df.freqItems(cols, support)

    freqItems.__doc__ = DataFrame.freqItems.__doc__

    def sampleBy(
        self, col: str, fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> DataFrame:
        return self.df.sampleBy(col, fractions, seed)

    sampleBy.__doc__ = DataFrame.sampleBy.__doc__


def _test() -> None:
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext, SparkSession
    import pyspark.sql.dataframe

    globs = pyspark.sql.dataframe.__dict__.copy()
    sc = SparkContext("local[4]", "PythonTest")
    globs["sc"] = sc
    globs["sqlContext"] = SQLContext(sc)
    globs["spark"] = SparkSession(sc)
    globs["df"] = sc.parallelize([(2, "Alice"), (5, "Bob")]).toDF(
        StructType([StructField("age", IntegerType()), StructField("name", StringType())])
    )
    globs["df2"] = sc.parallelize([Row(height=80, name="Tom"), Row(height=85, name="Bob")]).toDF()
    globs["df3"] = sc.parallelize([Row(age=2, name="Alice"), Row(age=5, name="Bob")]).toDF()
    globs["df4"] = sc.parallelize(
        [
            Row(age=10, height=80, name="Alice"),
            Row(age=5, height=None, name="Bob"),
            Row(age=None, height=None, name="Tom"),
            Row(age=None, height=None, name=None),
        ]
    ).toDF()
    globs["df5"] = sc.parallelize(
        [
            Row(age=10, name="Alice", spy=False),
            Row(age=5, name="Bob", spy=None),
            Row(age=None, name="Mallory", spy=True),
        ]
    ).toDF()
    globs["sdf"] = sc.parallelize(
        [Row(name="Tom", time=1479441846), Row(name="Bob", time=1479442946)]
    ).toDF()

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.dataframe,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    globs["sc"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
