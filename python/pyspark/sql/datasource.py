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
from collections import UserDict
from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from pyspark.errors import PySparkNotImplementedError
from pyspark.sql import Row
from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadLimit
from pyspark.sql.types import DataType, StructType

if TYPE_CHECKING:
    from pyarrow import RecordBatch

    from pyspark.sql.session import SparkSession

__all__ = [
    "DataSource",
    "DataSourceReader",
    "DataSourceLookupReader",
    "DataSourceStreamReader",
    "DataSourceWriter",
    "DataSourceArrowWriter",
    "DataSourceStreamWriter",
    "DataSourceStreamArrowWriter",
    "SimpleDataSourceStreamReader",
    "DataSourceRegistration",
    "InputPartition",
    "WriterCommitMessage",
    "Filter",
    "EqualTo",
    "EqualNullSafe",
    "GreaterThan",
    "GreaterThanOrEqual",
    "LessThan",
    "LessThanOrEqual",
    "In",
    "IsNull",
    "IsNotNull",
    "Not",
    "StringStartsWith",
    "StringEndsWith",
    "StringContains",
    "LookupParameter",
]


class DataSource(ABC):
    """
    A base class for data sources.

    This class represents a custom data source that allows for reading from and/or
    writing to it. The data source provides methods to create readers and writers
    for reading and writing data, respectively. At least one of the methods
    :meth:`DataSource.reader` or :meth:`DataSource.writer` must be implemented
    by any subclass to make the data source either readable or writable (or both).

    After implementing this interface, you can start to load your data source using
    ``spark.read.format(...).load()`` and save data using ``df.write.format(...).save()``.

    .. versionadded:: 4.0.0
    """

    def __init__(self, options: MutableMapping[str, str]) -> None:
        """
        Initializes the data source with user-provided options.

        Parameters
        ----------
        options : dict
            A case-insensitive dictionary representing the options for this data source.

        Notes
        -----
        This method should not be overridden.
        """
        self.options = options

    @classmethod
    def name(cls) -> str:
        """
        Returns a string represents the format name of this data source.

        By default, it is the class name of the data source. It can be overridden to
        provide a customized short name for the data source.

        Examples
        --------
        >>> def name(cls):
        ...     return "my_data_source"
        """
        return cls.__name__

    def schema(self) -> Union[StructType, str]:
        """
        Returns the schema of the data source.

        It can refer any field initialized in the :meth:`DataSource.__init__` method
        to infer the data source's schema when users do not explicitly specify it.
        This method is invoked once when calling ``spark.read.format(...).load()``
        to get the schema for a data source read operation. If this method is not
        implemented, and a user does not provide a schema when reading the data source,
        an exception will be thrown.

        Returns
        -------
        schema : :class:`StructType` or str
            The schema of this data source or a DDL string represents the schema

        Examples
        --------
        Returns a DDL string:

        >>> def schema(self):
        ...    return "a INT, b STRING"

        Returns a :class:`StructType`:

        >>> def schema(self):
        ...   return StructType().add("a", "int").add("b", "string")
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "schema"},
        )

    def reader(self, schema: StructType) -> "DataSourceReader":
        """
        Returns a :class:`DataSourceReader` instance for reading data.

        The implementation is required for readable data sources.

        Parameters
        ----------
        schema : :class:`StructType`
            The schema of the data to be read.

        Returns
        -------
        reader : :class:`DataSourceReader`
            A reader instance for this data source.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "reader"},
        )

    def writer(self, schema: StructType, overwrite: bool) -> "DataSourceWriter":
        """
        Returns a :class:`DataSourceWriter` instance for writing data.

        The implementation is required for writable data sources.

        Parameters
        ----------
        schema : :class:`StructType`
            The schema of the data to be written.
        overwrite : bool
            A flag indicating whether to overwrite existing data when writing to the data source.

        Returns
        -------
        writer : :class:`DataSourceWriter`
            A writer instance for this data source.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "writer"},
        )

    def streamWriter(self, schema: StructType, overwrite: bool) -> "DataSourceStreamWriter":
        """
        Returns a :class:`DataSourceStreamWriter` instance for writing data into a streaming sink.

        The implementation is required for writable streaming data sources.

        Parameters
        ----------
        schema : :class:`StructType`
            The schema of the data to be written.
        overwrite : bool
            A flag indicating whether to overwrite existing data when writing current microbatch.

        Returns
        -------
        writer : :class:`DataSourceStreamWriter`
            A writer instance for writing data into a streaming sink.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "streamWriter"},
        )

    def simpleStreamReader(self, schema: StructType) -> "SimpleDataSourceStreamReader":
        """
        Returns a :class:`SimpleDataSourceStreamReader` instance for reading data.

        One of simpleStreamReader() and streamReader() must be implemented for readable streaming
        data source. Spark will check whether streamReader() is implemented, if yes, create a
        DataSourceStreamReader to read data. simpleStreamReader() will only be invoked when
        streamReader() is not implemented.

        Parameters
        ----------
        schema : :class:`StructType`
            The schema of the data to be read.

        Returns
        -------
        reader : :class:`SimpleDataSourceStreamReader`
            A reader instance for this data source.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "simpleStreamReader"},
        )

    def streamReader(self, schema: StructType) -> "DataSourceStreamReader":
        """
        Returns a :class:`DataSourceStreamReader` instance for reading streaming data.

        One of simpleStreamReader() and streamReader() must be implemented for readable streaming
        data source.

        Parameters
        ----------
        schema : :class:`StructType`
            The schema of the data to be read.

        Returns
        -------
        reader : :class:`DataSourceStreamReader`
            A reader instance for this streaming data source.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "streamReader"},
        )

    def lookupReader(self, schema: StructType) -> "DataSourceLookupReader":
        """
        Returns a :class:`DataSourceLookupReader` instance that serves lookup joins on this
        data source.

        The implementation is required for data sources that Spark reads by lookups, as the
        static side of a stream-static join. For each batch of stream rows, Spark then sends
        the join keys to the reader and joins the rows that it returns, instead of reading the
        whole data source.

        Spark detects lookup support by whether the data source class overrides this method:

        - A data source that overrides both :meth:`reader` and this method serves batch reads
          and lookups. Spark reads it by lookups only in a join whose lookup side carries the
          `EXTERNAL_LOOKUP` hint, such as ``df.hint("EXTERNAL_LOOKUP")``, and by batch reads
          otherwise.
        - A data source that overrides this method but not :meth:`reader` is lookup-only.
          Spark reads it by lookups in every supported stream-static join, with no hint, and
          rejects a batch read of it.

        A supported stream-static join has the stream on the left and this data source on the
        right, and is an inner, left outer, left semi or left anti join. A join of another
        shape over a lookup side fails rather than falling back to a batch read.

        Spark calls this method on the driver when it plans a lookup in Python, and plans the
        new reader with :meth:`DataSourceLookupReader.pushFilters`,
        :meth:`DataSourceLookupReader.selectLookupPredicates` and
        :meth:`DataSourceLookupReader.maxParameterRowsPerRequest`. The plan made when a
        streaming query starts, with its reader, can serve its later micro-batches; see
        :class:`DataSourceLookupReader` for when Spark reuses a plan.

        Spark pushes the filters into the scan of a lookup before it tells the scan that it serves
        a lookup. So for a data source that also implements :meth:`reader`, while
        `spark.sql.python.filterPushdown.enabled` is true and there is a filter to push, a planning
        of a lookup first plans a batch read, as for any batch read: Spark creates a batch reader
        with :meth:`reader`, calls its :meth:`DataSourceReader.pushFilters` and, while
        `spark.sql.python.limitPushdown.enabled` is false, its :meth:`DataSourceReader.partitions`,
        and then discards that plan.

        .. versionadded:: 4.5.0

        Parameters
        ----------
        schema : :class:`StructType`
            The schema of the data source: the result of :meth:`schema`, or the schema that the
            user gave.

        Returns
        -------
        reader : :class:`DataSourceLookupReader`
            A lookup reader instance for this data source.

        Notes
        -----
        A result of `None` fails the query with
        `DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_RESULT`, and a result of another type with
        `DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_RESULT_TYPE`. A streaming query stops on
        either when it starts. An exception fails the query with `PYTHON_DATA_SOURCE_ERROR`.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "lookupReader"},
        )


ColumnPath = Tuple[str, ...]
"""
A tuple of strings representing a column reference.

For example, `("a", "b", "c")` represents the column `a.b.c`.

.. versionadded:: 4.1.0
"""


@dataclass(frozen=True)
class Filter(ABC):
    """
    The base class for filters used for filter pushdown, and for the candidate predicates
    that :meth:`DataSourceLookupReader.selectLookupPredicates` receives. In a candidate, the
    value is a :class:`LookupParameter` rather than a literal.

    .. versionadded:: 4.1.0

    Notes
    -----
    Column references are represented as a tuple of strings. For example:

    +----------------+----------------------+
    | Column         | Representation       |
    +----------------+----------------------+
    | `col1`         | `("col1",)`          |
    | `a.b.c`        | `("a", "b", "c")`    |
    +----------------+----------------------+

    Literal values are represented as Python objects of types such as
    `int`, `float`, `str`, `bool`, `datetime`, etc.
    See `Data Types <https://spark.apache.org/docs/latest/sql-ref-datatypes.html>`_
    for more information about how values are represented in Python.

    Examples
    --------
    Supported filters

    +---------------------+--------------------------------------------+
    | SQL filter          | Representation                             |
    +---------------------+--------------------------------------------+
    | `a.b.c = 1`         | `EqualTo(("a", "b", "c"), 1)`              |
    | `a = 1`             | `EqualTo(("a",), 1)`                       |
    | `a = 'hi'`          | `EqualTo(("a",), "hi")`                    |
    | `a = array(1, 2)`   | `EqualTo(("a",), [1, 2])`                  |
    | `a`                 | `EqualTo(("a",), True)`                    |
    | `not a`             | `Not(EqualTo(("a",), True))`               |
    | `a <> 1`            | `Not(EqualTo(("a",), 1))`                  |
    | `a > 1`             | `GreaterThan(("a",), 1)`                   |
    | `a >= 1`            | `GreaterThanOrEqual(("a",), 1)`            |
    | `a < 1`             | `LessThan(("a",), 1)`                      |
    | `a <= 1`            | `LessThanOrEqual(("a",), 1)`               |
    | `a in (1, 2, 3)`    | `In(("a",), (1, 2, 3))`                    |
    | `a is null`         | `IsNull(("a",))`                           |
    | `a is not null`     | `IsNotNull(("a",))`                        |
    | `a like 'abc%'`     | `StringStartsWith(("a",), "abc")`          |
    | `a like '%abc'`     | `StringEndsWith(("a",), "abc")`            |
    | `a like '%abc%'`    | `StringContains(("a",), "abc")`            |
    +---------------------+--------------------------------------------+

    Unsupported filters
    - `a = b`
    - `f(a, b) = 1`
    - `a % 2 = 1`
    - `a[0] = 1`
    - `a < 0 or a > 1`
    - `a like 'c%c%'`
    - `a ilike 'hi'`
    - `a = 'hi' collate zh`
    """


@dataclass(frozen=True)
class EqualTo(Filter):
    """
    A filter that evaluates to `True` iff the column evaluates to a value
    equal to `value`.
    """

    attribute: ColumnPath
    value: Any


@dataclass(frozen=True)
class EqualNullSafe(Filter):
    """
    Performs equality comparison, similar to EqualTo. However, this differs from EqualTo
    in that it returns `true` (rather than NULL) if both inputs are NULL, and `false`
    (rather than NULL) if one of the input is NULL and the other is not NULL.
    """

    attribute: ColumnPath
    value: Any


@dataclass(frozen=True)
class GreaterThan(Filter):
    """
    A filter that evaluates to `True` iff the attribute evaluates to a value
    greater than `value`.
    """

    attribute: ColumnPath
    value: Any


@dataclass(frozen=True)
class GreaterThanOrEqual(Filter):
    """
    A filter that evaluates to `True` iff the attribute evaluates to a value
    greater than or equal to `value`.
    """

    attribute: ColumnPath
    value: Any


@dataclass(frozen=True)
class LessThan(Filter):
    """
    A filter that evaluates to `True` iff the attribute evaluates to a value
    less than `value`.
    """

    attribute: ColumnPath
    value: Any


@dataclass(frozen=True)
class LessThanOrEqual(Filter):
    """
    A filter that evaluates to `True` iff the attribute evaluates to a value
    less than or equal to `value`.
    """

    attribute: ColumnPath
    value: Any


@dataclass(frozen=True)
class In(Filter):
    """
    A filter that evaluates to `True` iff the attribute evaluates to one of the values
    in the array.
    """

    attribute: ColumnPath
    value: Tuple[Any, ...]


@dataclass(frozen=True)
class IsNull(Filter):
    """
    A filter that evaluates to `True` iff the attribute evaluates to null.
    """

    attribute: ColumnPath


@dataclass(frozen=True)
class IsNotNull(Filter):
    """
    A filter that evaluates to `True` iff the attribute evaluates to a non-null value.
    """

    attribute: ColumnPath


@dataclass(frozen=True)
class Not(Filter):
    """
    A filter that evaluates to `True` iff `child` is evaluated to `False`.
    """

    child: Filter


@dataclass(frozen=True)
class StringStartsWith(Filter):
    """
    A filter that evaluates to `True` iff the attribute evaluates to
    a string that starts with `value`.
    """

    attribute: ColumnPath
    value: str


@dataclass(frozen=True)
class StringEndsWith(Filter):
    """
    A filter that evaluates to `True` iff the attribute evaluates to
    a string that ends with `value`.
    """

    attribute: ColumnPath
    value: str


@dataclass(frozen=True)
class StringContains(Filter):
    """
    A filter that evaluates to `True` iff the attribute evaluates to
    a string that contains the string `value`.
    """

    attribute: ColumnPath
    value: str


@dataclass(frozen=True, eq=False)
class LookupParameter:
    """
    A value that Spark binds at run time in a candidate lookup predicate, such as the `?` in
    `tenant = ?`. Its values arrive in the parameter rows of
    :meth:`DataSourceLookupReader.lookup`.

    Each candidate predicate has its own instance, which equals only itself. So two candidates
    of the same shape, such as two `id = ?` from `t.id = s.a AND t.id = s.b`, are not equal,
    and `candidates.index(candidate)` returns the position of `candidate`.

    Instances are read-only and picklable, so a reader may keep the selected candidates, or
    their parameters, for :meth:`DataSourceLookupReader.lookup`.

    .. versionadded:: 4.5.0

    Attributes
    ----------
    dataType : :class:`DataType`
        The Spark SQL data type of the value. Its values arrive in the Python type that
        :meth:`DataSourceLookupReader.lookup` lists for that data type.
    """

    dataType: DataType


class InputPartition:
    """
    A base class representing an input partition returned by the `partitions()`
    method of :class:`DataSourceReader`.

    .. versionadded:: 4.0.0

    Notes
    -----
    This class must be picklable.

    Examples
    --------
    Use the default input partition implementation:

    >>> def partitions(self):
    ...     return [InputPartition(1)]

    Subclass the input partition class:

    >>> from dataclasses import dataclass
    >>> @dataclass
    ... class RangeInputPartition(InputPartition):
    ...     start: int
    ...     end: int

    >>> def partitions(self):
    ...     return [RangeInputPartition(1, 3), RangeInputPartition(4, 6)]
    """

    def __init__(self, value: Any) -> None:
        self.value = value

    def __repr__(self) -> str:
        attributes = ", ".join([f"{k}={v!r}" for k, v in self.__dict__.items()])
        return f"{self.__class__.__name__}({attributes})"


class DataSourceReader(ABC):
    """
    A base class for data source readers. Data source readers are responsible for
    outputting data from a data source.

    .. versionadded:: 4.0.0
    """

    def pushFilters(self, filters: List["Filter"]) -> Iterable["Filter"]:
        """
        Called with the list of filters that can be pushed down to the data source.

        The list of filters should be interpreted as the AND of the elements.

        Filter pushdown allows the data source to handle a subset of filters. This
        can improve performance by reducing the amount of data that needs to be
        processed by Spark.

        This method may be called more than once during query planning, on separate reader
        instances (see the note below). By default, it returns all filters, indicating that no
        filters can be pushed down. Subclasses can override this method to implement filter
        pushdown.

        .. note::
            When limit pushdown is enabled (see :meth:`pushLimit`), planning may create
            additional reader instances and call this method on each with the same filters, so
            that a reader reaches the same state before a limit is pushed and read planning runs
            only after the limit is known. Implementations must therefore be deterministic --
            returning a different set of supported filters across calls fails the query -- and
            must not rely on side effects outside of `self`.

        It's recommended to implement this method only for data sources that natively
        support filtering, such as databases and GraphQL APIs.

        .. versionadded:: 4.1.0

        Parameters
        ----------
        filters : list of :class:`Filter`\\s

        Returns
        -------
        iterable of :class:`Filter`\\s
            Filters that still need to be evaluated by Spark post the data source
            scan. This includes unsupported filters and partially pushed filters.
            Every returned filter must be one of the input filters by reference.

        Side effects
        ------------
        This method is allowed to modify `self`. The object must remain picklable.
        Modifications to `self` are visible to the `partitions()` and `read()` methods.

        Examples
        --------
        Example filters and the resulting arguments passed to pushFilters:

        +-------------------------------+---------------------------------------------+
        | Filters                       | Pushdown Arguments                          |
        +-------------------------------+---------------------------------------------+
        | `a = 1 and b = 2`             | `[EqualTo(("a",), 1), EqualTo(("b",), 2)]`  |
        | `a = 1 or b = 2`              | `[]`                                        |
        | `a = 1 or (b = 2 and c = 3)`  | `[]`                                        |
        | `a = 1 and (b = 2 or c = 3)`  | `[EqualTo(("a",), 1)]`                      |
        +-------------------------------+---------------------------------------------+

        Implement pushFilters to support EqualTo filters only:

        >>> def pushFilters(self, filters):
        ...     for filter in filters:
        ...         if isinstance(filter, EqualTo):
        ...             # Save supported filter for handling in partitions() and read()
        ...             self.filters.append(filter)
        ...         else:
        ...             # Unsupported filter
        ...             yield filter
        """
        return filters

    def pushLimit(self, limit: int) -> bool:
        """
        Called with the maximum number of rows that the query needs from this data source.

        Limit pushdown allows the data source to fetch less data, for example by adding a
        `LIMIT` clause to a SQL query or a page size parameter to a REST request.

        This method is called once during query planning, before :meth:`partitions` and
        :meth:`read`. By default, it returns False, indicating that the limit cannot be
        pushed down. Subclasses can override this method to implement limit pushdown.

        :meth:`pushFilters` is called before this method only when the query has filters that
        Spark can push down; for a query without them, :meth:`pushFilters` is not called at
        all. This method may use state that :meth:`pushFilters` set when filters were pushed,
        but must not assume :meth:`pushFilters` ran: initialize defaults in `__init__` so this
        method works whether or not it did.

        A limit is only pushed down when every filter was pushed down, because Spark cannot
        apply a limit before a filter it still has to evaluate itself. To benefit from limit
        pushdown alongside filters, :meth:`pushFilters` should return an empty iterable.

        Pushing down a limit is only a hint: Spark always applies the limit again after the
        scan, so it is safe to return True even if `read()` yields more than `limit` rows.
        Returning True never causes the query to see fewer rows than it requires.

        .. versionadded:: 4.4.0

        Parameters
        ----------
        limit : int
            The maximum number of rows the query needs. Always positive: `LIMIT 0` is
            optimized into an empty relation and never reaches the data source.

        Returns
        -------
        bool
            True if the data source will use the limit to reduce the amount of data it
            reads, False otherwise.

        Side effects
        ------------
        This method is allowed to modify `self`. The object must remain picklable.
        Modifications to `self` are visible to the `partitions()` and `read()` methods
        only when this method returns True. When it returns False, planning proceeds as if
        `pushLimit` was never called -- `partitions()` and `read()` run on a reader that did
        not observe these modifications -- so any state they rely on must be initialized in
        `__init__` instead.

        Notes
        -----
        This method is only called when the configuration
        `spark.sql.python.limitPushdown.enabled` is set to true.

        Examples
        --------
        Implement pushLimit to fetch fewer rows from the data source. Initialize the limit in
        `__init__`, because :meth:`partitions` and :meth:`read` may run even when `pushLimit`
        was not called -- for a query without a limit, or when this method returned False:

        >>> class MyReader(DataSourceReader):
        ...     def __init__(self):
        ...         self.limit = None
        ...
        ...     def pushLimit(self, limit):
        ...         # Save the limit for handling in partitions() and read().
        ...         self.limit = limit
        ...         return True
        ...
        ...     def partitions(self):
        ...         # A limit can reduce the number of partitions, since every partition opens
        ...         # its own connection to the data source.
        ...         if self.limit is not None:
        ...             return [InputPartition(None)]
        ...         return [InputPartition(i) for i in range(16)]
        """
        return False

    def partitions(self) -> Sequence[InputPartition]:
        """
        Returns an iterator of partitions for this data source.

        Partitions are used to split data reading operations into parallel tasks.
        If this method returns N partitions, the query planner will create N tasks.
        Each task will execute :meth:`DataSourceReader.read` in parallel, using the respective
        partition value to read the data.

        This method is called once during query planning. By default, it returns a
        single partition with the value `InputPartition(None)`. Subclasses can override
        this method to return multiple partitions.

        It's recommended to override this method for better performance when reading
        large datasets.

        Returns
        -------
        sequence of :class:`InputPartition`\\s
            A sequence of partitions for this data source. Each partition value
            must be an instance of `InputPartition` or a subclass of it.

        Notes
        -----
        All partition values must be picklable objects.

        Examples
        --------
        Returns a list of integers:

        >>> def partitions(self):
        ...     return [InputPartition(1), InputPartition(2), InputPartition(3)]

        Returns a list of string:

        >>> def partitions(self):
        ...     return [InputPartition("a"), InputPartition("b"), InputPartition("c")]

        Returns a list of ranges:

        >>> class RangeInputPartition(InputPartition):
        ...    def __init__(self, start, end):
        ...        self.start = start
        ...        self.end = end

        >>> def partitions(self):
        ...     return [RangeInputPartition(1, 3), RangeInputPartition(5, 10)]
        """
        return [InputPartition(None)]

    @abstractmethod
    def read(self, partition: InputPartition) -> Union[Iterator[Tuple], Iterator["RecordBatch"]]:
        """
        Generates data for a given partition and returns an iterator of tuples or rows.

        This method is invoked once per partition to read the data. Implementing
        this method is required for readable data sources. You can initialize any
        non-serializable resources required for reading data from the data source
        within this method.

        Parameters
        ----------
        partition : InputPartition
            The partition to read. It must be one of the partition values returned by
            :meth:`DataSourceReader.partitions`.

        Returns
        -------
        iterator of tuples or PyArrow's `RecordBatch`
            An iterator of tuples or rows. Each tuple or row will be converted to a row
            in the final DataFrame.
            It can also return an iterator of PyArrow's `RecordBatch` if the data source
            supports it.

        Examples
        --------
        Yields a list of tuples:

        >>> def read(self, partition: InputPartition):
        ...     yield (partition.value, 0)
        ...     yield (partition.value, 1)

        Yields a list of rows:

        >>> def read(self, partition: InputPartition):
        ...     yield Row(partition=partition.value, value=0)
        ...     yield Row(partition=partition.value, value=1)

        Yields PyArrow RecordBatches:

        >>> def read(self, partition: InputPartition):
        ...     import pyarrow as pa
        ...     data = {
        ...         "partition": [partition.value] * 2,
        ...         "value": [0, 1]
        ...     }
        ...     table = pa.Table.from_pydict(data)
        ...     for batch in table.to_batches():
        ...         yield batch
        """
        ...


class DataSourceStreamReader(ABC):
    """
    A base class for streaming data source readers. Data source stream readers are responsible
    for outputting data from a streaming data source.

    .. versionadded:: 4.0.0
    """

    def initialOffset(self) -> dict:
        """
        Return the initial offset of the streaming data source.
        A new streaming query starts reading data from the initial offset.
        If Spark is restarting an existing query, it will restart from the check-pointed offset
        rather than the initial one.

        Returns
        -------
        dict
            A dict or recursive dict whose key and value are primitive types, which includes
            Integer, String and Boolean.

        Examples
        --------
        >>> def initialOffset(self):
        ...     return {"parititon-1": {"index": 3, "closed": True}, "partition-2": {"index": 5}}
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "initialOffset"},
        )

    def latestOffset(self, start: dict, limit: ReadLimit) -> dict:
        """
        Returns the most recent offset available given a read limit. The start offset can be used
        to figure out how much new data should be read given the limit.

        The `start` will be provided from the return value of :meth:`initialOffset()` for
        the very first micro-batch, and for subsequent micro-batches, the start offset is the
        ending offset from the previous micro-batch. The source can return the `start` parameter
        as it is, if there is no data to process.

        :class:`ReadLimit` can be used by the source to limit the amount of data returned in this
        call. The implementation should implement :meth:`getDefaultReadLimit()` to provide the
        proper :class:`ReadLimit` if the source can limit the amount of data returned based on the
        source options.

        The engine can still call :meth:`latestOffset()` with :class:`ReadAllAvailable` even if the
        source produces the different read limit from :meth:`getDefaultReadLimit()`, to respect the
        semantic of trigger. The source must always respect the given readLimit provided by the
        engine; e.g. if the readLimit is :class:`ReadAllAvailable`, the source must ignore the read
        limit configured through options.

        .. versionadded:: 4.2.0

        Parameters
        ----------
        start : dict
            The start offset of the microbatch to continue reading from.
        limit : :class:`ReadLimit`
            The limit on the amount of data to be returned by this call.

        Returns
        -------
        dict
            A dict or recursive dict whose key and value are primitive types, which includes
            Integer, String and Boolean.

        Examples
        --------
        >>> from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadMaxRows
        >>> def latestOffset(self, start, limit):
        ...     # Assume the source has 10 new records between start and latest offset
        ...     if isinstance(limit, ReadAllAvailable):
        ...        return {"index": start["index"] + 10}
        ...     else:  # e.g., limit is ReadMaxRows(5)
        ...        return {"index": start["index"] + min(10, limit.maxRows)}
        """
        # NOTE: Previous Spark versions didn't have start offset and read limit parameters for this
        # method. While Spark will ensure the backward compatibility for existing data sources, the
        # new data sources are strongly encouraged to implement this new method signature.
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "latestOffset"},
        )

    def getDefaultReadLimit(self) -> ReadLimit:
        """
        Returns the read limits potentially passed to the data source through options when creating
        the data source. See the built-in implementations of :class:`ReadLimit` for available read
        limits.

        Implementing this method is optional. By default, it returns :class:`ReadAllAvailable`,
        which means there is no limit on the amount of data returned by :meth:`latestOffset()`.

        .. versionadded:: 4.2.0
        """
        return ReadAllAvailable()

    def reportLatestOffset(self) -> Optional[dict]:
        """
        Returns the most recent offset available. The information is used to report the latest
        offset in the streaming query status.
        The source can return `None`, if there is no data to process or the source does not support
        to this method.

        .. versionadded:: 4.2.0

        Returns
        -------
        dict or None
            A dict or recursive dict whose key and value are primitive types, which includes
            Integer, String and Boolean.
            Returns `None` if the source does not support reporting latest offset.

        Examples
        --------
        >>> def reportLatestOffset(self):
        ...     return {"partition-1": {"index": 100}, "partition-2": {"index": 200}}
        """
        return None

    def partitions(self, start: dict, end: dict) -> Sequence[InputPartition]:
        """
        Returns a list of InputPartition given the start and end offsets. Each InputPartition
        represents a data split that can be processed by one Spark task. This may be called with
        an empty offset range when start == end, in that case the method should return
        an empty sequence of InputPartition.

        Parameters
        ----------
        start : dict
            The start offset of the microbatch to plan partitioning.
        end : dict
            The end offset of the microbatch to plan partitioning.

        Returns
        -------
        sequence of :class:`InputPartition`\\s
            A sequence of partitions for this data source. Each partition value
            must be an instance of `InputPartition` or a subclass of it.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "partitions"},
        )

    @abstractmethod
    def read(self, partition: InputPartition) -> Union[Iterator[Tuple], Iterator["RecordBatch"]]:
        """
        Generates data for a given partition and returns an iterator of tuples or rows.

        This method is invoked once per partition to read the data. Implementing
        this method is required for stream reader. You can initialize any
        non-serializable resources required for reading data from the data source
        within this method.

        Notes
        -----
        This method is static and stateless. You shouldn't access mutable class member
        or keep in memory state between different invocations of read().

        Parameters
        ----------
        partition : :class:`InputPartition`
            The partition to read. It must be one of the partition values returned by
            :meth:`DataSourceStreamReader.partitions`.

        Returns
        -------
        iterator of tuples or PyArrow's `RecordBatch`
            An iterator of tuples or rows. Each tuple or row will be converted to a row
            in the final DataFrame.
            It can also return an iterator of PyArrow's `RecordBatch` if the data source
            supports it.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "read"},
        )

    def commit(self, end: dict) -> None:
        """
        Informs the source that Spark has completed processing all data for offsets less than or
        equal to `end` and will only request offsets greater than `end` in the future.

        Parameters
        ----------
        end : dict
            The latest offset that the streaming query has processed for this source.
        """
        ...

    def stop(self) -> None:
        """
        Stop this source and free any resources it has allocated.
        Invoked when the streaming query terminated.
        """
        ...


class SimpleDataSourceStreamReader(ABC):
    """
    A base class for simplified streaming data source readers.
    Compared to :class:`DataSourceStreamReader`, :class:`SimpleDataSourceStreamReader` doesn't
    require planning data partition. Also, the read api of :class:`SimpleDataSourceStreamReader`
    allows reading data and planning the latest offset at the same time.

    Because  :class:`SimpleDataSourceStreamReader` read records in Spark driver node to determine
    end offset of each batch without partitioning, it is only supposed to be used in
    lightweight use cases where input rate and batch size is small.
    Use :class:`DataSourceStreamReader` when read throughput is high and can't be handled
    by a single process.

    .. versionadded:: 4.0.0
    """

    def initialOffset(self) -> dict:
        """
        Return the initial offset of the streaming data source.
        A new streaming query starts reading data from the initial offset.
        If Spark is restarting an existing query, it will restart from the check-pointed offset
        rather than the initial one.

        Returns
        -------
        dict
            A dict or recursive dict whose key and value are primitive types, which includes
            Integer, String and Boolean.

        Examples
        --------
        >>> def initialOffset(self):
        ...     return {"parititon-1": {"index": 3, "closed": True}, "partition-2": {"index": 5}}
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "initialOffset"},
        )

    def read(self, start: dict) -> Tuple[Iterator[Tuple], dict]:
        """
        Read all available data from start offset and return the offset that next read attempt
        starts from.

        Parameters
        ----------
        start : dict
            The start offset to start reading from.

        Returns
        -------
        A :class:`Tuple` of an iterator of :class:`Tuple` and a dict\\s
            The iterator contains all the available records after start offset.
            The dict is the end offset of this read attempt and the start of next read attempt.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "read"},
        )

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[Tuple]:
        """
        Read all available data from specific start offset and end offset.
        This is invoked during failure recovery to re-read a batch deterministically.

        Parameters
        ----------
        start : dict
            The start offset to start reading from.

        end : dict
            The offset where the reading stop.

        Returns
        -------
        iterator of :class:`Tuple`\\s
            All the records between start offset and end offset.
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "readBetweenOffsets"},
        )

    def commit(self, end: dict) -> None:
        """
        Informs the source that Spark has completed processing all data for offsets less than or
        equal to `end` and will only request offsets greater than `end` in the future.

        Parameters
        ----------
        end : dict
            The latest offset that the streaming query has processed for this source.
        """
        ...


class DataSourceLookupReader(ABC):
    """
    A base class for data source lookup readers. A lookup reader serves lookup joins: for each
    batch of stream rows, Spark sends the join keys to it and joins the rows that it returns.

    When Spark plans a lookup in Python, on the driver, it creates a reader with
    :meth:`DataSource.lookupReader` and calls these methods on it, each at most once, in this
    order: :meth:`pushFilters`, only while Python filter pushdown is on and there is a filter to
    push; :meth:`selectLookupPredicates`; and :meth:`maxParameterRowsPerRequest`, only after a
    selection that Spark accepts. A plan, with its reader, can serve several plannings, also of
    several queries; see below for when Spark reuses it. Spark then pickles the planned reader and, on the
    executors, calls :meth:`lookup` and :meth:`close` on a copy of it per input partition. So
    the reader must stay picklable, and changes that the planning methods make to `self` are
    visible to :meth:`lookup`. Create connections and other resources lazily, in the first
    :meth:`lookup` call, not in `__init__` or in a planning method.

    A streaming query plans its lookups when it starts and in every micro-batch. Spark keeps a
    plan, with its reader, on the data source of the loaded DataFrame, and reuses it for every
    planning with the same inputs: the start and the later micro-batches of a query, and also
    another query over the same DataFrame, such as one started again after a stop. It plans in
    Python again each time only when the planning pushes filters into the reader: while
    `spark.sql.python.filterPushdown.enabled` is true and the lookup side has a filter that Spark
    passes to Python. Spark may still plan again at any time, for example after a change of a
    pushdown configuration. So do not rely on how often the planning methods run, and get
    resources that expire, such as a credential, in :meth:`lookup`. In real-time mode, a query
    runs lookup joins with Python data sources only when
    `spark.sql.python.externalLookup.realTimeMode.enabled` is true.

    For each parameter row, :meth:`lookup` must return every stored row that matches the
    selected predicates and passes the filters that :meth:`pushFilters` reports as pushed,
    under Spark semantics, with no cap on the number of rows and no row lost to paging. Extra
    rows are fine: Spark evaluates every selected predicate and every lookup-side filter again
    on each returned row, and drops the rows that fail them. Spark cannot recover a missing row.

    The result of a lookup must depend only on its parameter row. Within a task, Spark may serve
    a parameter row that it has already requested from its own cache, so the task may not see a
    later change in the source. A query turns the cache off with
    `spark.sql.join.externalLookup.cache.enabled` or the hint option `cache_enabled=false`.
    Spark may send a request again when it retries a task. Requests are not snapshot
    boundaries: results may mix versions of the source. Any exception that the reader raises
    fails the task; a failure is never taken as a miss.

    A reader reads a file that the session ships, with :meth:`SparkContext.addFile`,
    `--files`, or ``spark.addArtifacts(path, file=True)`` in Spark Connect, through
    :meth:`SparkFiles.get` in :meth:`lookup`.

    .. versionadded:: 4.5.0

    Examples
    --------
    A lookup reader over a dictionary, keyed by the `id` column:

    >>> class DictLookupReader(DataSourceLookupReader):
    ...     def __init__(self, rows):
    ...         self.rows = rows  # {id: (id, name)}
    ...
    ...     def selectLookupPredicates(self, candidates):
    ...         return [i for i, c in enumerate(candidates)
    ...                 if isinstance(c, EqualTo) and c.attribute == ("id",)][:1]
    ...
    ...     def lookup(self, parameters):
    ...         for ordinal, (key,) in enumerate(parameters):
    ...             if key in self.rows:
    ...                 yield ordinal, self.rows[key]
    """

    def pushFilters(self, filters: List["Filter"]) -> Iterable["Filter"]:
        """
        Called with the filters on lookup-side columns that Spark can translate, such as
        `t.tier >= 2`, before :meth:`selectLookupPredicates`. The filters carry literal values;
        compare :meth:`selectLookupPredicates`, whose candidates carry a
        :class:`LookupParameter`.

        The list of filters should be interpreted as the AND of the elements. The filters are
        the same classes that :meth:`DataSourceReader.pushFilters` receives. Spark calls this
        method only when `spark.sql.python.filterPushdown.enabled` is true, and only when there
        is a filter to push, at most once on each reader.

        By default, it returns all filters, indicating that no filters are pushed down.

        Unlike a batch read, Spark evaluates every lookup-side filter again on the returned
        rows, also the filters that this method reports as pushed. A pushed filter only lets the
        reader omit the rows that fail it.

        Parameters
        ----------
        filters : list of :class:`Filter`\\s

        Returns
        -------
        iterable of :class:`Filter`\\s
            Filters that the reader does not apply. Every returned filter must be one of the
            input filters by reference.

        Side effects
        ------------
        This method is allowed to modify `self`. The object must remain picklable.
        Modifications to `self` are visible to the :meth:`lookup` method.

        Notes
        -----
        A reader that overrides this method while `spark.sql.python.filterPushdown.enabled` is
        false fails the query with `DATA_SOURCE_PUSHDOWN_DISABLED`, and a returned filter that
        is not one of the input filters with `DATA_SOURCE_EXTRANEOUS_FILTERS`. Both, and any
        exception, are reported as `PYTHON_DATA_SOURCE_ERROR`.
        """
        return filters

    @abstractmethod
    def selectLookupPredicates(self, candidates: List["Filter"]) -> Sequence[int]:
        """
        Selects the candidate predicates that this reader serves as one lookup access path, and
        returns their positions in `candidates`.

        Each candidate is an :class:`EqualTo`, :class:`EqualNullSafe`, :class:`GreaterThan`,
        :class:`GreaterThanOrEqual`, :class:`LessThan` or :class:`LessThanOrEqual` filter whose
        `attribute` is a top-level column of the lookup side, such as `("id",)`, and whose
        `value` is a :class:`LookupParameter`. The list is never empty. When the query writes
        the lookup column on the right, Spark flips the operator, so `s.ts < t.event_time`
        arrives as `GreaterThan(("event_time",), LookupParameter(...))`.

        Spark offers `=` and `<=>` only on columns of an integral type, BOOLEAN, DECIMAL, DATE,
        TIMESTAMP, TIMESTAMP_NTZ, BINARY, or a STRING or VARCHAR whose collation compares
        bytes, such as UTF8_BINARY. It offers `<`, `<=`, `>` and `>=` only on columns of an
        integral type, DECIMAL, DATE, TIMESTAMP or TIMESTAMP_NTZ, and only when the parameter
        cannot fail to evaluate. It offers no predicate on VARCHAR under
        `spark.sql.charVarchar.standardSemantics.enabled`, and never one on FLOAT, DOUBLE,
        CHAR, a string with another collation or a complex type. It also offers a comparison of
        a column with a constant from a filter, such as the constant tenant of a composite key,
        whether or not :meth:`pushFilters` accepted it, under the same rules: an equality on the
        columns above, and a range only when the constant cannot fail to evaluate.

        Selecting a range predicate whose value reads a stream column makes every request hold
        exactly one parameter row, and turns off Spark's lookup cache for that join. Leave such
        a range unselected when the equality predicates alone serve the lookup well; Spark still
        applies it to the returned rows.

        The rules of the selection:

        - The positions must be distinct, each an `int` from 0 to ``len(candidates) - 1``. Two
          candidates of the same shape, such as two `id = ?`, are told apart by position.
        - The order of the positions defines the parameter rows: field `i` of every parameter
          row that :meth:`lookup` receives is the value for the candidate at the `i`-th returned
          position. A column may appear in several selected predicates, as range bounds do.
        - Spark evaluates every selected predicate again on each returned row and drops a row
          that fails it, so extra rows are fine. For each parameter row, :meth:`lookup` must
          return every stored row that matches the selected predicates and passes the pushed
          filters, under Spark semantics. Do not select a predicate that the reader cannot
          evaluate that way.
        - An empty result means that no access path fits; the query then fails.
        - `candidates` and its elements are read-only. The positions refer to the offered
          order, so Spark fails the query if, when this method returns a non-empty result, the
          list no longer holds the offered candidates in the offered order.

        Spark calls this method once on each reader, after :meth:`pushFilters`.

        Parameters
        ----------
        candidates : list of :class:`Filter`\\s
            The candidate predicates, each with a :class:`LookupParameter` value.

        Returns
        -------
        sequence of int
            The positions in `candidates` of the selected predicates, in the order of the fields
            of a parameter row.

        Side effects
        ------------
        This method is allowed to modify `self`. The object must remain picklable.
        Modifications to `self` are visible to the :meth:`lookup` method.

        Notes
        -----
        Spark fails the query with `DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_RESULT` for a
        result of `None`, `INVALID_RESULT_TYPE` for a result that is not a sequence of `int`\\s,
        such as one that holds a candidate or a `bool`, `INVALID_SELECTED_PREDICATE_POSITION`
        for a position out of range, `DUPLICATE_SELECTED_PREDICATE` for a position that
        repeats, and `MODIFIED_CANDIDATES` for a changed list of candidates. It fails with
        `EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE` for an empty result. A streaming
        query stops on each of them when it starts. An exception fails the query with
        `PYTHON_DATA_SOURCE_ERROR`.

        Examples
        --------
        :meth:`pushFilters` receives filters with literal values and returns the ones that the
        reader leaves to Spark. This method receives candidates with :class:`LookupParameter`
        values and returns positions, whose order defines the parameter rows. For example, for
        this join of a stream `s` with a lookup source `t` whose `tier` is an INT, a reader may
        answer as follows, where Spark may also pass filters that it infers, such as `IsNotNull`:

        .. code-block:: text

            s JOIN t ON t.tenant = s.tenant
                AND t.event_time >= s.start AND t.event_time < s.end
                AND t.region = s.region
            WHERE t.tier >= 2

            pushFilters([tier >= 2])                     -> []  (the reader applies it)
            selectLookupPredicates([tenant = ?,          -> [1, 0, 2]
                                    event_time >= ?,
                                    event_time < ?,
                                    region = ?,
                                    tier >= ?])
            => each parameter row is (s.start, s.tenant, s.end);
               Spark evaluates region = ? and tier >= 2 itself on the returned rows

        The two predicates on `event_time` are two fields of the parameter row, in the order of
        their positions. A reader that selects them so:

        >>> def selectLookupPredicates(self, candidates):
        ...     def position(cls, column):
        ...         return next(
        ...             (i for i, c in enumerate(candidates)
        ...              if type(c) is cls and c.attribute == (column,)),
        ...             None,
        ...         )
        ...     positions = [
        ...         position(GreaterThanOrEqual, "event_time"),
        ...         position(EqualTo, "tenant"),
        ...         position(LessThan, "event_time"),
        ...     ]
        ...     return [] if None in positions else positions
        """
        ...

    def maxParameterRowsPerRequest(self) -> int:
        """
        Returns the maximum number of parameter rows that one :meth:`lookup` call receives.

        Spark never sends more parameter rows in one request. Spark also caps a request at 1000
        parameter rows, and by limits of its own, such as the size of a request in bytes. The
        reader must return complete results for a request of that size, with no cap on the
        number of result rows. Spark calls this method at most once on each reader, after a
        selection of :meth:`selectLookupPredicates` that it accepts: not after an empty one, or
        one that repeats a position.

        By default, it returns 1000, Spark's own cap, so Spark never splits a request for the
        reader.

        .. warning::
            If your backend limits the number of values in one call (for example, the number of
            `?` markers in a SQL statement, or the keys of one multi-get), override this method
            and return a number that fits, counting every selected predicate. A request with
            more values than the backend accepts fails the task.

        Returns
        -------
        int
            A positive number of parameter rows. A value outside the range of a Java `int` is
            clamped to it, keeping its sign.

        Notes
        -----
        Spark fails the query with `DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_RESULT` for a
        result of `None`, `INVALID_RESULT_TYPE` for a result that is not an `int`, including a
        `bool`, and `NON_POSITIVE_MAX_PARAMETER_ROWS` for a result that is not positive. A
        streaming query stops on each of them when it starts. An exception fails the query
        with `PYTHON_DATA_SOURCE_ERROR`.
        """
        return 1000

    @abstractmethod
    def lookup(
        self, parameters: List[Tuple]
    ) -> Union[Iterator[Tuple[int, Tuple]], Iterator["RecordBatch"]]:
        """
        Looks up the stored rows that match each of the given parameter rows.

        Spark calls this method on an executor once per request, in order, on the copy of the
        reader that it unpickled for the input partition. Calls never overlap.

        Parameters
        ----------
        parameters : list of tuples
            Between 1 and :meth:`maxParameterRowsPerRequest` parameter rows. Field `i` of each
            tuple is the value for the candidate at the `i`-th position that
            :meth:`selectLookupPredicates` returned. Spark sends no two equal parameter rows,
            but two TIMESTAMP values in a repeated local hour, such as the hour that a
            daylight-saving change repeats, arrive as equal values (see below), so do not rely
            on distinct tuples, for example as dictionary keys, when a selected parameter is a
            TIMESTAMP. Only the fields of
            `<=>` predicates can be `None`; a `None` there matches a stored NULL. When a
            selected range predicate reads a stream column, each request holds exactly one
            parameter row. The list and its tuples are read-only. Each value has the Python
            type for the data type of its :class:`LookupParameter`:

            - an integral type: `int`
            - BOOLEAN: `bool`
            - DECIMAL: `decimal.Decimal`, at the column's precision and scale
            - DATE: `datetime.date`
            - TIMESTAMP: a naive `datetime.datetime` in the local time zone of the Python
              worker, as for the rows that a DataFrame passes to Python elsewhere, with `fold`
              always 0. During a daylight-saving change, two instants can have the same local
              time, and so the same value.
            - TIMESTAMP_NTZ: a naive `datetime.datetime` of the stored date and time
            - BINARY: `bytes`, or `bytearray` when
              `spark.sql.execution.pyspark.binaryAsBytes` is false
            - STRING or VARCHAR: `str`, which Spark compares as UTF-8 bytes

            A conversion that loses precision, such as to milliseconds, can lose matches.

        Returns
        -------
        iterator of (int, tuple or :class:`Row`) pairs, or of PyArrow's `RecordBatch`
            One `(ordinal, row)` pair per result row, in any order. `ordinal` is the position of
            the matched parameter row in `parameters`: an `int` in ``[0, len(parameters))``,
            not a `bool` or `None`. `row` is a tuple in the order of the read schema, or a
            :class:`Row`, as :meth:`DataSourceReader.read` yields. An ordinal may repeat, for
            several matching rows; an ordinal that does not appear is a miss. A stored row that
            matches several parameter rows is returned once per ordinal, and equal stored rows
            are each returned.

            It can also return an iterator of PyArrow's `RecordBatch`, as
            :meth:`DataSourceReader.read` can: the first column of each batch is the ordinal of
            each row, of any integer type and with no nulls, and the other columns are those of
            the read schema, with their names and the Arrow types that a batch of
            :meth:`DataSourceReader.read` has. The first value that the iterator yields decides
            the form for the whole call.

        Notes
        -----
        Every returned row must carry the stored values of the columns that the selected
        predicates read. A value may be NULL only where both the table and the read schema
        allow one. In the `(ordinal, row)` form, the Python worker rounds a DECIMAL to the scale
        of its column, half to even, and it must then fit the column's precision. The results may
        omit a row that fails a pushed filter. Do not keep `parameters` after the generator ends.

        Spark fails the task with a :class:`PythonException` (`PYTHON_EXCEPTION`) for an
        exception that this method raises, and for these errors, which the Python worker
        reports: `DATA_SOURCE_INVALID_RETURN_TYPE`, for a result that is not an iterator, an
        item that is not an `(ordinal, row)` pair, or an ordinal that is not an `int`; and
        `DATA_SOURCE_RETURN_SCHEMA_MISMATCH` or `DATA_SOURCE_INVALID_RETURN_TYPE`, for a row
        that does not fit the read schema. It fails the task with
        `DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_INPUT_ORDINAL` for an ordinal out of range,
        and with `NULL_IN_NOT_NULL_FIELD` for a `None` in a top-level column that is not
        nullable. In the `(ordinal, row)` form, the Python worker raises an exception, which
        fails the task with `PYTHON_EXCEPTION`, for a DECIMAL with more digits than its
        precision allows, and for a `None` in a nested field, an array element or a map value
        that is not nullable. In the `RecordBatch` form, the Python worker reports
        `DATA_SOURCE_INVALID_RETURN_TYPE` for an item that is not a `RecordBatch`, or an ordinal
        column that is not of an integer type or holds a null, and checks the other columns as
        it checks a batch of :meth:`DataSourceReader.read`, such as with
        `DATA_SOURCE_RETURN_SCHEMA_MISMATCH` for a wrong number of columns.

        Spark may stop reading the result early, for example when a semi join has decided every
        stream row. It then closes the generator, so `try`/`finally` and `with` blocks run. The
        stop takes effect at the end of a batch: Python reads the pairs in batches of
        `spark.sql.execution.arrow.maxRecordsPerBatch`, so the generator may run up to one batch
        further, and in the `RecordBatch` form, it may run until it yields its next batch. If
        the generator does not stop within `spark.python.task.killTimeout`, or within 2 seconds
        when that is 0 or less, Spark ends the Python worker: neither the generator's `finally`
        blocks nor :meth:`close` run on that copy of the reader, and a later request of the task
        runs on a new copy of the planned reader.

        `spark.sql.pyspark.dataSource.profiler` does not profile this method or :meth:`close`.

        Examples
        --------
        Yields `(ordinal, row)` pairs:

        >>> def lookup(self, parameters):
        ...     for ordinal, (user_id,) in enumerate(parameters):
        ...         for row in self.backend.get(user_id):
        ...             yield ordinal, row

        Yields PyArrow RecordBatches, with the ordinal as the first column, for the read schema
        `user_id BIGINT, name STRING`. Each column has the Arrow type of its column in the read
        schema, also in a batch without rows, so the types are given rather than inferred:

        >>> def lookup(self, parameters):
        ...     import pyarrow as pa
        ...     ordinals, user_ids, names = [], [], []
        ...     for ordinal, (user_id,) in enumerate(parameters):
        ...         for row in self.backend.get(user_id):
        ...             ordinals.append(ordinal)
        ...             user_ids.append(row[0])
        ...             names.append(row[1])
        ...     yield pa.RecordBatch.from_arrays(
        ...         [
        ...             pa.array(ordinals, pa.int32()),
        ...             pa.array(user_ids, pa.int64()),
        ...             pa.array(names, pa.string()),
        ...         ],
        ...         names=["ordinal", "user_id", "name"],
        ...     )
        """
        ...

    def close(self) -> None:
        """
        Releases the resources of this reader, such as connections.

        Spark calls this method on an executor at most once on each copy of the reader, after
        its last :meth:`lookup` call, when the task ends, also when it fails. This method must
        return within `spark.python.task.killTimeout`, or within 2 seconds when that is 0 or
        less; otherwise, Spark ends the Python worker and fails the task. An exception from this method fails the task too. When the task has
        failed already, that failure stays the task's error, and Spark adds the failure of this
        method, a timeout or an exception, to it as suppressed.

        Spark does not call this method, and ends the Python worker instead, when the task is
        killed, when another thread closes the reader, when the Python worker has failed, as it
        does after :meth:`lookup` raises an exception, and for a copy whose :meth:`lookup` does
        not stop in time after Spark stopped reading its result. So do not rely on it for
        correctness.
        """
        ...


class DataSourceWriter(ABC):
    """
    A base class for data source writers. Data source writers are responsible for saving
    the data to the data source.

    .. versionadded:: 4.0.0
    """

    @abstractmethod
    def write(self, iterator: Iterator[Row]) -> "WriterCommitMessage":
        """
        Writes data into the data source.

        This method is called once on each executor to write data to the data source.
        It accepts an iterator of input data and returns a single row representing a
        commit message, or None if there is no commit message.

        The driver collects commit messages, if any, from all executors and passes them
        to the :class:`DataSourceWriter.commit` method if all tasks run successfully. If any
        task fails, the :class:`DataSourceWriter.abort` method will be called with the
        collected commit messages.

        Parameters
        ----------
        iterator : iterator of :class:`Row`\\s
            An iterator of input data.

        Returns
        -------
        :class:`WriterCommitMessage`
            a serializable commit message
        """
        ...

    def commit(self, messages: List[Optional["WriterCommitMessage"]]) -> None:
        """
        Commits this writing job with a list of commit messages.

        This method is invoked on the driver when all tasks run successfully. The
        commit messages are collected from the :meth:`DataSourceWriter.write` method call
        from each task, and are passed to this method. The implementation should use the
        commit messages to commit the writing job to the data source.

        Parameters
        ----------
        messages : list of :class:`WriterCommitMessage`\\s
            A list of commit messages. If a write task fails, the commit message will be `None`.
        """
        ...

    def abort(self, messages: List[Optional["WriterCommitMessage"]]) -> None:
        """
        Aborts this writing job due to task failures.

        This method is invoked on the driver when one or more tasks failed. The commit
        messages are collected from the :meth:`DataSourceWriter.write` method call from
        each task, and are passed to this method. The implementation should use the
        commit messages to abort the writing job to the data source.

        Parameters
        ----------
        messages : list of :class:`WriterCommitMessage`\\s
            A list of commit messages. If a write task fails, the commit message will be `None`.
        """
        ...


class DataSourceArrowWriter(DataSourceWriter):
    """
    A base class for data source writers that process data using PyArrow's `RecordBatch`.

    Unlike :class:`DataSourceWriter`, which works with an iterator of Spark Rows, this class
    is optimized for using the Arrow format when writing data. It can offer better performance
    when interfacing with systems or libraries that natively support Arrow.

    .. versionadded:: 4.0.0
    """

    @abstractmethod
    def write(self, iterator: Iterator["RecordBatch"]) -> "WriterCommitMessage":
        """
        Writes an iterator of PyArrow `RecordBatch` objects to the sink.

        This method is called once on each executor to write data to the data source.
        It accepts an iterator of PyArrow `RecordBatch`\\s and returns a single row
        representing a commit message, or None if there is no commit message.

        The driver collects commit messages, if any, from all executors and passes them
        to the :class:`DataSourceWriter.commit` method if all tasks run successfully. If any
        task fails, the :class:`DataSourceWriter.abort` method will be called with the
        collected commit messages.

        Parameters
        ----------
        iterator : iterator of :class:`RecordBatch`\\s
            An iterator of PyArrow `RecordBatch` objects representing the input data.

        Returns
        -------
        :class:`WriterCommitMessage`
            a serializable commit message

        Examples
        --------
        >>> from dataclasses import dataclass
        >>> @dataclass
        ... class MyCommitMessage(WriterCommitMessage):
        ...     num_rows: int
        ...
        >>> def write(self, iterator: Iterator["RecordBatch"]) -> "WriterCommitMessage":
        ...     total_rows = 0
        ...     for batch in iterator:
        ...         total_rows += len(batch)
        ...     return MyCommitMessage(num_rows=total_rows)
        """
        ...


class DataSourceStreamWriter(ABC):
    """
    A base class for data stream writers. Data stream writers are responsible for writing
    the data to the streaming sink.

    .. versionadded:: 4.0.0
    """

    @abstractmethod
    def write(self, iterator: Iterator[Row]) -> "WriterCommitMessage":
        """
        Writes data into the streaming sink.

        This method is called on executors to write data to the streaming data sink in
        each microbatch. It accepts an iterator of input data and returns a single row
        representing a commit message, or None if there is no commit message.

        The driver collects commit messages, if any, from all executors and passes them
        to the ``commit`` method if all tasks run successfully. If any task fails, the
        ``abort`` method will be called with the collected commit messages.

        Parameters
        ----------
        iterator : Iterator[Row]
            An iterator of input data.

        Returns
        -------
        WriterCommitMessage : a serializable commit message
        """
        ...

    def commit(self, messages: List[Optional["WriterCommitMessage"]], batchId: int) -> None:
        """
        Commits this microbatch with a list of commit messages.

        This method is invoked on the driver when all tasks run successfully. The
        commit messages are collected from the ``write`` method call from each task,
        and are passed to this method. The implementation should use the commit messages
        to commit the microbatch in the streaming sink.

        Parameters
        ----------
        messages : list of :class:`WriterCommitMessage`\\s
            A list of commit messages. If a write task fails, the commit message will be `None`.
        batchId: int
            An integer that uniquely identifies a batch of data being written.
            The integer increase by 1 with each microbatch processed.
        """
        ...

    def abort(self, messages: List[Optional["WriterCommitMessage"]], batchId: int) -> None:
        """
        Aborts this microbatch due to task failures.

        This method is invoked on the driver when one or more tasks failed. The commit
        messages are collected from the ``write`` method call from each task, and are
        passed to this method. The implementation should use the commit messages to
        abort the microbatch in the streaming sink.

        Parameters
        ----------
        messages : list of :class:`WriterCommitMessage`\\s
            A list of commit messages. If a write task fails, the commit message will be `None`.
        batchId: int
            An integer that uniquely identifies a batch of data being written.
            The integer increase by 1 with each microbatch processed.
        """
        ...


class DataSourceStreamArrowWriter(DataSourceStreamWriter):
    """
    A base class for data stream writers that process data using PyArrow's `RecordBatch`.

    Unlike :class:`DataSourceStreamWriter`, which works with an iterator of Spark Rows, this class
    is optimized for using the Arrow format when writing streaming data. It can offer better
    performance when interfacing with systems or libraries that natively support Arrow for
    streaming use cases.

    .. versionadded:: 4.1.0
    """

    @abstractmethod
    def write(self, iterator: Iterator["RecordBatch"]) -> "WriterCommitMessage":
        """
        Writes an iterator of PyArrow `RecordBatch` objects to the streaming sink.

        This method is called on executors to write data to the streaming data sink in
        each microbatch. It accepts an iterator of PyArrow `RecordBatch` objects and
        returns a single row representing a commit message, or None if there is no commit message.

        The driver collects commit messages, if any, from all executors and passes them
        to the :class:`DataSourceStreamArrowWriter.commit` method if all tasks run
        successfully. If any task fails, the :class:`DataSourceStreamArrowWriter.abort` method
        will be called with the collected commit messages.

        Parameters
        ----------
        iterator : iterator of :class:`RecordBatch`\\s
            An iterator of PyArrow `RecordBatch` objects representing the input data.

        Returns
        -------
        :class:`WriterCommitMessage`
            a serializable commit message

        Examples
        --------
        >>> from dataclasses import dataclass
        >>> @dataclass
        ... class MyCommitMessage(WriterCommitMessage):
        ...     num_rows: int
        ...     batch_id: int
        ...
        >>> def write(self, iterator: Iterator["RecordBatch"]) -> "WriterCommitMessage":
        ...     total_rows = 0
        ...     for batch in iterator:
        ...         total_rows += len(batch)
        ...     return MyCommitMessage(num_rows=total_rows, batch_id=self.current_batch_id)
        """
        ...


class WriterCommitMessage:
    """
    A commit message returned by the :meth:`DataSourceWriter.write` and will be
    sent back to the driver side as input parameter of :meth:`DataSourceWriter.commit`
    or :meth:`DataSourceWriter.abort` method.

    .. versionadded:: 4.0.0

    Notes
    -----
    This class must be picklable.
    """

    ...


class DataSourceRegistration:
    """
    Wrapper for data source registration. This instance can be accessed by
    :attr:`spark.dataSource`.

    .. versionadded:: 4.0.0
    """

    def __init__(self, sparkSession: "SparkSession"):
        self.sparkSession = sparkSession

    def register(
        self,
        dataSource: Type["DataSource"],
    ) -> None:
        """Register a Python user-defined data source.

        Parameters
        ----------
        dataSource : type
            The data source class to be registered. It should be a subclass of DataSource.
        """
        from pyspark.sql.udf import _wrap_function

        name = dataSource.name()
        sc = self.sparkSession.sparkContext
        # Serialize the data source class.
        wrapped = _wrap_function(sc, dataSource)
        assert sc._jvm is not None
        jvm = sc._jvm
        ds = getattr(
            jvm, "org.apache.spark.sql.execution.datasources.v2.python.UserDefinedPythonDataSource"
        )(wrapped)
        self.sparkSession._jsparkSession.dataSource().registerPython(name, ds)


class CaseInsensitiveDict(UserDict):
    """
    A case-insensitive map of string keys to values.

    This is used by Python data source options to ensure consistent case insensitivity.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.update(*args, **kwargs)

    def __setitem__(self, key: str, value: Any) -> None:
        super().__setitem__(key.lower(), value)

    def __getitem__(self, key: str) -> Any:
        return super().__getitem__(key.lower())

    def __delitem__(self, key: str) -> None:
        super().__delitem__(key.lower())

    def __contains__(self, key: object) -> bool:
        if isinstance(key, str):
            return super().__contains__(key.lower())
        return False

    def update(self, *args: Any, **kwargs: Any) -> None:
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def copy(self) -> "CaseInsensitiveDict":
        return type(self)(self)
