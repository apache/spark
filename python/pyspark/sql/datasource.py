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
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    TYPE_CHECKING,
)

from pyspark.sql import Row
from pyspark.sql.types import StructType
from pyspark.errors import PySparkNotImplementedError

if TYPE_CHECKING:
    from pyarrow import RecordBatch
    from pyspark.sql.session import SparkSession

__all__ = [
    "DataSource",
    "DataSourceReader",
    "DataSourceStreamReader",
    "SimpleDataSourceStreamReader",
    "DataSourceWriter",
    "DataSourceArrowWriter",
    "DataSourceStreamWriter",
    "DataSourceRegistration",
    "InputPartition",
    "SimpleDataSourceStreamReader",
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

    .. versionadded: 4.0.0
    """

    def __init__(self, options: Dict[str, str]) -> None:
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


ColumnPath = Tuple[str, ...]
"""
A tuple of strings representing a column reference.

For example, `("a", "b", "c")` represents the column `a.b.c`.

.. versionadded: 4.1.0
"""


@dataclass(frozen=True)
class Filter(ABC):
    """
    The base class for filters used for filter pushdown.

    .. versionadded: 4.1.0

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


class InputPartition:
    """
    A base class representing an input partition returned by the `partitions()`
    method of :class:`DataSourceReader`.

    .. versionadded: 4.0.0

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

    .. versionadded: 4.0.0
    """

    def pushFilters(self, filters: List["Filter"]) -> Iterable["Filter"]:
        """
        Called with the list of filters that can be pushed down to the data source.

        The list of filters should be interpreted as the AND of the elements.

        Filter pushdown allows the data source to handle a subset of filters. This
        can improve performance by reducing the amount of data that needs to be
        processed by Spark.

        This method is called once during query planning. By default, it returns
        all filters, indicating that no filters can be pushed down. Subclasses can
        override this method to implement filter pushdown.

        It's recommended to implement this method only for data sources that natively
        support filtering, such as databases and GraphQL APIs.

        .. versionadded: 4.1.0

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

    def partitions(self) -> Sequence[InputPartition]:
        """
        Returns an iterator of partitions for this data source.

        Partitions are used to split data reading operations into parallel tasks.
        If this method returns N partitions, the query planner will create N tasks.
        Each task will execute :meth:`DataSourceReader.read` in parallel, using the respective
        partition value to read the data.

        This method is called once during query planning. By default, it returns a
        single partition with the value ``None``. Subclasses can override this method
        to return multiple partitions.

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
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "partitions"},
        )

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
        partition : object
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

    .. versionadded: 4.0.0
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

    def latestOffset(self) -> dict:
        """
        Returns the most recent offset available.

        Returns
        -------
        dict
            A dict or recursive dict whose key and value are primitive types, which includes
            Integer, String and Boolean.

        Examples
        --------
        >>> def latestOffset(self):
        ...     return {"parititon-1": {"index": 3, "closed": True}, "partition-2": {"index": 5}}
        """
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "latestOffset"},
        )

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

    .. versionadded: 4.0.0
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


class DataSourceWriter(ABC):
    """
    A base class for data source writers. Data source writers are responsible for saving
    the data to the data source.

    .. versionadded: 4.0.0
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

    .. versionadded: 4.0.0
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

    .. versionadded: 4.0.0
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


class WriterCommitMessage:
    """
    A commit message returned by the :meth:`DataSourceWriter.write` and will be
    sent back to the driver side as input parameter of :meth:`DataSourceWriter.commit`
    or :meth:`DataSourceWriter.abort` method.

    .. versionadded: 4.0.0

    Notes
    -----
    This class must be picklable.
    """

    ...


class DataSourceRegistration:
    """
    Wrapper for data source registration. This instance can be accessed by
    :attr:`spark.dataSource`.

    .. versionadded: 4.0.0
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
        super().__init__(*args, **kwargs)
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
