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
import copy
from abc import ABC, abstractmethod
from collections import UserDict
from itertools import chain
from typing import Any, Dict, Iterator, List, Sequence, Tuple, Type, Union, TYPE_CHECKING

from pyspark.sql import Row
from pyspark.sql.types import StructType
from pyspark.errors import PySparkNotImplementedError

if TYPE_CHECKING:
    from pyspark.sql.session import SparkSession


__all__ = [
    "DataSource",
    "DataSourceReader",
    "DataSourceStreamReader",
    "DataSourceWriter",
    "DataSourceRegistration",
    "InputPartition",
    "WriterCommitMessage",
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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "schema"},
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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "reader"},
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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "writer"},
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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "streamWriter"},
        )

    def _streamReader(self, schema: StructType) -> "DataSourceStreamReader":
        try:
            return self.streamReader(schema=schema)
        except PySparkNotImplementedError:
            return _SimpleStreamReaderWrapper(self.simpleStreamReader(schema=schema))

    def simpleStreamReader(self, schema: StructType) -> "SimpleDataSourceStreamReader":
        """
        Returns a :class:`SimpleDataSourceStreamReader` instance for reading data.

        One of simpleStreamReader() and streamReader() must be implemented for readable streaming
        data source.

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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "simpleStreamReader"},
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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "streamReader"},
        )


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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "partitions"},
        )

    @abstractmethod
    def read(self, partition: InputPartition) -> Union[Iterator[Tuple], Iterator[Row]]:
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
        iterator of tuples or :class:`Row`\\s
            An iterator of tuples or rows. Each tuple or row will be converted to a row
            in the final DataFrame.

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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "initialOffset"},
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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "latestOffset"},
        )

    def partitions(self, start: dict, end: dict) -> Sequence[InputPartition]:
        """
        Returns a list of InputPartition  given the start and end offsets. Each InputPartition
        represents a data split that can be processed by one Spark task.

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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "partitions"},
        )

    @abstractmethod
    def read(self, partition: InputPartition) -> Union[Iterator[Tuple], Iterator[Row]]:
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
        iterator of tuples or :class:`Row`\\s
            An iterator of tuples or rows. Each tuple or row will be converted to a row
            in the final DataFrame.
        """
        raise PySparkNotImplementedError(
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "read"},
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


class SimpleInputPartition(InputPartition):
    def __init__(self, start: dict, end: dict):
        self.start = start
        self.end = end


class SimpleDataSourceStreamReader(ABC):
    """
    A base class for simplified streaming data source readers.
    Compared to :class:`DataSourceStreamReader`, :class:`SimpleDataSourceStreamReader` doesn't
    require planning data partition. Also, the read api of :class:`SimpleDataSourceStreamReader`
    allows reading data and planning the latest offset at the same time.

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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "initialOffset"},
        )

    def read(self, start: dict) -> (Iterator[Tuple], dict):
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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "read"},
        )

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[Tuple]:
        """
        Read all available data from specific start offset and end offset.
        This is invoked during failure recovery to re-read a batch deterministically
        in order to achieve exactly once.

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
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "read2"},
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


class _SimpleStreamReaderWrapper(DataSourceStreamReader):
    """
    A private class that wrap :class:`SimpleDataSourceStreamReader` in prefetch and cache pattern,
    so that :class:`SimpleDataSourceStreamReader` can integrate with streaming engine like an
    ordinary :class:`DataSourceStreamReader`.

    current_offset track the latest progress of the record prefetching, it is initialized to be
    initialOffset() when query start for the first time or initialized to be the end offset of
    the last committed batch when query restarts.

    When streaming engine call latestOffset(), the wrapper calls read() that start from
    current_offset, prefetch and cache the data, then update the current_offset to be
    the end offset of the new data.

    When streaming engine call planInputPartitions(start, end), the wrapper get the prefetched data
    from cache and send it to JVM along with the input partitions.

    When query restart, batches in write ahead offset log that has not been committed will be
    replayed by reading data between start and end offset through read2(start, end).
    """

    def __init__(self, simple_reader: SimpleDataSourceStreamReader):
        self.simple_reader = simple_reader
        self.initial_offset = None
        self.current_offset = None
        self.cache = []

    def initialOffset(self) -> dict:
        if self.initial_offset is None:
            self.initial_offset = self.simple_reader.initialOffset()
        return self.initial_offset

    def latestOffset(self) -> dict:
        # when query start for the first time, use initial offset as the start offset.
        if self.current_offset is None:
            self.current_offset = self.initialOffset()
        (iter, end) = self.simple_reader.read(self.current_offset)
        self.cache.append((self.current_offset, end, iter))
        self.current_offset = end
        return end

    def commit(self, end: dict) -> None:
        if self.current_offset is None:
            self.current_offset = end

        end_idx = -1
        for i in range(len(self.cache)):
            if json.dumps(self.cache[i][1]) == json.dumps(end):
                end_idx = i
                break
        if end_idx > 0:
            # Drop prefetched data for batch that has been committed.
            self.cache = self.cache[end_idx:]
        self.simple_reader.commit(end)

    def partitions(self, start: dict, end: dict) -> Sequence["InputPartition"]:
        # when query restart from checkpoint, use the last committed offset as the start offset.
        # This depends on the current behavior that streaming engine call getBatch on the last
        # microbatch when query restart.
        if self.current_offset is None:
            self.current_offset = end
        if len(self.cache) > 0:
            assert self.cache[-1][1] == end
        return [SimpleInputPartition(start, end)]

    def getCache(self, start: dict, end: dict) -> Iterator[Tuple]:
        start_idx = -1
        end_idx = -1
        for i in range(len(self.cache)):
            # There is no convenient way to compare 2 offsets.
            # Serialize into json string before comparison.
            if json.dumps(self.cache[i][0]) == json.dumps(start):
                start_idx = i
            if json.dumps(self.cache[i][1]) == json.dumps(end):
                end_idx = i
        if start_idx == -1 or end_idx == -1:
            return None
        # Chain all the data iterator between start offset and end offset
        # need to copy here to avoid exhausting the original data iterator.
        entries = [copy.copy(entry[2]) for entry in self.cache[start_idx : end_idx + 1]]
        it = chain(*entries)
        return it

    def read(self, input_partition: InputPartition):
        return self.simple_reader.readBetweenOffsets(input_partition.start, input_partition.end)


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

    def commit(self, messages: List["WriterCommitMessage"]) -> None:
        """
        Commits this writing job with a list of commit messages.

        This method is invoked on the driver when all tasks run successfully. The
        commit messages are collected from the :meth:`DataSourceWriter.write` method call
        from each task, and are passed to this method. The implementation should use the
        commit messages to commit the writing job to the data source.

        Parameters
        ----------
        messages : list of :class:`WriterCommitMessage`\\s
            A list of commit messages.
        """
        ...

    def abort(self, messages: List["WriterCommitMessage"]) -> None:
        """
        Aborts this writing job due to task failures.

        This method is invoked on the driver when one or more tasks failed. The commit
        messages are collected from the :meth:`DataSourceWriter.write` method call from
        each task, and are passed to this method. The implementation should use the
        commit messages to abort the writing job to the data source.

        Parameters
        ----------
        messages : list of :class:`WriterCommitMessage`\\s
            A list of commit messages.
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

    def commit(self, messages: List["WriterCommitMessage"], batchId: int) -> None:
        """
        Commits this microbatch with a list of commit messages.

        This method is invoked on the driver when all tasks run successfully. The
        commit messages are collected from the ``write`` method call from each task,
        and are passed to this method. The implementation should use the commit messages
        to commit the microbatch in the streaming sink.

        Parameters
        ----------
        messages : List[WriterCommitMessage]
            A list of commit messages.
        batchId: int
            An integer that uniquely identifies a batch of data being written.
            The integer increase by 1 with each microbatch processed.
        """
        ...

    def abort(self, messages: List["WriterCommitMessage"], batchId: int) -> None:
        """
        Aborts this microbatch due to task failures.

        This method is invoked on the driver when one or more tasks failed. The commit
        messages are collected from the ``write`` method call from each task, and are
        passed to this method. The implementation should use the commit messages to
        abort the microbatch in the streaming sink.

        Parameters
        ----------
        messages : List[WriterCommitMessage]
            A list of commit messages.
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
        ds = jvm.org.apache.spark.sql.execution.datasources.v2.python.UserDefinedPythonDataSource(
            wrapped
        )
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
