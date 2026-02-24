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
from itertools import chain
from typing import Iterator, List, Sequence, Tuple, Type, Dict

from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamReader,
    InputPartition,
    SimpleDataSourceStreamReader,
)
from pyspark.sql.streaming.datasource import (
    ReadAllAvailable,
    ReadLimit,
    ReadMaxBytes,
    ReadMaxRows,
    ReadMinRows,
    ReadMaxFiles,
)
from pyspark.sql.types import StructType
from pyspark.errors import PySparkNotImplementedError
from pyspark.errors.exceptions.base import PySparkException


def _streamReader(datasource: DataSource, schema: StructType) -> "DataSourceStreamReader":
    """
    Fallback to simpleStreamReader() method when streamReader() is not implemented.
    This should be invoked whenever a DataSourceStreamReader needs to be created instead of
    invoking datasource.streamReader() directly.
    """
    try:
        return datasource.streamReader(schema=schema)
    except PySparkNotImplementedError:
        return _SimpleStreamReaderWrapper(datasource.simpleStreamReader(schema=schema))


class SimpleInputPartition(InputPartition):
    def __init__(self, start: dict, end: dict):
        self.start = start
        self.end = end


class PrefetchedCacheEntry:
    def __init__(self, start: dict, end: dict, iterator: Iterator[Tuple]):
        self.start = start
        self.end = end
        self.iterator = iterator


class _SimpleStreamReaderWrapper(DataSourceStreamReader):
    """
    A private class that wrap :class:`SimpleDataSourceStreamReader` in prefetch and cache pattern,
    so that :class:`SimpleDataSourceStreamReader` can integrate with streaming engine like an
    ordinary :class:`DataSourceStreamReader`.

    When streaming engine calls latestOffset(), the wrapper calls read() that starts from
    start provided via the parameter of latestOffset(), prefetches and cache the data, then updates
    the current_offset to be the end offset of the new data.

    When streaming engine call planInputPartitions(start, end), the wrapper get the prefetched data
    from cache and send it to JVM along with the input partitions.

    When query restart, batches in write ahead offset log that has not been committed will be
    replayed by reading data between start and end offset through readBetweenOffsets(start, end).
    """

    def __init__(self, simple_reader: SimpleDataSourceStreamReader):
        self.simple_reader = simple_reader
        self.cache: List[PrefetchedCacheEntry] = []

    def initialOffset(self) -> dict:
        return self.simple_reader.initialOffset()

    def getDefaultReadLimit(self) -> ReadLimit:
        # We do not consider providing different read limit on simple stream reader.
        return ReadAllAvailable()

    def add_result_to_cache(self, start: dict, end: dict, it: Iterator[Tuple]) -> None:
        """
        Validates that read() did not return a non-empty batch with end equal to start,
        which would cause the same batch to be processed repeatedly. When end != start,
        appends the result to the cache; when end == start with empty iterator, does not
        cache (avoids unbounded cache growth).
        """
        start_str = json.dumps(start)
        end_str = json.dumps(end)
        if end_str != start_str:
            self.cache.append(PrefetchedCacheEntry(start, end, it))
            return
        try:
            next(it)
        except StopIteration:
            return
        raise PySparkException(
            errorClass="SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE",
            messageParameters={
                "start_offset": start_str,
                "end_offset": end_str,
            },
        )

    def latestOffset(self, start: dict, limit: ReadLimit) -> dict:
        assert start is not None, "start offset should not be None"
        assert isinstance(
            limit, ReadAllAvailable
        ), "simple stream reader does not support read limit"

        (iter, end) = self.simple_reader.read(start)
        self.add_result_to_cache(start, end, iter)
        return end

    def commit(self, end: dict) -> None:
        end_idx = -1
        for idx, entry in enumerate(self.cache):
            if json.dumps(entry.end) == json.dumps(end):
                end_idx = idx
                break
        if end_idx > 0:
            # Drop prefetched data for batch that has been committed.
            self.cache = self.cache[end_idx:]
        self.simple_reader.commit(end)

    def partitions(self, start: dict, end: dict) -> Sequence["InputPartition"]:
        if len(self.cache) > 0:
            assert self.cache[-1].end == end
        return [SimpleInputPartition(start, end)]

    def getCache(self, start: dict, end: dict) -> Iterator[Tuple]:
        start_idx = -1
        end_idx = -1
        for idx, entry in enumerate(self.cache):
            # There is no convenient way to compare 2 offsets.
            # Serialize into json string before comparison.
            if json.dumps(entry.start) == json.dumps(start):
                start_idx = idx
            if json.dumps(entry.end) == json.dumps(end):
                end_idx = idx
                break
        if start_idx == -1 or end_idx == -1:
            return None  # type: ignore[return-value]
        # Chain all the data iterator between start offset and end offset
        # need to copy here to avoid exhausting the original data iterator.
        entries = [copy.copy(entry.iterator) for entry in self.cache[start_idx : end_idx + 1]]
        it = chain(*entries)
        return it

    def read(
        self, input_partition: SimpleInputPartition  # type: ignore[override]
    ) -> Iterator[Tuple]:
        return self.simple_reader.readBetweenOffsets(input_partition.start, input_partition.end)


class ReadLimitRegistry:
    def __init__(self) -> None:
        self._registry: Dict[str, Type[ReadLimit]] = {}
        # Register built-in ReadLimit types
        self.__register(ReadAllAvailable)
        self.__register(ReadMinRows)
        self.__register(ReadMaxRows)
        self.__register(ReadMaxFiles)
        self.__register(ReadMaxBytes)

    def __register(self, read_limit_type: Type["ReadLimit"]) -> None:
        name = read_limit_type.__name__
        if name in self._registry:
            raise PySparkException(f"ReadLimit type '{name}' is already registered.")
        self._registry[name] = read_limit_type

    def get(self, params_with_type: dict) -> ReadLimit:
        type_name = params_with_type["_type"]
        if type_name is None:
            raise PySparkException("ReadLimit type name is missing.")

        read_limit_type = self._registry.get(type_name)
        if read_limit_type is None:
            raise PySparkException("name '{}' is not registered.".format(type_name))

        params_without_type = params_with_type.copy()
        del params_without_type["_type"]
        return read_limit_type(**params_without_type)
