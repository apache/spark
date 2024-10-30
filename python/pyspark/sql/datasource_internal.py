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
from typing import Iterator, List, Optional, Sequence, Tuple

from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamReader,
    InputPartition,
    SimpleDataSourceStreamReader,
)
from pyspark.sql.types import StructType
from pyspark.errors import PySparkNotImplementedError


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

    current_offset tracks the latest progress of the record prefetching, it is initialized to be
    initialOffset() when query start for the first time or initialized to be the end offset of
    the last planned batch when query restarts.

    When streaming engine calls latestOffset(), the wrapper calls read() that starts from
    current_offset, prefetches and cache the data, then updates the current_offset to be
    the end offset of the new data.

    When streaming engine call planInputPartitions(start, end), the wrapper get the prefetched data
    from cache and send it to JVM along with the input partitions.

    When query restart, batches in write ahead offset log that has not been committed will be
    replayed by reading data between start and end offset through readBetweenOffsets(start, end).
    """

    def __init__(self, simple_reader: SimpleDataSourceStreamReader):
        self.simple_reader = simple_reader
        self.initial_offset: Optional[dict] = None
        self.current_offset: Optional[dict] = None
        self.cache: List[PrefetchedCacheEntry] = []

    def initialOffset(self) -> dict:
        if self.initial_offset is None:
            self.initial_offset = self.simple_reader.initialOffset()
        return self.initial_offset

    def latestOffset(self) -> dict:
        # when query start for the first time, use initial offset as the start offset.
        if self.current_offset is None:
            self.current_offset = self.initialOffset()
        (iter, end) = self.simple_reader.read(self.current_offset)
        self.cache.append(PrefetchedCacheEntry(self.current_offset, end, iter))
        self.current_offset = end
        return end

    def commit(self, end: dict) -> None:
        if self.current_offset is None:
            self.current_offset = end

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
        # when query restart from checkpoint, use the last committed offset as the start offset.
        # This depends on the streaming engine calling planInputPartitions() of the last batch
        # in offset log when query restart.
        if self.current_offset is None:
            self.current_offset = end
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
