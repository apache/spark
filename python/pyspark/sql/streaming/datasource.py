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
from dataclasses import dataclass


class ReadLimit:
    """
    Specifies limits on how much data to read from a streaming source when
    determining the latest offset.

    As of Spark 4.2.0, only built-in implementations of :class:`ReadLimit` are supported. Please
    refer to the following classes for the supported types:

    - :class:`ReadAllAvailable`
    - :class:`ReadMinRows`
    - :class:`ReadMaxRows`
    - :class:`ReadMaxFiles`
    - :class:`ReadMaxBytes`
    """


@dataclass
class ReadAllAvailable(ReadLimit):
    """
    A :class:`ReadLimit` that indicates to read all available data, regardless of the given source
    options.
    """


@dataclass
class ReadMinRows(ReadLimit):
    """
    A :class:`ReadLimit` that indicates to read minimum N rows. If there is less than N rows
    available for read, the source should skip producing a new offset to read and wait until more
    data arrives.

    Note that the semantic does not work properly with Trigger.AvailableNow since the source
    may end up waiting forever for more data to arrive. It is the source's responsibility to
    handle this case properly.
    """

    min_rows: int


@dataclass
class ReadMaxRows(ReadLimit):
    """
    A :class:`ReadLimit` that indicates to read maximum N rows. The source should not read more
    than N rows when determining the latest offset.
    """

    max_rows: int


@dataclass
class ReadMaxFiles(ReadLimit):
    """
    A :class:`ReadLimit` that indicates to read maximum N files. The source should not read more
    than N files when determining the latest offset.
    """

    max_files: int


@dataclass
class ReadMaxBytes(ReadLimit):
    """
    A :class:`ReadLimit` that indicates to read maximum N bytes. The source should not read more
    than N bytes when determining the latest offset.
    """

    max_bytes: int


class SupportsTriggerAvailableNow(ABC):
    """
    A mixin interface for streaming sources that support Trigger.AvailableNow. This interface can
    be added to both :class:`DataSourceStreamReader` and :class:`SimpleDataSourceStreamReader`.
    """

    @abstractmethod
    def prepareForTriggerAvailableNow(self) -> None:
        """
        This will be called at the beginning of streaming queries with Trigger.AvailableNow, to let
        the source record the offset for the current latest data at the time (a.k.a the target
        offset for the query). The source must behave as if there is no new data coming in after
        the target offset, i.e., the source must not return an offset higher than the target offset
        when :meth:`DataSourceStreamReader.latestOffset()` is called.

        The source can extend the semantic of "current latest data" based on its own logic, but the
        extended semantic must not violate the expectation that the source will not read any data
        which is added later than the time this method has called.

        Note that it is the source's responsibility to ensure that calling
        :meth:`DataSourceStreamReader.latestOffset()` or :meth:`SimpleDataSourceStreamReader.read()`
        after calling this method will eventually reach the target offset, and finally returns the
        same offset as given start parameter, to indicate that there is no more data to read. This
        includes the case where the query is restarted and the source is asked to read from the
        offset being journaled in previous run - source should take care of exceptional cases like
        new partition has added during the restart, etc, to ensure that the query run will be
        completed at some point.
        """
        pass
