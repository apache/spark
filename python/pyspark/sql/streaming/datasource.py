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
from typing import Optional


class ReadLimit(ABC):
    """
    Specifies limits on how much data to read from a streaming source when
    determining the latest offset.
    """

    @classmethod
    @abstractmethod
    def type_name(cls) -> str:
        """
        The name of this :class:`ReadLimit` type. This is used to register the type into registry.

        Returns
        -------
        str
            The name of this :class:`ReadLimit` type.
        """
        pass

    @classmethod
    @abstractmethod
    def load(cls, params: dict) -> "ReadLimit":
        """
        Create an instance of :class:`ReadLimit` from parameters.

        Parameter
        ---------
        params : dict
            The parameters to create the :class:`ReadLimit`. type name isn't included.

        Returns
        -------
        ReadLimit
            The created :class:`ReadLimit` instance.
        """
        pass

    def dump(self) -> dict:
        """
        Method to serialize this :class:`ReadLimit` instance. Implementations of :class:`ReadLimit`
        are expected to not implement this method directly and rather implement the
        :meth:`_dump()` method.

        Returns
        -------
        dict
            A dictionary containing the serialized parameters of this :class:`ReadLimit`,
            including the type name.
        """
        params = self._dump()
        params.update({"type": self.type_name()})
        return params

    @abstractmethod
    def _dump(self) -> dict:
        """
        Method to serialize this :class:`ReadLimit` instance. Implementations of :class:`ReadLimit`
        are expected to implement this method to handle their specific parameters. type name will
        be handled in the :meth:`dump()` method.

        Returns
        -------
        dict
            A dictionary containing the serialized parameters of this :class:`ReadLimit`,
            excluding the type name.
        """
        pass


class ReadAllAvailable(ReadLimit):
    """
    A :class:`ReadLimit` that indicates to read all available data, regardless of the given source
    options.
    """

    @classmethod
    def type_name(cls) -> str:
        return "ReadAllAvailable"

    @classmethod
    def load(cls, params: dict) -> "ReadAllAvailable":
        return ReadAllAvailable()

    def _dump(self) -> dict:
        return {}


class ReadMinRows(ReadLimit):
    """
    A :class:`ReadLimit` that indicates to read minimum N rows. If there is less than N rows
    available for read, the source should skip producing a new offset to read and wait until more
    data arrives.

    Note that the semantic does not work properly with Trigger.AvailableNow since the source
    may end up waiting forever for more data to arrive. It is the source's responsibility to
    handle this case properly.
    """

    def __init__(self, min_rows: int) -> None:
        self.min_rows = min_rows

    @classmethod
    def type_name(cls) -> str:
        return "ReadMinRows"

    @classmethod
    def load(cls, params: dict) -> "ReadMinRows":
        return ReadMinRows(params["min_rows"])

    def _dump(self) -> dict:
        return {"min_rows": self.min_rows}


class ReadMaxRows(ReadLimit):
    """
    A :class:`ReadLimit` that indicates to read maximum N rows. The source should not read more
    than N rows when determining the latest offset.
    """

    def __init__(self, max_rows: int) -> None:
        self.max_rows = max_rows

    @classmethod
    def type_name(cls) -> str:
        return "ReadMaxRows"

    @classmethod
    def load(cls, params: dict) -> "ReadMaxRows":
        return ReadMaxRows(params["max_rows"])

    def _dump(self) -> dict:
        return {"max_rows": self.max_rows}


class ReadMaxFiles(ReadLimit):
    """
    A :class:`ReadLimit` that indicates to read maximum N files. The source should not read more
    than N files when determining the latest offset.
    """

    def __init__(self, max_files: int) -> None:
        self.max_files = max_files

    @classmethod
    def type_name(cls) -> str:
        return "ReadMaxFiles"

    @classmethod
    def load(cls, params: dict) -> "ReadMaxFiles":
        return ReadMaxFiles(params["max_files"])

    def _dump(self) -> dict:
        return {"max_files": self.max_files}


class ReadMaxBytes(ReadLimit):
    """
    A :class:`ReadLimit` that indicates to read maximum N bytes. The source should not read more
    than N bytes when determining the latest offset.
    """

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes

    @classmethod
    def type_name(cls) -> str:
        return "ReadMaxBytes"

    @classmethod
    def load(cls, params: dict) -> "ReadMaxBytes":
        return ReadMaxBytes(params["max_bytes"])

    def _dump(self) -> dict:
        return {"max_bytes": self.max_bytes}


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
