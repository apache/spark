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
import uuid
from typing import Optional
from abc import ABC, abstractmethod

__all__ = ["StreamingQueryListener"]


class StreamingQueryListener(ABC):
    """
    Interface for listening to events related to :class:`~pyspark.sql.streaming.StreamingQuery`.

    .. versionadded:: 3.5.0

    Notes
    -----
    The methods are not thread-safe as they may be called from different threads.
    The events received are identical with Scala API. Refer to its documentation.

    This API is evolving.

    Examples
    --------
    >>> class MyListener(StreamingQueryListener):
    ...    def onQueryStarted(self, event: QueryStartedEvent) -> None:
    ...        # Do something with event.
    ...        pass
    ...
    ...    def onQueryProgress(self, event: QueryProgressEvent) -> None:
    ...        # Do something with event.
    ...        pass
    ...
    ...    def onQueryIdle(self, event: QueryIdleEvent) -> None:
    ...        # Do something with event.
    ...        pass
    ...
    ...    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
    ...        # Do something with event.
    ...        pass
    ...
    >>> spark.streams.addListener(MyListener())
    """

    @abstractmethod
    def onQueryStarted(self, event: "QueryStartedEvent") -> None:
        """
        Called when a query is started.

        Notes
        -----
        This is called synchronously with :py:meth:`~pyspark.sql.streaming.DataStreamWriter.start`,
        that is, `onQueryStart` will be called on all listeners before `DataStreamWriter.start()`
        returns the corresponding :class:`~pyspark.sql.streaming.StreamingQuery`.
        Please don't block this method as it will block your query.
        """
        pass

    @abstractmethod
    def onQueryProgress(self, event: "QueryProgressEvent") -> None:
        """
        Called when there is some status update (ingestion rate updated, etc.)

        Notes
        -----
        This method is asynchronous. The status in :class:`~pyspark.sql.streaming.StreamingQuery`
        will always be latest no matter when this method is called. Therefore, the status of
        :class:`~pyspark.sql.streaming.StreamingQuery`.
        may be changed before/when you process the event. E.g., you may find
        :class:`~pyspark.sql.streaming.StreamingQuery` is terminated when you are
        processing `QueryProgressEvent`.
        """
        pass

    @abstractmethod
    def onQueryIdle(self, event: "QueryIdleEvent") -> None:
        """
        Called when the query is idle and waiting for new data to process.
        """
        pass

    @abstractmethod
    def onQueryTerminated(self, event: "QueryTerminatedEvent") -> None:
        """
        Called when a query is stopped, with or without error.
        """
        pass

class QueryStartedEvent:
    """
    Event representing the start of a query.

    .. versionadded:: 3.5.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, id: str, runId: str, name: Optional[str], timestamp: str) -> None:
        self._id = id
        self._runId = runId
        self._name = name
        self._timestamp = timestamp

    # TODO (wei): change back to UUID? what about connect/StreamingQuery
    @property
    def id(self) -> str:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._id

    @property
    def runId(self) -> str:
        """
        A query id that is unique for every start/restart. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.runId`.
        """
        return self._runId

    @property
    def name(self) -> Optional[str]:
        """
        User-specified name of the query, `None` if not specified.
        """
        return self._name

    @property
    def timestamp(self) -> str:
        """
        The timestamp to start a query.
        """
        return self._timestamp

    @classmethod
    def fromJson(cls, j) -> 'QueryStartedEvent':
        return cls(j["id"], j["runId"], j["name"], j["timestamp"])


class QueryIdleEvent:
    """
    Event representing that query is idle and waiting for new data to process.

    .. versionadded:: 3.5.0

    Notes
    -----
    This API is evolving.
    """

    @property
    def id(self) -> str:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._id

    @property
    def runId(self) -> str:
        """
        A query id that is unique for every start/restart. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.runId`.
        """
        return self._runId

    @property
    def name(self) -> Optional[str]:
        """
        User-specified name of the query, `None` if not specified.
        """
        return self._name

    @property
    def timestamp(self) -> str:
        """
        The timestamp to start a query.
        """
        return self._timestamp

    @classmethod
    def fromJson(cls, j) -> 'QueryStartedEvent':
        return cls(j["id"], j["runId"], j["name"], j["timestamp"])

# class QueryProgressEvent:
#     """
#     Event representing any progress updates in a query.
#
#     .. versionadded:: 3.4.0
#
#     Notes
#     -----
#     This API is evolving.
#     """
#
#     def __init__(self, progress: "StreamingQueryProgress") -> None:
#         self._progress = progress
#
#     @property
#     def progress(self) -> "StreamingQueryProgress":
#         """
#         The query progress updates.
#         """
#         return self._progress
#
#     @classmethod
#     def fromJson(cls, j) -> "QueryProgressEvent":
#         return cls(QueryProgressEvent.fromJson(j["progress"]))
