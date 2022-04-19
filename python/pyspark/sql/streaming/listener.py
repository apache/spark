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
from typing import Optional, Dict, List
from abc import ABC, abstractmethod

from py4j.java_gateway import JavaObject

from pyspark.sql import Row
from pyspark import cloudpickle

__all__ = ["StreamingQueryListener"]


class StreamingQueryListener(ABC):
    """
    Interface for listening to events related to :class:`~pyspark.sql.streaming.StreamingQuery`.

    .. versionadded:: 3.4.0

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
    def onQueryTerminated(self, event: "QueryTerminatedEvent") -> None:
        """
        Called when a query is stopped, with or without error.
        """
        pass

    @property
    def _jlistener(self) -> JavaObject:
        from pyspark import SparkContext

        if hasattr(self, "_jlistenerobj"):
            return self._jlistenerobj

        self._jlistenerobj: JavaObject = (
            SparkContext._jvm.PythonStreamingQueryListenerWrapper(  # type: ignore[union-attr]
                JStreamingQueryListener(self)
            )
        )
        return self._jlistenerobj


class JStreamingQueryListener:
    """
    Python class that implements Java interface by Py4J.
    """

    def __init__(self, pylistener: StreamingQueryListener) -> None:
        self.pylistener = pylistener

    def onQueryStarted(self, jevent: JavaObject) -> None:
        self.pylistener.onQueryStarted(QueryStartedEvent(jevent))

    def onQueryProgress(self, jevent: JavaObject) -> None:
        self.pylistener.onQueryProgress(QueryProgressEvent(jevent))

    def onQueryTerminated(self, jevent: JavaObject) -> None:
        self.pylistener.onQueryTerminated(QueryTerminatedEvent(jevent))

    class Java:
        implements = ["org.apache.spark.sql.streaming.PythonStreamingQueryListener"]


class QueryStartedEvent:
    """
    Event representing the start of a query.

    .. versionadded:: 3.4.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jevent: JavaObject) -> None:
        self._id: uuid.UUID = uuid.UUID(jevent.id().toString())
        self._runId: uuid.UUID = uuid.UUID(jevent.runId().toString())
        self._name: Optional[str] = jevent.name()
        self._timestamp: str = jevent.timestamp()

    @property
    def id(self) -> uuid.UUID:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._id

    @property
    def runId(self) -> uuid.UUID:
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


class QueryProgressEvent:
    """
    Event representing any progress updates in a query.

    .. versionadded:: 3.4.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jevent: JavaObject) -> None:
        self._progress: StreamingQueryProgress = StreamingQueryProgress(jevent.progress())

    @property
    def progress(self) -> "StreamingQueryProgress":
        """
        The query progress updates.
        """
        return self._progress


class QueryTerminatedEvent:
    """
    Event representing that termination of a query.

    .. versionadded:: 3.4.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jevent: JavaObject) -> None:
        self._id: uuid.UUID = uuid.UUID(jevent.id().toString())
        self._runId: uuid.UUID = uuid.UUID(jevent.runId().toString())
        jexception = jevent.exception()
        self._exception: Optional[str] = jexception.get() if jexception.isDefined() else None

    @property
    def id(self) -> uuid.UUID:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._id

    @property
    def runId(self) -> uuid.UUID:
        """
        A query id that is unique for every start/restart. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.runId`.
        """
        return self._runId

    @property
    def exception(self) -> Optional[str]:
        """
        The exception message of the query if the query was terminated
        with an exception. Otherwise, it will be `None`.
        """
        return self._exception


class StreamingQueryProgress:
    """
    .. versionadded:: 3.4.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jprogress: JavaObject) -> None:
        from pyspark import SparkContext

        self._jprogress: JavaObject = jprogress
        self._id: uuid.UUID = uuid.UUID(jprogress.id().toString())
        self._runId: uuid.UUID = uuid.UUID(jprogress.runId().toString())
        self._name: Optional[str] = jprogress.name()
        self._timestamp: str = jprogress.timestamp()
        self._batchId: int = jprogress.batchId()
        self._batchDuration: int = jprogress.batchDuration()
        self._durationMs: Dict[str, int] = dict(jprogress.durationMs())
        self._eventTime: Dict[str, str] = dict(jprogress.eventTime())
        self._stateOperators: List[StateOperatorProgress] = [
            StateOperatorProgress(js) for js in jprogress.stateOperators()
        ]
        self._sources: List[SourceProgress] = [SourceProgress(js) for js in jprogress.sources()]
        self._sink: SinkProgress = SinkProgress(jprogress.sink())

        self._observedMetrics: Dict[str, Row] = {
            k: cloudpickle.loads(
                SparkContext._jvm.PythonSQLUtils.toPyRow(jr)  # type: ignore[union-attr]
            )
            for k, jr in dict(jprogress.observedMetrics()).items()
        }

    @property
    def id(self) -> uuid.UUID:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._id

    @property
    def runId(self) -> uuid.UUID:
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

    @property
    def batchId(self) -> int:
        """
        A unique id for the current batch of data being processed.  Note that in the
        case of retries after a failure a given batchId my be executed more than once.
        Similarly, when there is no data to be processed, the batchId will not be
        incremented.
        """
        return self._batchId

    @property
    def batchDuration(self) -> int:
        """
        The process duration of each batch.
        """
        return self._batchDuration

    @property
    def durationMs(self) -> Dict[str, int]:
        """
        The amount of time taken to perform various operations in milliseconds.
        """
        return self._durationMs

    @property
    def eventTime(self) -> Dict[str, str]:
        """
        Statistics of event time seen in this batch. It may contain the following keys:

        .. code-block:: python

            {
                "max": "2016-12-05T20:54:20.827Z",  # maximum event time seen in this trigger
                "min": "2016-12-05T20:54:20.827Z",  # minimum event time seen in this trigger
                "avg": "2016-12-05T20:54:20.827Z",  # average event time seen in this trigger
                "watermark": "2016-12-05T20:54:20.827Z"  # watermark used in this trigger
            }

        All timestamps are in ISO8601 format, i.e. UTC timestamps.
        """
        return self._eventTime

    @property
    def stateOperators(self) -> List["StateOperatorProgress"]:
        """
        Information about operators in the query that store state.
        """
        return self._stateOperators

    @property
    def sources(self) -> List["SourceProgress"]:
        """
        detailed statistics on data being read from each of the streaming sources.
        """
        return self._sources

    @property
    def sink(self) -> "SinkProgress":
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._sink

    @property
    def observedMetrics(self) -> Dict[str, Row]:
        return self._observedMetrics

    @property
    def numInputRows(self) -> Optional[str]:
        """
        The aggregate (across all sources) number of records processed in a trigger.
        """
        return self._jprogress.numInputRows()

    @property
    def inputRowsPerSecond(self) -> str:
        """
        The aggregate (across all sources) rate of data arriving.
        """
        return self._jprogress.inputRowsPerSecond()

    @property
    def processedRowsPerSecond(self) -> str:
        """
        The aggregate (across all sources) rate at which Spark is processing data..
        """
        return self._jprogress.processedRowsPerSecond()

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        return self._jprogress.json()

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        return self._jprogress.prettyJson()

    def __str__(self) -> str:
        return self.prettyJson


class StateOperatorProgress:
    """
    .. versionadded:: 3.4.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jprogress: JavaObject) -> None:
        self._jprogress: JavaObject = jprogress
        self._operatorName: str = jprogress.operatorName()
        self._numRowsTotal: int = jprogress.numRowsTotal()
        self._numRowsUpdated: int = jprogress.numRowsUpdated()
        self._allUpdatesTimeMs: int = jprogress.allUpdatesTimeMs()
        self._numRowsRemoved: int = jprogress.numRowsRemoved()
        self._allRemovalsTimeMs: int = jprogress.allRemovalsTimeMs()
        self._commitTimeMs: int = jprogress.commitTimeMs()
        self._memoryUsedBytes: int = jprogress.memoryUsedBytes()
        self._numRowsDroppedByWatermark: int = jprogress.numRowsDroppedByWatermark()
        self._numShufflePartitions: int = jprogress.numShufflePartitions()
        self._numStateStoreInstances: int = jprogress.numStateStoreInstances()
        self._customMetrics: Dict[str, int] = dict(jprogress.customMetrics())

    @property
    def operatorName(self) -> str:
        return self._operatorName

    @property
    def numRowsTotal(self) -> int:
        return self._numRowsTotal

    @property
    def numRowsUpdated(self) -> int:
        return self._numRowsUpdated

    @property
    def allUpdatesTimeMs(self) -> int:
        return self._allUpdatesTimeMs

    @property
    def numRowsRemoved(self) -> int:
        return self._numRowsRemoved

    @property
    def allRemovalsTimeMs(self) -> int:
        return self._allRemovalsTimeMs

    @property
    def commitTimeMs(self) -> int:
        return self._commitTimeMs

    @property
    def memoryUsedBytes(self) -> int:
        return self._memoryUsedBytes

    @property
    def numRowsDroppedByWatermark(self) -> int:
        return self._numRowsDroppedByWatermark

    @property
    def numShufflePartitions(self) -> int:
        return self._numShufflePartitions

    @property
    def numStateStoreInstances(self) -> int:
        return self._numStateStoreInstances

    @property
    def customMetrics(self) -> Dict[str, int]:
        return self._customMetrics

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        return self._jprogress.json()

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        return self._jprogress.prettyJson()

    def __str__(self) -> str:
        return self.prettyJson


class SourceProgress:
    """
    .. versionadded:: 3.4.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jprogress: JavaObject) -> None:
        self._jprogress: JavaObject = jprogress
        self._description: str = jprogress.description()
        self._startOffset: str = jprogress.startOffset()
        self._endOffset: str = jprogress.endOffset()
        self._latestOffset: str = jprogress.latestOffset()
        self._numInputRows: int = jprogress.numInputRows()
        self._inputRowsPerSecond: float = jprogress.inputRowsPerSecond()
        self._processedRowsPerSecond: float = jprogress.processedRowsPerSecond()
        self._metrics: Dict[str, str] = dict(jprogress.metrics())

    @property
    def description(self) -> str:
        """
        Description of the source.
        """
        return self._description

    @property
    def startOffset(self) -> str:
        """
        The starting offset for data being read.
        """
        return self._startOffset

    @property
    def endOffset(self) -> str:
        """
        The ending offset for data being read.
        """
        return self._endOffset

    @property
    def latestOffset(self) -> str:
        """
        The latest offset from this source.
        """
        return self._latestOffset

    @property
    def numInputRows(self) -> int:
        """
        The number of records read from this source.
        """
        return self._numInputRows

    @property
    def inputRowsPerSecond(self) -> float:
        """
        The rate at which data is arriving from this source.
        """
        return self._inputRowsPerSecond

    @property
    def processedRowsPerSecond(self) -> float:
        """
        The rate at which data from this source is being processed by Spark.
        """
        return self._processedRowsPerSecond

    @property
    def metrics(self) -> Dict[str, str]:
        return self._metrics

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        return self._jprogress.json()

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        return self._jprogress.prettyJson()

    def __str__(self) -> str:
        return self.prettyJson


class SinkProgress:
    """
    .. versionadded:: 3.4.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jprogress: JavaObject) -> None:
        self._jprogress: JavaObject = jprogress
        self._description: str = jprogress.description()
        self._numOutputRows: int = jprogress.numOutputRows()
        self._metrics: Dict[str, str] = dict(jprogress.metrics())

    @property
    def description(self) -> str:
        """
        Description of the source.
        """
        return self._description

    @property
    def numOutputRows(self) -> int:
        """
        Number of rows written to the sink or -1 for Continuous Mode (temporarily)
        or Sink V1 (until decommissioned).
        """
        return self._numOutputRows

    @property
    def metrics(self) -> Dict[str, str]:
        return self._metrics

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        return self._jprogress.json()

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        return self._jprogress.prettyJson()

    def __str__(self) -> str:
        return self.prettyJson


def _test() -> None:
    import sys
    import doctest
    import os
    from pyspark.sql import SparkSession
    import pyspark.sql.streaming.listener
    from py4j.protocol import Py4JError

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.streaming.listener.__dict__.copy()
    try:
        spark = SparkSession._getActiveSessionOrCreate()
    except Py4JError:  # noqa: F821
        spark = SparkSession(sc)  # type: ignore[name-defined] # noqa: F821

    globs["spark"] = spark

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.streaming.listener,
        globs=globs,
    )
    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
