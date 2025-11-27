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
import json
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING
from abc import ABC, abstractmethod

from pyspark.sql import Row
from pyspark import cloudpickle

__all__ = ["StreamingQueryListener"]

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject


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

    def _set_spark_session(
        self, session: "SparkSession"  # type: ignore[name-defined] # noqa: F821
    ) -> None:
        if self.spark is None:
            self.spark = session

    @property
    def spark(self) -> Optional["SparkSession"]:  # type: ignore[name-defined] # noqa: F821
        return getattr(self, "_sparkSession", None)

    @spark.setter
    def spark(self, session: "SparkSession") -> None:  # type: ignore[name-defined] # noqa: F821
        # For backward compatibility
        self._sparkSession = session

    def _init_listener_id(self) -> None:
        self._id = str(uuid.uuid4())

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

    # NOTE: Do not mark this as abstract method, since we released this abstract class without
    # this method in prior version and marking this as abstract method would break existing
    # implementations.
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

    @property
    def _jlistener(self) -> "JavaObject":
        from pyspark import SparkContext

        if hasattr(self, "_jlistenerobj"):
            return self._jlistenerobj

        self._jlistenerobj: "JavaObject" = (
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

    def onQueryStarted(self, jevent: "JavaObject") -> None:
        self.pylistener.onQueryStarted(QueryStartedEvent.fromJObject(jevent))

    def onQueryProgress(self, jevent: "JavaObject") -> None:
        self.pylistener.onQueryProgress(QueryProgressEvent.fromJObject(jevent))

    def onQueryIdle(self, jevent: "JavaObject") -> None:
        self.pylistener.onQueryIdle(QueryIdleEvent.fromJObject(jevent))

    def onQueryTerminated(self, jevent: "JavaObject") -> None:
        self.pylistener.onQueryTerminated(QueryTerminatedEvent.fromJObject(jevent))

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

    def __init__(
        self,
        id: uuid.UUID,
        runId: uuid.UUID,
        name: Optional[str],
        timestamp: str,
        jobTags: Set[str],
    ) -> None:
        self._id: uuid.UUID = id
        self._runId: uuid.UUID = runId
        self._name: Optional[str] = name
        self._timestamp: str = timestamp
        self._jobTags: Set[str] = jobTags

    @classmethod
    def fromJObject(cls, jevent: "JavaObject") -> "QueryStartedEvent":
        job_tags = set()
        java_iterator = jevent.jobTags().iterator()
        while java_iterator.hasNext():
            job_tags.add(java_iterator.next().toString())

        return cls(
            id=uuid.UUID(jevent.id().toString()),
            runId=uuid.UUID(jevent.runId().toString()),
            name=jevent.name(),
            timestamp=jevent.timestamp(),
            jobTags=job_tags,
        )

    @classmethod
    def fromJson(cls, j: Dict[str, Any]) -> "QueryStartedEvent":
        # Json4s will convert jobTags to a list, so we need to convert it back to a set.
        job_tags = j["jobTags"] if "jobTags" in j else []
        return cls(
            id=uuid.UUID(j["id"]),
            runId=uuid.UUID(j["runId"]),
            name=j["name"],
            timestamp=j["timestamp"],
            jobTags=set(job_tags),
        )

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
    def jobTags(self) -> Set[str]:
        """
        The job tags of the query.
        """
        return self._jobTags


class QueryProgressEvent:
    """
    Event representing any progress updates in a query.

    .. versionadded:: 3.4.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, progress: "StreamingQueryProgress") -> None:
        self._progress: StreamingQueryProgress = progress

    @classmethod
    def fromJObject(cls, jevent: "JavaObject") -> "QueryProgressEvent":
        return cls(progress=StreamingQueryProgress.fromJObject(jevent.progress()))

    @classmethod
    def fromJson(cls, j: Dict[str, Any]) -> "QueryProgressEvent":
        return cls(progress=StreamingQueryProgress.fromJson(j["progress"]))

    @property
    def progress(self) -> "StreamingQueryProgress":
        """
        The query progress updates.
        """
        return self._progress


class QueryIdleEvent:
    """
    Event representing that query is idle and waiting for new data to process.

    .. versionadded:: 3.5.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, id: uuid.UUID, runId: uuid.UUID, timestamp: str) -> None:
        self._id: uuid.UUID = id
        self._runId: uuid.UUID = runId
        self._timestamp: str = timestamp

    @classmethod
    def fromJObject(cls, jevent: "JavaObject") -> "QueryIdleEvent":
        return cls(
            id=uuid.UUID(jevent.id().toString()),
            runId=uuid.UUID(jevent.runId().toString()),
            timestamp=jevent.timestamp(),
        )

    @classmethod
    def fromJson(cls, j: Dict[str, Any]) -> "QueryIdleEvent":
        return cls(id=uuid.UUID(j["id"]), runId=uuid.UUID(j["runId"]), timestamp=j["timestamp"])

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
    def timestamp(self) -> str:
        """
        The timestamp when the latest no-batch trigger happened.
        """
        return self._timestamp


class QueryTerminatedEvent:
    """
    Event representing that termination of a query.

    .. versionadded:: 3.4.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(
        self,
        id: uuid.UUID,
        runId: uuid.UUID,
        exception: Optional[str],
        errorClassOnException: Optional[str],
    ) -> None:
        self._id: uuid.UUID = id
        self._runId: uuid.UUID = runId
        self._exception: Optional[str] = exception
        self._errorClassOnException: Optional[str] = errorClassOnException

    @classmethod
    def fromJObject(cls, jevent: "JavaObject") -> "QueryTerminatedEvent":
        jexception = jevent.exception()
        jerrorclass = jevent.errorClassOnException()
        return cls(
            id=uuid.UUID(jevent.id().toString()),
            runId=uuid.UUID(jevent.runId().toString()),
            exception=jexception.get() if jexception.isDefined() else None,
            errorClassOnException=jerrorclass.get() if jerrorclass.isDefined() else None,
        )

    @classmethod
    def fromJson(cls, j: Dict[str, Any]) -> "QueryTerminatedEvent":
        return cls(
            id=uuid.UUID(j["id"]),
            runId=uuid.UUID(j["runId"]),
            exception=j["exception"],
            errorClassOnException=j["errorClassOnException"],
        )

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

    @property
    def errorClassOnException(self) -> Optional[str]:
        """
        The error class from the exception if the query was terminated
        with an exception which is a part of error class framework.
        If the query was terminated without an exception, or the
        exception is not a part of error class framework, it will be
        `None`.

        .. versionadded:: 3.5.0
        """
        return self._errorClassOnException


class StreamingQueryProgress(dict):
    """
    .. versionadded:: 3.4.0

    .. versionchanged:: 4.0.0
        Becomes a subclass of dict

    Notes
    -----
    This API is evolving.
    """

    def __init__(
        self,
        id: uuid.UUID,
        runId: uuid.UUID,
        name: Optional[str],
        timestamp: str,
        batchId: int,
        batchDuration: int,
        durationMs: Dict[str, int],
        eventTime: Dict[str, str],
        stateOperators: List["StateOperatorProgress"],
        sources: List["SourceProgress"],
        sink: "SinkProgress",
        numInputRows: int,
        inputRowsPerSecond: float,
        processedRowsPerSecond: float,
        observedMetrics: Dict[str, Row],
        jprogress: Optional["JavaObject"] = None,
        jdict: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            id=id,
            runId=runId,
            name=name,
            timestamp=timestamp,
            batchId=batchId,
            batchDuration=batchDuration,
            durationMs=durationMs,
            eventTime=eventTime,
            stateOperators=stateOperators,
            sources=sources,
            sink=sink,
            numInputRows=numInputRows,
            inputRowsPerSecond=inputRowsPerSecond,
            processedRowsPerSecond=processedRowsPerSecond,
            observedMetrics=observedMetrics,
        )
        self._jprogress: Optional["JavaObject"] = jprogress
        self._jdict: Optional[Dict[str, Any]] = jdict

    @classmethod
    def fromJObject(cls, jprogress: "JavaObject") -> "StreamingQueryProgress":
        from pyspark import SparkContext

        return cls(
            jprogress=jprogress,
            id=uuid.UUID(jprogress.id().toString()),
            runId=uuid.UUID(jprogress.runId().toString()),
            name=jprogress.name(),
            timestamp=jprogress.timestamp(),
            batchId=jprogress.batchId(),
            batchDuration=jprogress.batchDuration(),
            durationMs=dict(jprogress.durationMs()),
            eventTime=dict(jprogress.eventTime()),
            stateOperators=[
                StateOperatorProgress.fromJObject(js) for js in jprogress.stateOperators()
            ],
            sources=[SourceProgress.fromJObject(js) for js in jprogress.sources()],
            sink=SinkProgress.fromJObject(jprogress.sink()),
            numInputRows=jprogress.numInputRows(),
            inputRowsPerSecond=jprogress.inputRowsPerSecond(),
            processedRowsPerSecond=jprogress.processedRowsPerSecond(),
            observedMetrics={
                k: cloudpickle.loads(
                    SparkContext._jvm.PythonSQLUtils.toPyRow(jr)  # type: ignore[union-attr]
                )
                for k, jr in dict(jprogress.observedMetrics()).items()
            },
        )

    @classmethod
    def fromJson(cls, j: Dict[str, Any]) -> "StreamingQueryProgress":
        return cls(
            jdict=j,
            id=uuid.UUID(j["id"]),
            runId=uuid.UUID(j["runId"]),
            name=j["name"],
            timestamp=j["timestamp"],
            batchId=j["batchId"],
            batchDuration=j["batchDuration"],
            durationMs=dict(j["durationMs"]) if "durationMs" in j else {},
            eventTime=dict(j["eventTime"]) if "eventTime" in j else {},
            stateOperators=[StateOperatorProgress.fromJson(s) for s in j["stateOperators"]],
            sources=[SourceProgress.fromJson(s) for s in j["sources"]],
            sink=SinkProgress.fromJson(j["sink"]),
            numInputRows=j["numInputRows"] if "numInputRows" in j else None,
            inputRowsPerSecond=j["inputRowsPerSecond"] if "inputRowsPerSecond" in j else None,
            processedRowsPerSecond=j["processedRowsPerSecond"]
            if "processedRowsPerSecond" in j
            else None,
            observedMetrics={
                k: Row(*row_dict.keys())(*row_dict.values())  # Assume no nested rows
                for k, row_dict in j["observedMetrics"].items()
            }
            if "observedMetrics" in j
            else {},
        )

    @property
    def id(self) -> uuid.UUID:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        # Before Spark 4.0, StreamingQuery.lastProgress returns a dict, which casts id and runId
        # to string. But here they are UUID.
        # To prevent breaking change, do not cast them to string when accessed with attribute.
        return super().__getitem__("id")

    @property
    def runId(self) -> uuid.UUID:
        """
        A query id that is unique for every start/restart. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.runId`.
        """
        # Before Spark 4.0, StreamingQuery.lastProgress returns a dict, which casts id and runId
        # to string. But here they are UUID.
        # To prevent breaking change, do not cast them to string when accessed with attribute.
        return super().__getitem__("runId")

    @property
    def name(self) -> Optional[str]:
        """
        User-specified name of the query, `None` if not specified.
        """
        return self["name"]

    @property
    def timestamp(self) -> str:
        """
        The timestamp to start a query.
        """
        return self["timestamp"]

    @property
    def batchId(self) -> int:
        """
        A unique id for the current batch of data being processed.  Note that in the
        case of retries after a failure a given batchId my be executed more than once.
        Similarly, when there is no data to be processed, the batchId will not be
        incremented.
        """
        return self["batchId"]

    @property
    def batchDuration(self) -> int:
        """
        The process duration of each batch.
        """
        return self["batchDuration"]

    @property
    def durationMs(self) -> Dict[str, int]:
        """
        The amount of time taken to perform various operations in milliseconds.
        """
        return self["durationMs"]

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
        return self["eventTime"]

    @property
    def stateOperators(self) -> List["StateOperatorProgress"]:
        """
        Information about operators in the query that store state.
        """
        return self["stateOperators"]

    @property
    def sources(self) -> List["SourceProgress"]:
        """
        detailed statistics on data being read from each of the streaming sources.
        """
        return self["sources"]

    @property
    def sink(self) -> "SinkProgress":
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self["sink"]

    @property
    def observedMetrics(self) -> Dict[str, Row]:
        return self["observedMetrics"]

    @property
    def numInputRows(self) -> int:
        """
        The aggregate (across all sources) number of records processed in a trigger.
        """
        if self["numInputRows"] is not None:
            return self["numInputRows"]
        else:
            return sum(s.numInputRows for s in self.sources)

    @property
    def inputRowsPerSecond(self) -> float:
        """
        The aggregate (across all sources) rate of data arriving.
        """
        if self["inputRowsPerSecond"] is not None:
            return self["inputRowsPerSecond"]
        else:
            return sum(s.inputRowsPerSecond for s in self.sources)

    @property
    def processedRowsPerSecond(self) -> float:
        """
        The aggregate (across all sources) rate at which Spark is processing data.
        """
        if self["processedRowsPerSecond"] is not None:
            return self["processedRowsPerSecond"]
        else:
            return sum(s.processedRowsPerSecond for s in self.sources)

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        assert self._jdict is not None or self._jprogress is not None
        if self._jprogress:
            return self._jprogress.json()
        else:
            return json.dumps(self._jdict)

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        assert self._jdict is not None or self._jprogress is not None
        if self._jprogress:
            return self._jprogress.prettyJson()
        else:
            return json.dumps(self._jdict, indent=4)

    def __getitem__(self, key: str) -> Any:
        # Before Spark 4.0, StreamingQuery.lastProgress returns a dict, which casts id and runId
        # to string. But here they are UUID.
        # To prevent breaking change, also cast them to string when accessed with __getitem__.
        if key == "id" or key == "runId":
            return str(super().__getitem__(key))
        else:
            return super().__getitem__(key)

    def __str__(self) -> str:
        return self.prettyJson

    def __repr__(self) -> str:
        return self.prettyJson


class StateOperatorProgress(dict):
    """
    .. versionadded:: 3.4.0

    .. versionchanged:: 4.0.0
        Becomes a subclass of dict

    Notes
    -----
    This API is evolving.
    """

    def __init__(
        self,
        operatorName: str,
        numRowsTotal: int,
        numRowsUpdated: int,
        numRowsRemoved: int,
        allUpdatesTimeMs: int,
        allRemovalsTimeMs: int,
        commitTimeMs: int,
        memoryUsedBytes: int,
        numRowsDroppedByWatermark: int,
        numShufflePartitions: int,
        numStateStoreInstances: int,
        customMetrics: Dict[str, int],
        jprogress: Optional["JavaObject"] = None,
        jdict: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            operatorName=operatorName,
            numRowsTotal=numRowsTotal,
            numRowsUpdated=numRowsUpdated,
            numRowsRemoved=numRowsRemoved,
            allUpdatesTimeMs=allUpdatesTimeMs,
            allRemovalsTimeMs=allRemovalsTimeMs,
            commitTimeMs=commitTimeMs,
            memoryUsedBytes=memoryUsedBytes,
            numRowsDroppedByWatermark=numRowsDroppedByWatermark,
            numShufflePartitions=numShufflePartitions,
            numStateStoreInstances=numStateStoreInstances,
            customMetrics=customMetrics,
        )
        self._jprogress: Optional["JavaObject"] = jprogress
        self._jdict: Optional[Dict[str, Any]] = jdict

    @classmethod
    def fromJObject(cls, jprogress: "JavaObject") -> "StateOperatorProgress":
        return cls(
            jprogress=jprogress,
            operatorName=jprogress.operatorName(),
            numRowsTotal=jprogress.numRowsTotal(),
            numRowsUpdated=jprogress.numRowsUpdated(),
            allUpdatesTimeMs=jprogress.allUpdatesTimeMs(),
            numRowsRemoved=jprogress.numRowsRemoved(),
            allRemovalsTimeMs=jprogress.allRemovalsTimeMs(),
            commitTimeMs=jprogress.commitTimeMs(),
            memoryUsedBytes=jprogress.memoryUsedBytes(),
            numRowsDroppedByWatermark=jprogress.numRowsDroppedByWatermark(),
            numShufflePartitions=jprogress.numShufflePartitions(),
            numStateStoreInstances=jprogress.numStateStoreInstances(),
            customMetrics=dict(jprogress.customMetrics()),
        )

    @classmethod
    def fromJson(cls, j: Dict[str, Any]) -> "StateOperatorProgress":
        return cls(
            jdict=j,
            operatorName=j["operatorName"],
            numRowsTotal=j["numRowsTotal"],
            numRowsUpdated=j["numRowsUpdated"],
            numRowsRemoved=j["numRowsRemoved"],
            allUpdatesTimeMs=j["allUpdatesTimeMs"],
            allRemovalsTimeMs=j["allRemovalsTimeMs"],
            commitTimeMs=j["commitTimeMs"],
            memoryUsedBytes=j["memoryUsedBytes"],
            numRowsDroppedByWatermark=j["numRowsDroppedByWatermark"],
            numShufflePartitions=j["numShufflePartitions"],
            numStateStoreInstances=j["numStateStoreInstances"],
            customMetrics=dict(j["customMetrics"]) if "customMetrics" in j else {},
        )

    @property
    def operatorName(self) -> str:
        return self["operatorName"]

    @property
    def numRowsTotal(self) -> int:
        return self["numRowsTotal"]

    @property
    def numRowsUpdated(self) -> int:
        return self["numRowsUpdated"]

    @property
    def allUpdatesTimeMs(self) -> int:
        return self["allUpdatesTimeMs"]

    @property
    def numRowsRemoved(self) -> int:
        return self["numRowsRemoved"]

    @property
    def allRemovalsTimeMs(self) -> int:
        return self["allRemovalsTimeMs"]

    @property
    def commitTimeMs(self) -> int:
        return self["commitTimeMs"]

    @property
    def memoryUsedBytes(self) -> int:
        return self["memoryUsedBytes"]

    @property
    def numRowsDroppedByWatermark(self) -> int:
        return self["numRowsDroppedByWatermark"]

    @property
    def numShufflePartitions(self) -> int:
        return self["numShufflePartitions"]

    @property
    def numStateStoreInstances(self) -> int:
        return self["numStateStoreInstances"]

    @property
    def customMetrics(self) -> dict:
        return self["customMetrics"]

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        assert self._jdict is not None or self._jprogress is not None
        if self._jprogress:
            return self._jprogress.json()
        else:
            return json.dumps(self._jdict)

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        assert self._jdict is not None or self._jprogress is not None
        if self._jprogress:
            return self._jprogress.prettyJson()
        else:
            return json.dumps(self._jdict, indent=4)

    def __str__(self) -> str:
        return self.prettyJson

    def __repr__(self) -> str:
        return self.prettyJson


class SourceProgress(dict):
    """
    .. versionadded:: 3.4.0

    .. versionchanged:: 4.0.0
        Becomes a subclass of dict

    Notes
    -----
    This API is evolving.
    """

    def __init__(
        self,
        description: str,
        startOffset: str,
        endOffset: str,
        latestOffset: str,
        numInputRows: int,
        inputRowsPerSecond: float,
        processedRowsPerSecond: float,
        metrics: Dict[str, str],
        jprogress: Optional["JavaObject"] = None,
        jdict: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(
            description=description,
            startOffset=startOffset,
            endOffset=endOffset,
            latestOffset=latestOffset,
            numInputRows=numInputRows,
            inputRowsPerSecond=inputRowsPerSecond,
            processedRowsPerSecond=processedRowsPerSecond,
            metrics=metrics,
        )
        self._jprogress: Optional["JavaObject"] = jprogress
        self._jdict: Optional[Dict[str, Any]] = jdict

    @classmethod
    def fromJObject(cls, jprogress: "JavaObject") -> "SourceProgress":
        return cls(
            jprogress=jprogress,
            description=jprogress.description(),
            startOffset=str(jprogress.startOffset()),
            endOffset=str(jprogress.endOffset()),
            latestOffset=str(jprogress.latestOffset()),
            numInputRows=jprogress.numInputRows(),
            inputRowsPerSecond=jprogress.inputRowsPerSecond(),
            processedRowsPerSecond=jprogress.processedRowsPerSecond(),
            metrics=dict(jprogress.metrics()),
        )

    @classmethod
    def fromJson(cls, j: Dict[str, Any]) -> "SourceProgress":
        return cls(
            jdict=j,
            description=j["description"],
            startOffset=str(j["startOffset"]),
            endOffset=str(j["endOffset"]),
            latestOffset=str(j["latestOffset"]),
            numInputRows=j["numInputRows"],
            inputRowsPerSecond=j["inputRowsPerSecond"],
            processedRowsPerSecond=j["processedRowsPerSecond"],
            metrics=dict(j["metrics"]) if "metrics" in j else {},
        )

    @property
    def description(self) -> str:
        """
        Description of the source.
        """
        return self["description"]

    @property
    def startOffset(self) -> str:
        """
        The starting offset for data being read.
        """
        return self["startOffset"]

    @property
    def endOffset(self) -> str:
        """
        The ending offset for data being read.
        """
        return self["endOffset"]

    @property
    def latestOffset(self) -> str:
        """
        The latest offset from this source.
        """
        return self["latestOffset"]

    @property
    def numInputRows(self) -> int:
        """
        The number of records read from this source.
        """
        return self["numInputRows"]

    @property
    def inputRowsPerSecond(self) -> float:
        """
        The rate at which data is arriving from this source.
        """
        return self["inputRowsPerSecond"]

    @property
    def processedRowsPerSecond(self) -> float:
        """
        The rate at which data from this source is being processed by Spark.
        """
        return self["processedRowsPerSecond"]

    @property
    def metrics(self) -> dict:
        return self["metrics"]

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        assert self._jdict is not None or self._jprogress is not None
        if self._jprogress:
            return self._jprogress.json()
        else:
            return json.dumps(self._jdict)

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        assert self._jdict is not None or self._jprogress is not None
        if self._jprogress:
            return self._jprogress.prettyJson()
        else:
            return json.dumps(self._jdict, indent=4)

    def __str__(self) -> str:
        return self.prettyJson

    def __repr__(self) -> str:
        return self.prettyJson


class SinkProgress(dict):
    """
    .. versionadded:: 3.4.0

    .. versionchanged:: 4.0.0
        Becomes a subclass of dict

    Notes
    -----
    This API is evolving.
    """

    def __init__(
        self,
        description: str,
        numOutputRows: int,
        metrics: Dict[str, str],
        jprogress: Optional["JavaObject"] = None,
        jdict: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(
            description=description,
            numOutputRows=numOutputRows,
            metrics=metrics,
        )
        self._jprogress: Optional["JavaObject"] = jprogress
        self._jdict: Optional[Dict[str, Any]] = jdict

    @classmethod
    def fromJObject(cls, jprogress: "JavaObject") -> "SinkProgress":
        return cls(
            jprogress=jprogress,
            description=jprogress.description(),
            numOutputRows=jprogress.numOutputRows(),
            metrics=dict(jprogress.metrics()),
        )

    @classmethod
    def fromJson(cls, j: Dict[str, Any]) -> "SinkProgress":
        return cls(
            jdict=j,
            description=j["description"],
            numOutputRows=j["numOutputRows"],
            metrics=dict(j["metrics"]) if "metrics" in j else {},
        )

    @property
    def description(self) -> str:
        """
        Description of the source.
        """
        return self["description"]

    @property
    def numOutputRows(self) -> int:
        """
        Number of rows written to the sink or -1 for Continuous Mode (temporarily)
        or Sink V1 (until decommissioned).
        """
        return self["numOutputRows"]

    @property
    def metrics(self) -> Dict[str, str]:
        return self["metrics"]

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        assert self._jdict is not None or self._jprogress is not None
        if self._jprogress:
            return self._jprogress.json()
        else:
            return json.dumps(self._jdict)

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        assert self._jdict is not None or self._jprogress is not None
        if self._jprogress:
            return self._jprogress.prettyJson()
        else:
            return json.dumps(self._jdict, indent=4)

    def __str__(self) -> str:
        return self.prettyJson

    def __repr__(self) -> str:
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
