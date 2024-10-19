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
from typing import Any, Callable, List, Optional, TypeVar

from py4j.java_gateway import java_import, is_instance_of, JavaObject

from pyspark import RDD, SparkConf
from pyspark.serializers import NoOpSerializer, UTF8Deserializer, CloudPickleSerializer
from pyspark.core.context import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.dstream import DStream
from pyspark.streaming.listener import StreamingListener
from pyspark.streaming.util import TransformFunction, TransformFunctionSerializer

import warnings

__all__ = ["StreamingContext"]

T = TypeVar("T")


class StreamingContext:
    """
    Main entry point for Spark Streaming functionality. A StreamingContext
    represents the connection to a Spark cluster, and can be used to create
    :class:`DStream` various input sources. It can be from an existing :class:`SparkContext`.
    After creating and transforming DStreams, the streaming computation can
    be started and stopped using `context.start()` and `context.stop()`,
    respectively. `context.awaitTermination()` allows the current thread
    to wait for the termination of the context by `stop()` or by an exception.

    .. deprecated:: Spark 3.4.0
       This is deprecated as of Spark 3.4.0.
       There are no longer updates to DStream and it's a legacy project.
       There is a newer and easier to use streaming engine in Spark called Structured Streaming.
       You should use Spark Structured Streaming for your streaming applications.

    Parameters
    ----------
    sparkContext : :class:`SparkContext`
        SparkContext object.
    batchDuration : int, optional
        the time interval (in seconds) at which streaming
        data will be divided into batches
    """

    _transformerSerializer = None

    # Reference to a currently active StreamingContext
    _activeContext = None

    def __init__(
        self,
        sparkContext: SparkContext,
        batchDuration: Optional[int] = None,
        jssc: Optional[JavaObject] = None,
    ):
        warnings.warn(
            "DStream is deprecated as of Spark 3.4.0. Migrate to Structured Streaming.",
            FutureWarning,
        )
        self._sc = sparkContext
        self._jvm = self._sc._jvm
        self._jssc = jssc or self._initialize_context(self._sc, batchDuration)

    def _initialize_context(self, sc: SparkContext, duration: Optional[int]) -> JavaObject:
        self._ensure_initialized()
        assert self._jvm is not None and duration is not None
        return self._jvm.JavaStreamingContext(sc._jsc, self._jduration(duration))

    def _jduration(self, seconds: int) -> JavaObject:
        """
        Create Duration object given number of seconds
        """
        assert self._jvm is not None
        return self._jvm.Duration(int(seconds * 1000))

    @classmethod
    def _ensure_initialized(cls) -> None:
        SparkContext._ensure_initialized()
        gw = SparkContext._gateway

        assert gw is not None

        java_import(gw.jvm, "org.apache.spark.streaming.*")
        java_import(gw.jvm, "org.apache.spark.streaming.api.java.*")
        java_import(gw.jvm, "org.apache.spark.streaming.api.python.*")

        from pyspark.java_gateway import ensure_callback_server_started

        ensure_callback_server_started(gw)

        # register serializer for TransformFunction
        # it happens before creating SparkContext when loading from checkpointing
        cls._transformerSerializer = TransformFunctionSerializer(
            SparkContext._active_spark_context,
            CloudPickleSerializer(),
            gw,
        )

    @classmethod
    def getOrCreate(
        cls, checkpointPath: str, setupFunc: Callable[[], "StreamingContext"]
    ) -> "StreamingContext":
        """
        Either recreate a StreamingContext from checkpoint data or create a new StreamingContext.
        If checkpoint data exists in the provided `checkpointPath`, then StreamingContext will be
        recreated from the checkpoint data. If the data does not exist, then the provided setupFunc
        will be used to create a new context.

        Parameters
        ----------
        checkpointPath : str
            Checkpoint directory used in an earlier streaming program
        setupFunc : function
            Function to create a new context and setup DStreams
        """
        cls._ensure_initialized()
        gw = SparkContext._gateway

        assert gw is not None

        # Check whether valid checkpoint information exists in the given path
        ssc_option = gw.jvm.StreamingContextPythonHelper().tryRecoverFromCheckpoint(checkpointPath)
        if ssc_option.isEmpty():
            ssc = setupFunc()
            ssc.checkpoint(checkpointPath)
            return ssc

        jssc = gw.jvm.JavaStreamingContext(ssc_option.get())

        # If there is already an active instance of Python SparkContext use it, or create a new one
        if not SparkContext._active_spark_context:
            jsc = jssc.sparkContext()
            conf = SparkConf(_jconf=jsc.getConf())
            SparkContext(conf=conf, gateway=gw, jsc=jsc)

        sc = SparkContext._active_spark_context

        assert sc is not None

        # update ctx in serializer
        assert cls._transformerSerializer is not None
        cls._transformerSerializer.ctx = sc
        return StreamingContext(sc, None, jssc)

    @classmethod
    def getActive(cls) -> Optional["StreamingContext"]:
        """
        Return either the currently active StreamingContext (i.e., if there is a context started
        but not stopped) or None.
        """
        activePythonContext = cls._activeContext
        if activePythonContext is not None:
            # Verify that the current running Java StreamingContext is active and is the same one
            # backing the supposedly active Python context
            activePythonContextJavaId = activePythonContext._jssc.ssc().hashCode()
            activeJvmContextOption = activePythonContext._jvm.StreamingContext.getActive()

            if activeJvmContextOption.isEmpty():
                cls._activeContext = None
            elif activeJvmContextOption.get().hashCode() != activePythonContextJavaId:
                cls._activeContext = None
                raise RuntimeError(
                    "JVM's active JavaStreamingContext is not the JavaStreamingContext "
                    "backing the action Python StreamingContext. This is unexpected."
                )
        return cls._activeContext

    @classmethod
    def getActiveOrCreate(
        cls, checkpointPath: str, setupFunc: Callable[[], "StreamingContext"]
    ) -> "StreamingContext":
        """
        Either return the active StreamingContext (i.e. currently started but not stopped),
        or recreate a StreamingContext from checkpoint data or create a new StreamingContext
        using the provided setupFunc function. If the checkpointPath is None or does not contain
        valid checkpoint data, then setupFunc will be called to create a new context and setup
        DStreams.

        Parameters
        ----------
        checkpointPath : str
            Checkpoint directory used in an earlier streaming program. Can be
            None if the intention is to always create a new context when there
            is no active context.
        setupFunc : function
            Function to create a new JavaStreamingContext and setup DStreams
        """

        if not callable(setupFunc):
            raise TypeError("setupFunc should be callable.")
        activeContext = cls.getActive()
        if activeContext is not None:
            return activeContext
        elif checkpointPath is not None:
            return cls.getOrCreate(checkpointPath, setupFunc)
        else:
            return setupFunc()

    @property
    def sparkContext(self) -> SparkContext:
        """
        Return SparkContext which is associated with this StreamingContext.
        """
        return self._sc

    def start(self) -> None:
        """
        Start the execution of the streams.
        """
        self._jssc.start()
        StreamingContext._activeContext = self

    def awaitTermination(self, timeout: Optional[int] = None) -> None:
        """
        Wait for the execution to stop.

        Parameters
        ----------
        timeout : int, optional
            time to wait in seconds
        """
        if timeout is None:
            self._jssc.awaitTermination()
        else:
            self._jssc.awaitTerminationOrTimeout(int(timeout * 1000))

    def awaitTerminationOrTimeout(self, timeout: int) -> None:
        """
        Wait for the execution to stop. Return `true` if it's stopped; or
        throw the reported error during the execution; or `false` if the
        waiting time elapsed before returning from the method.

        Parameters
        ----------
        timeout : int
            time to wait in seconds
        """
        return self._jssc.awaitTerminationOrTimeout(int(timeout * 1000))

    def stop(self, stopSparkContext: bool = True, stopGraceFully: bool = False) -> None:
        """
        Stop the execution of the streams, with option of ensuring all
        received data has been processed.

        Parameters
        ----------
        stopSparkContext : bool, optional
            Stop the associated SparkContext or not
        stopGracefully : bool, optional
            Stop gracefully by waiting for the processing of all received
            data to be completed
        """
        self._jssc.stop(stopSparkContext, stopGraceFully)
        StreamingContext._activeContext = None
        if stopSparkContext:
            self._sc.stop()

    def remember(self, duration: int) -> None:
        """
        Set each DStreams in this context to remember RDDs it generated
        in the last given duration. DStreams remember RDDs only for a
        limited duration of time and releases them for garbage collection.
        This method allows the developer to specify how long to remember
        the RDDs (if the developer wishes to query old data outside the
        DStream computation).

        Parameters
        ----------
        duration : int
            Minimum duration (in seconds) that each DStream should remember its RDDs
        """
        self._jssc.remember(self._jduration(duration))

    def checkpoint(self, directory: str) -> None:
        """
        Sets the context to periodically checkpoint the DStream operations for master
        fault-tolerance. The graph will be checkpointed every batch interval.

        Parameters
        ----------
        directory : str
            HDFS-compatible directory where the checkpoint data will be reliably stored
        """
        self._jssc.checkpoint(directory)

    def socketTextStream(
        self, hostname: str, port: int, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2
    ) -> "DStream[str]":
        """
        Create an input from TCP source hostname:port. Data is received using
        a TCP socket and receive byte is interpreted as UTF8 encoded ``\\n`` delimited
        lines.

        Parameters
        ----------
        hostname : str
            Hostname to connect to for receiving data
        port : int
            Port to connect to for receiving data
        storageLevel : :class:`pyspark.StorageLevel`, optional
            Storage level to use for storing the received objects
        """
        jlevel = self._sc._getJavaStorageLevel(storageLevel)
        return DStream(
            self._jssc.socketTextStream(hostname, port, jlevel), self, UTF8Deserializer()
        )

    def textFileStream(self, directory: str) -> "DStream[str]":
        """
        Create an input stream that monitors a Hadoop-compatible file system
        for new files and reads them as text files. Files must be written to the
        monitored directory by "moving" them from another location within the same
        file system. File names starting with . are ignored.
        The text files must be encoded as UTF-8.
        """
        return DStream(self._jssc.textFileStream(directory), self, UTF8Deserializer())

    def binaryRecordsStream(self, directory: str, recordLength: int) -> "DStream[bytes]":
        """
        Create an input stream that monitors a Hadoop-compatible file system
        for new files and reads them as flat binary files with records of
        fixed length. Files must be written to the monitored directory by "moving"
        them from another location within the same file system.
        File names starting with . are ignored.

        Parameters
        ----------
        directory : str
            Directory to load data from
        recordLength : int
            Length of each record in bytes
        """
        return DStream(
            self._jssc.binaryRecordsStream(directory, recordLength), self, NoOpSerializer()
        )

    def _check_serializers(self, rdds: List[RDD[T]]) -> None:
        # make sure they have same serializer
        if len(set(rdd._jrdd_deserializer for rdd in rdds)) > 1:
            for i in range(len(rdds)):
                # reset them to sc.serializer
                rdds[i] = rdds[i]._reserialize()

    def queueStream(
        self,
        rdds: List[RDD[T]],
        oneAtATime: bool = True,
        default: Optional[RDD[T]] = None,
    ) -> "DStream[T]":
        """
        Create an input stream from a queue of RDDs or list. In each batch,
        it will process either one or all of the RDDs returned by the queue.

        Parameters
        ----------
        rdds : list
            Queue of RDDs
        oneAtATime : bool, optional
            pick one rdd each time or pick all of them once.
        default : :class:`pyspark.RDD`, optional
            The default rdd if no more in rdds

        Notes
        -----
        Changes to the queue after the stream is created will not be recognized.
        """
        if default and not isinstance(default, RDD):
            default = self._sc.parallelize(default)

        if not rdds and default:
            rdds = [rdds]  # type: ignore[list-item]

        if rdds and not isinstance(rdds[0], RDD):
            rdds = [self._sc.parallelize(input) for input in rdds]
        self._check_serializers(rdds)

        assert self._jvm is not None
        queue = self._jvm.PythonDStream.toRDDQueue([r._jrdd for r in rdds])
        if default:
            default = default._reserialize(rdds[0]._jrdd_deserializer)
            assert default is not None
            jdstream = self._jssc.queueStream(queue, oneAtATime, default._jrdd)
        else:
            jdstream = self._jssc.queueStream(queue, oneAtATime)
        return DStream(jdstream, self, rdds[0]._jrdd_deserializer)

    def transform(
        self, dstreams: List["DStream[Any]"], transformFunc: Callable[..., RDD[T]]
    ) -> "DStream[T]":
        """
        Create a new DStream in which each RDD is generated by applying
        a function on RDDs of the DStreams. The order of the JavaRDDs in
        the transform function parameter will be the same as the order
        of corresponding DStreams in the list.
        """
        jdstreams = [d._jdstream for d in dstreams]
        # change the final serializer to sc.serializer
        func = TransformFunction(
            self._sc,
            lambda t, *rdds: transformFunc(rdds),
            *[d._jrdd_deserializer for d in dstreams],
        )

        assert self._jvm is not None
        jfunc = self._jvm.TransformFunction(func)
        jdstream = self._jssc.transform(jdstreams, jfunc)
        return DStream(jdstream, self, self._sc.serializer)

    def union(self, *dstreams: "DStream[T]") -> "DStream[T]":
        """
        Create a unified DStream from multiple DStreams of the same
        type and same slide duration.
        """
        if not dstreams:
            raise ValueError("should have at least one DStream to union")
        if len(dstreams) == 1:
            return dstreams[0]
        if len(set(s._jrdd_deserializer for s in dstreams)) > 1:
            raise ValueError("All DStreams should have same serializer")
        if len(set(s._slideDuration for s in dstreams)) > 1:
            raise ValueError("All DStreams should have same slide duration")

        assert SparkContext._jvm is not None
        jdstream_cls = SparkContext._jvm.org.apache.spark.streaming.api.java.JavaDStream
        jpair_dstream_cls = SparkContext._jvm.org.apache.spark.streaming.api.java.JavaPairDStream
        gw = SparkContext._gateway
        if is_instance_of(gw, dstreams[0]._jdstream, jdstream_cls):
            cls = jdstream_cls
        elif is_instance_of(gw, dstreams[0]._jdstream, jpair_dstream_cls):
            cls = jpair_dstream_cls
        else:
            cls_name = dstreams[0]._jdstream.getClass().getCanonicalName()
            raise TypeError("Unsupported Java DStream class %s" % cls_name)

        assert gw is not None
        jdstreams = gw.new_array(cls, len(dstreams))
        for i in range(0, len(dstreams)):
            jdstreams[i] = dstreams[i]._jdstream
        return DStream(
            self._jssc.union(jdstreams),
            self,
            dstreams[0]._jrdd_deserializer,
        )

    def addStreamingListener(self, streamingListener: StreamingListener) -> None:
        """
        Add a [[org.apache.spark.streaming.scheduler.StreamingListener]] object for
        receiving system events related to streaming.
        """
        assert self._jvm is not None
        self._jssc.addStreamingListener(
            self._jvm.JavaStreamingListenerWrapper(
                self._jvm.PythonStreamingListenerWrapper(streamingListener)
            )
        )
