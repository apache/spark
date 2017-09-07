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

from __future__ import print_function

import os
import sys

from py4j.java_gateway import java_import, JavaObject

from pyspark import RDD, SparkConf
from pyspark.serializers import NoOpSerializer, UTF8Deserializer, CloudPickleSerializer
from pyspark.context import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.dstream import DStream
from pyspark.streaming.util import TransformFunction, TransformFunctionSerializer

__all__ = ["StreamingContext"]


class StreamingContext(object):
    """
    Main entry point for Spark Streaming functionality. A StreamingContext
    represents the connection to a Spark cluster, and can be used to create
    L{DStream} various input sources. It can be from an existing L{SparkContext}.
    After creating and transforming DStreams, the streaming computation can
    be started and stopped using `context.start()` and `context.stop()`,
    respectively. `context.awaitTermination()` allows the current thread
    to wait for the termination of the context by `stop()` or by an exception.
    """
    _transformerSerializer = None

    # Reference to a currently active StreamingContext
    _activeContext = None

    def __init__(self, sparkContext, batchDuration=None, jssc=None):
        """
        Create a new StreamingContext.

        @param sparkContext: L{SparkContext} object.
        @param batchDuration: the time interval (in seconds) at which streaming
                              data will be divided into batches
        """

        self._sc = sparkContext
        self._jvm = self._sc._jvm
        self._jssc = jssc or self._initialize_context(self._sc, batchDuration)

    def _initialize_context(self, sc, duration):
        self._ensure_initialized()
        return self._jvm.JavaStreamingContext(sc._jsc, self._jduration(duration))

    def _jduration(self, seconds):
        """
        Create Duration object given number of seconds
        """
        return self._jvm.Duration(int(seconds * 1000))

    @classmethod
    def _ensure_initialized(cls):
        SparkContext._ensure_initialized()
        gw = SparkContext._gateway

        java_import(gw.jvm, "org.apache.spark.streaming.*")
        java_import(gw.jvm, "org.apache.spark.streaming.api.java.*")
        java_import(gw.jvm, "org.apache.spark.streaming.api.python.*")

        # start callback server
        # getattr will fallback to JVM, so we cannot test by hasattr()
        if "_callback_server" not in gw.__dict__ or gw._callback_server is None:
            gw.callback_server_parameters.eager_load = True
            gw.callback_server_parameters.daemonize = True
            gw.callback_server_parameters.daemonize_connections = True
            gw.callback_server_parameters.port = 0
            gw.start_callback_server(gw.callback_server_parameters)
            cbport = gw._callback_server.server_socket.getsockname()[1]
            gw._callback_server.port = cbport
            # gateway with real port
            gw._python_proxy_port = gw._callback_server.port
            # get the GatewayServer object in JVM by ID
            jgws = JavaObject("GATEWAY_SERVER", gw._gateway_client)
            # update the port of CallbackClient with real port
            jgws.resetCallbackClient(jgws.getCallbackClient().getAddress(), gw._python_proxy_port)

        # register serializer for TransformFunction
        # it happens before creating SparkContext when loading from checkpointing
        cls._transformerSerializer = TransformFunctionSerializer(
            SparkContext._active_spark_context, CloudPickleSerializer(), gw)

    @classmethod
    def getOrCreate(cls, checkpointPath, setupFunc):
        """
        Either recreate a StreamingContext from checkpoint data or create a new StreamingContext.
        If checkpoint data exists in the provided `checkpointPath`, then StreamingContext will be
        recreated from the checkpoint data. If the data does not exist, then the provided setupFunc
        will be used to create a new context.

        @param checkpointPath: Checkpoint directory used in an earlier streaming program
        @param setupFunc:      Function to create a new context and setup DStreams
        """
        cls._ensure_initialized()
        gw = SparkContext._gateway

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

        # update ctx in serializer
        cls._transformerSerializer.ctx = sc
        return StreamingContext(sc, None, jssc)

    @classmethod
    def getActive(cls):
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
                raise Exception("JVM's active JavaStreamingContext is not the JavaStreamingContext "
                                "backing the action Python StreamingContext. This is unexpected.")
        return cls._activeContext

    @classmethod
    def getActiveOrCreate(cls, checkpointPath, setupFunc):
        """
        Either return the active StreamingContext (i.e. currently started but not stopped),
        or recreate a StreamingContext from checkpoint data or create a new StreamingContext
        using the provided setupFunc function. If the checkpointPath is None or does not contain
        valid checkpoint data, then setupFunc will be called to create a new context and setup
        DStreams.

        @param checkpointPath: Checkpoint directory used in an earlier streaming program. Can be
                               None if the intention is to always create a new context when there
                               is no active context.
        @param setupFunc:      Function to create a new JavaStreamingContext and setup DStreams
        """

        if setupFunc is None:
            raise Exception("setupFunc cannot be None")
        activeContext = cls.getActive()
        if activeContext is not None:
            return activeContext
        elif checkpointPath is not None:
            return cls.getOrCreate(checkpointPath, setupFunc)
        else:
            return setupFunc()

    @property
    def sparkContext(self):
        """
        Return SparkContext which is associated with this StreamingContext.
        """
        return self._sc

    def start(self):
        """
        Start the execution of the streams.
        """
        self._jssc.start()
        StreamingContext._activeContext = self

    def awaitTermination(self, timeout=None):
        """
        Wait for the execution to stop.

        @param timeout: time to wait in seconds
        """
        if timeout is None:
            self._jssc.awaitTermination()
        else:
            self._jssc.awaitTerminationOrTimeout(int(timeout * 1000))

    def awaitTerminationOrTimeout(self, timeout):
        """
        Wait for the execution to stop. Return `true` if it's stopped; or
        throw the reported error during the execution; or `false` if the
        waiting time elapsed before returning from the method.

        @param timeout: time to wait in seconds
        """
        return self._jssc.awaitTerminationOrTimeout(int(timeout * 1000))

    def stop(self, stopSparkContext=True, stopGraceFully=False):
        """
        Stop the execution of the streams, with option of ensuring all
        received data has been processed.

        @param stopSparkContext: Stop the associated SparkContext or not
        @param stopGracefully: Stop gracefully by waiting for the processing
                              of all received data to be completed
        """
        self._jssc.stop(stopSparkContext, stopGraceFully)
        StreamingContext._activeContext = None
        if stopSparkContext:
            self._sc.stop()

    def remember(self, duration):
        """
        Set each DStreams in this context to remember RDDs it generated
        in the last given duration. DStreams remember RDDs only for a
        limited duration of time and releases them for garbage collection.
        This method allows the developer to specify how to long to remember
        the RDDs (if the developer wishes to query old data outside the
        DStream computation).

        @param duration: Minimum duration (in seconds) that each DStream
                        should remember its RDDs
        """
        self._jssc.remember(self._jduration(duration))

    def checkpoint(self, directory):
        """
        Sets the context to periodically checkpoint the DStream operations for master
        fault-tolerance. The graph will be checkpointed every batch interval.

        @param directory: HDFS-compatible directory where the checkpoint data
                         will be reliably stored
        """
        self._jssc.checkpoint(directory)

    def socketTextStream(self, hostname, port, storageLevel=StorageLevel.MEMORY_AND_DISK_2):
        """
        Create an input from TCP source hostname:port. Data is received using
        a TCP socket and receive byte is interpreted as UTF8 encoded ``\\n`` delimited
        lines.

        @param hostname:      Hostname to connect to for receiving data
        @param port:          Port to connect to for receiving data
        @param storageLevel:  Storage level to use for storing the received objects
        """
        jlevel = self._sc._getJavaStorageLevel(storageLevel)
        return DStream(self._jssc.socketTextStream(hostname, port, jlevel), self,
                       UTF8Deserializer())

    def textFileStream(self, directory):
        """
        Create an input stream that monitors a Hadoop-compatible file system
        for new files and reads them as text files. Files must be wrriten to the
        monitored directory by "moving" them from another location within the same
        file system. File names starting with . are ignored.
        """
        return DStream(self._jssc.textFileStream(directory), self, UTF8Deserializer())

    def binaryRecordsStream(self, directory, recordLength):
        """
        Create an input stream that monitors a Hadoop-compatible file system
        for new files and reads them as flat binary files with records of
        fixed length. Files must be written to the monitored directory by "moving"
        them from another location within the same file system.
        File names starting with . are ignored.

        @param directory:       Directory to load data from
        @param recordLength:    Length of each record in bytes
        """
        return DStream(self._jssc.binaryRecordsStream(directory, recordLength), self,
                       NoOpSerializer())

    def _check_serializers(self, rdds):
        # make sure they have same serializer
        if len(set(rdd._jrdd_deserializer for rdd in rdds)) > 1:
            for i in range(len(rdds)):
                # reset them to sc.serializer
                rdds[i] = rdds[i]._reserialize()

    def queueStream(self, rdds, oneAtATime=True, default=None):
        """
        Create an input stream from an queue of RDDs or list. In each batch,
        it will process either one or all of the RDDs returned by the queue.

        .. note:: Changes to the queue after the stream is created will not be recognized.

        @param rdds:       Queue of RDDs
        @param oneAtATime: pick one rdd each time or pick all of them once.
        @param default:    The default rdd if no more in rdds
        """
        if default and not isinstance(default, RDD):
            default = self._sc.parallelize(default)

        if not rdds and default:
            rdds = [rdds]

        if rdds and not isinstance(rdds[0], RDD):
            rdds = [self._sc.parallelize(input) for input in rdds]
        self._check_serializers(rdds)

        queue = self._jvm.PythonDStream.toRDDQueue([r._jrdd for r in rdds])
        if default:
            default = default._reserialize(rdds[0]._jrdd_deserializer)
            jdstream = self._jssc.queueStream(queue, oneAtATime, default._jrdd)
        else:
            jdstream = self._jssc.queueStream(queue, oneAtATime)
        return DStream(jdstream, self, rdds[0]._jrdd_deserializer)

    def transform(self, dstreams, transformFunc):
        """
        Create a new DStream in which each RDD is generated by applying
        a function on RDDs of the DStreams. The order of the JavaRDDs in
        the transform function parameter will be the same as the order
        of corresponding DStreams in the list.
        """
        jdstreams = [d._jdstream for d in dstreams]
        # change the final serializer to sc.serializer
        func = TransformFunction(self._sc,
                                 lambda t, *rdds: transformFunc(rdds).map(lambda x: x),
                                 *[d._jrdd_deserializer for d in dstreams])
        jfunc = self._jvm.TransformFunction(func)
        jdstream = self._jssc.transform(jdstreams, jfunc)
        return DStream(jdstream, self, self._sc.serializer)

    def union(self, *dstreams):
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
        first = dstreams[0]
        jrest = [d._jdstream for d in dstreams[1:]]
        return DStream(self._jssc.union(first._jdstream, jrest), self, first._jrdd_deserializer)

    def addStreamingListener(self, streamingListener):
        """
        Add a [[org.apache.spark.streaming.scheduler.StreamingListener]] object for
        receiving system events related to streaming.
        """
        self._jssc.addStreamingListener(self._jvm.JavaStreamingListenerWrapper(
            self._jvm.PythonStreamingListenerWrapper(streamingListener)))
