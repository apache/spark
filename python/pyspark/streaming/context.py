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

from py4j.java_collections import ListConverter
from py4j.java_gateway import java_import

from pyspark import RDD
from pyspark.serializers import UTF8Deserializer
from pyspark.context import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.dstream import DStream
from pyspark.streaming.util import RDDFunction

__all__ = ["StreamingContext"]


def _daemonize_callback_server():
    """
    Hack Py4J to daemonize callback server
    """
    # TODO: create a patch for Py4J
    import socket
    import py4j.java_gateway
    logger = py4j.java_gateway.logger
    from py4j.java_gateway import Py4JNetworkError
    from threading import Thread

    def start(self):
        """Starts the CallbackServer. This method should be called by the
        client instead of run()."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                                      1)
        try:
            self.server_socket.bind((self.address, self.port))
            # self.port = self.server_socket.getsockname()[1]
        except Exception:
            msg = 'An error occurred while trying to start the callback server'
            logger.exception(msg)
            raise Py4JNetworkError(msg)

        # Maybe thread needs to be cleanup up?
        self.thread = Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

    py4j.java_gateway.CallbackServer.start = start


class StreamingContext(object):
    """
    Main entry point for Spark Streaming functionality. A StreamingContext represents the
    connection to a Spark cluster, and can be used to create L{DStream}s and
    broadcast variables on that cluster.
    """

    def __init__(self, sparkContext, duration):
        """
        Create a new StreamingContext. At least the master and app name and duration
        should be set, either through the named parameters here or through C{conf}.

        @param sparkContext: L{SparkContext} object.
        @param duration: seconds for SparkStreaming.

        """
        self._sc = sparkContext
        self._jvm = self._sc._jvm
        self._start_callback_server()
        self._jssc = self._initialize_context(self._sc, duration)

    def _start_callback_server(self):
        gw = self._sc._gateway
        # getattr will fallback to JVM
        if "_callback_server" not in gw.__dict__:
            _daemonize_callback_server()
            gw._start_callback_server(gw._python_proxy_port)
            gw._python_proxy_port = gw._callback_server.port  # update port with real port

    def _initialize_context(self, sc, duration):
        java_import(self._jvm, "org.apache.spark.streaming.*")
        java_import(self._jvm, "org.apache.spark.streaming.api.java.*")
        java_import(self._jvm, "org.apache.spark.streaming.api.python.*")
        return self._jvm.JavaStreamingContext(sc._jsc, self._jduration(duration))

    def _jduration(self, seconds):
        """
        Create Duration object given number of seconds
        """
        return self._jvm.Duration(int(seconds * 1000))

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

    def awaitTermination(self, timeout=None):
        """
        Wait for the execution to stop.
        @param timeout: time to wait in seconds
        """
        if timeout is None:
            self._jssc.awaitTermination()
        else:
            self._jssc.awaitTermination(int(timeout * 1000))

    def stop(self, stopSparkContext=True, stopGraceFully=False):
        """
        Stop the execution of the streams immediately (does not wait for all received data
        to be processed).
        """
        self._jssc.stop(stopSparkContext, stopGraceFully)
        if stopSparkContext:
            self._sc.stop()

    def remember(self, duration):
        """
        Set each DStreams in this context to remember RDDs it generated
        in the last given duration. DStreams remember RDDs only for a
        limited duration of time and releases them for garbage collection.
        This method allows the developer to specify how to long to remember
        the RDDs ( if the developer wishes to query old data outside the
        DStream computation).

        @param duration Minimum duration (in seconds) that each DStream
                        should remember its RDDs
        """
        self._jssc.remember(self._jduration(duration))

    def checkpoint(self, directory):
        """
        Sets the context to periodically checkpoint the DStream operations for master
        fault-tolerance. The graph will be checkpointed every batch interval.

        @param directory HDFS-compatible directory where the checkpoint data
                         will be reliably stored
        """
        self._jssc.checkpoint(directory)

    def socketTextStream(self, hostname, port, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2):
        """
        Create an input from TCP source hostname:port. Data is received using
        a TCP socket and receive byte is interpreted as UTF8 encoded '\n' delimited
        lines.

        @param hostname      Hostname to connect to for receiving data
        @param port          Port to connect to for receiving data
        @param storageLevel  Storage level to use for storing the received objects
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

    def _check_serialzers(self, rdds):
        # make sure they have same serializer
        if len(set(rdd._jrdd_deserializer for rdd in rdds)):
            for i in range(len(rdds)):
                # reset them to sc.serializer
                rdds[i] = rdds[i].map(lambda x: x, preservesPartitioning=True)

    def queueStream(self, queue, oneAtATime=True, default=None):
        """
        Create an input stream from an queue of RDDs or list. In each batch,
        it will process either one or all of the RDDs returned by the queue.

        NOTE: changes to the queue after the stream is created will not be recognized.
        @param queue      Queue of RDDs
        @tparam T         Type of objects in the RDD
        """
        if queue and not isinstance(queue[0], RDD):
            rdds = [self._sc.parallelize(input) for input in queue]
        else:
            rdds = queue
        self._check_serialzers(rdds)
        jrdds = ListConverter().convert([r._jrdd for r in rdds],
                                        SparkContext._gateway._gateway_client)
        queue = self._jvm.PythonDStream.toRDDQueue(jrdds)
        if default:
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
        jdstreams = ListConverter().convert([d._jdstream for d in dstreams],
                                            SparkContext._gateway._gateway_client)
        # change the final serializer to sc.serializer
        jfunc = RDDFunction(self._sc,
                            lambda t, *rdds: transformFunc(rdds).map(lambda x: x),
                            *[d._jrdd_deserializer for d in dstreams])

        jdstream = self._jvm.PythonDStream.callTransform(self._jssc, jdstreams, jfunc)
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
        first = dstreams[0]
        jrest = ListConverter().convert([d._jdstream for d in dstreams[1:]],
                                        SparkContext._gateway._gateway_client)
        return DStream(self._jssc.union(first._jdstream, jrest), self, first._jrdd_deserializer)
