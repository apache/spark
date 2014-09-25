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

import sys
from signal import signal, SIGTERM, SIGINT
import atexit
import time

from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer
from pyspark.context import SparkContext
from pyspark.streaming.dstream import DStream
from pyspark.streaming.duration import Duration, Seconds

from py4j.java_collections import ListConverter


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
        @param duration: A L{Duration} object or seconds for SparkStreaming.

        """
        if isinstance(duration, (int, long, float)):
            duration = Seconds(duration)

        self._sc = sparkContext
        self._jvm = self._sc._jvm
        self._start_callback_server()
        self._jssc = self._initialize_context(self._sc, duration)

    def _start_callback_server(self):
        gw = self._sc._gateway
        # getattr will fallback to JVM
        if "_callback_server" not in gw.__dict__:
            gw._start_callback_server(gw._python_proxy_port)

    def _initialize_context(self, sc, duration):
        return self._jvm.JavaStreamingContext(sc._jsc, duration._jduration)

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
        @param timeout: time to wait in milliseconds
        """
        if timeout is None:
            self._jssc.awaitTermination()
        else:
            self._jssc.awaitTermination(timeout)

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
        Set each DStreams in this context to remember RDDs it generated in the last given duration.
        DStreams remember RDDs only for a limited duration of time and releases them for garbage
        collection. This method allows the developer to specify how to long to remember the RDDs (
        if the developer wishes to query old data outside the DStream computation).
        @param duration pyspark.streaming.duration.Duration object or seconds.
               Minimum duration that each DStream should remember its RDDs
        """
        if isinstance(duration, (int, long, float)):
            duration = Seconds(duration)

        self._jssc.remember(duration._jduration)

    # TODO: add storageLevel
    def socketTextStream(self, hostname, port):
        """
        Create an input from TCP source hostname:port. Data is received using
        a TCP socket and receive byte is interpreted as UTF8 encoded '\n' delimited
        lines.
        """
        return DStream(self._jssc.socketTextStream(hostname, port), self, UTF8Deserializer())

    def textFileStream(self, directory):
        """
        Create an input stream that monitors a Hadoop-compatible file system
        for new files and reads them as text files. Files must be wrriten to the
        monitored directory by "moving" them from another location within the same
        file system. File names starting with . are ignored.
        """
        return DStream(self._jssc.textFileStream(directory), self, UTF8Deserializer())

    def _makeStream(self, inputs, numSlices=None):
        """
        This function is only for unittest.
        It requires a list as input, and returns the i_th element at the i_th batch
        under manual clock.
        """
        rdds = [self._sc.parallelize(input, numSlices) for input in inputs]
        jrdds = ListConverter().convert([r._jrdd for r in rdds],
                                        SparkContext._gateway._gateway_client)
        jdstream = self._jvm.PythonDataInputStream(self._jssc, jrdds).asJavaDStream()
        return DStream(jdstream, self, rdds[0]._jrdd_deserializer)
