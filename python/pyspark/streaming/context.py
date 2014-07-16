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

import os
import shutil
import sys
from threading import Lock
from tempfile import NamedTemporaryFile

from pyspark import accumulators
from pyspark.accumulators import Accumulator
from pyspark.broadcast import Broadcast
from pyspark.conf import SparkConf
from pyspark.files import SparkFiles
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer
from pyspark.storagelevel import StorageLevel
from pyspark.rdd import RDD
from pyspark.context import SparkContext

from py4j.java_collections import ListConverter

from pyspark.streaming.dstream import DStream

class StreamingContext(object):
    """
    Main entry point for Spark Streaming functionality. A StreamingContext represents the
    connection to a Spark cluster, and can be used to create L{RDD}s and
    broadcast variables on that cluster.
    """

    def __init__(self, master=None, appName=None, sparkHome=None, pyFiles=None,
        environment=None, batchSize=1024, serializer=PickleSerializer(), conf=None,
        gateway=None, duration=None):
        """
        Create a new StreamingContext. At least the master and app name and duration
        should be set, either through the named parameters here or through C{conf}.

        @param master: Cluster URL to connect to
               (e.g. mesos://host:port, spark://host:port, local[4]).
        @param appName: A name for your job, to display on the cluster web UI.
        @param sparkHome: Location where Spark is installed on cluster nodes.
        @param pyFiles: Collection of .zip or .py files to send to the cluster
               and add to PYTHONPATH.  These can be paths on the local file
               system or HDFS, HTTP, HTTPS, or FTP URLs.
        @param environment: A dictionary of environment variables to set on
               worker nodes.
        @param batchSize: The number of Python objects represented as a single
               Java object.  Set 1 to disable batching or -1 to use an
               unlimited batch size.
        @param serializer: The serializer for RDDs.
        @param conf: A L{SparkConf} object setting Spark properties.
        @param gateway: Use an existing gateway and JVM, otherwise a new JVM
               will be instatiated.
        @param duration: A L{Duration} Duration for SparkStreaming

        """
        # Create the Python Sparkcontext
        self._sc = SparkContext(master=master, appName=appName, sparkHome=sparkHome,
                        pyFiles=pyFiles, environment=environment, batchSize=batchSize,
                        serializer=serializer, conf=conf, gateway=gateway)
        self._jvm = self._sc._jvm
        self._jssc = self._initialize_context(self._sc._jsc, duration._jduration)

    # Initialize StremaingContext in function to allow subclass specific initialization
    def _initialize_context(self, jspark_context, jduration):
        return self._jvm.JavaStreamingContext(jspark_context, jduration)

    def actorStream(self, props, name, storageLevel, supervisorStrategy):
        raise NotImplementedError

    def addStreamingListener(self, streamingListener):
        raise NotImplementedError

    def awaitTermination(self, timeout=None):
        if timeout:
            self._jssc.awaitTermination(timeout)
        else:
            self._jssc.awaitTermination()

    def checkpoint(self, directory):
        raise NotImplementedError

    def fileStream(self, directory, filter=None, newFilesOnly=None):
        raise NotImplementedError

    def networkStream(self, receiver):
        raise NotImplementedError

    def queueStream(self, queue, oneAtATime=True, defaultRDD=None):
        raise NotImplementedError

    def rawSocketStream(self, hostname, port, storagelevel):
        raise NotImplementedError

    def remember(self, duration):
        raise NotImplementedError

    def socketStream(hostname, port, converter,storageLevel):
        raise NotImplementedError

    def start(self):
        self._jssc.start()

    def stop(self, stopSparkContext=True):
        raise NotImplementedError

    def textFileStream(self, directory):
        return DStream(self._jssc.textFileStream(directory), self, UTF8Deserializer())

    def transform(self, seq):
        raise NotImplementedError

    def union(self, seq):
        raise NotImplementedError

