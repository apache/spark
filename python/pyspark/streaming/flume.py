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
if sys.version >= "3":
    from io import BytesIO
else:
    from StringIO import StringIO
from py4j.java_gateway import Py4JJavaError

from pyspark.storagelevel import StorageLevel
from pyspark.serializers import PairDeserializer, NoOpSerializer, UTF8Deserializer, read_int
from pyspark.streaming import DStream

__all__ = ['FlumeUtils', 'utf8_decoder']


def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s.decode('utf-8')


class FlumeUtils(object):

    @staticmethod
    def createStream(ssc, hostname, port,
                     storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2,
                     enableDecompression=False,
                     bodyDecoder=utf8_decoder):
        """
        Create an input stream that pulls events from Flume.

        :param ssc:  StreamingContext object
        :param hostname:  Hostname of the slave machine to which the flume data will be sent
        :param port:  Port of the slave machine to which the flume data will be sent
        :param storageLevel:  Storage level to use for storing the received objects
        :param enableDecompression:  Should netty server decompress input stream
        :param bodyDecoder:  A function used to decode body (default is utf8_decoder)
        :return: A DStream object
        """
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)

        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader()\
                .loadClass("org.apache.spark.streaming.flume.FlumeUtilsPythonHelper")
            helper = helperClass.newInstance()
            jstream = helper.createStream(ssc._jssc, hostname, port, jlevel, enableDecompression)
        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                FlumeUtils._printErrorMsg(ssc.sparkContext)
            raise e

        return FlumeUtils._toPythonDStream(ssc, jstream, bodyDecoder)

    @staticmethod
    def createPollingStream(ssc, addresses,
                            storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2,
                            maxBatchSize=1000,
                            parallelism=5,
                            bodyDecoder=utf8_decoder):
        """
        Creates an input stream that is to be used with the Spark Sink deployed on a Flume agent.
        This stream will poll the sink for data and will pull events as they are available.

        :param ssc:  StreamingContext object
        :param addresses:  List of (host, port)s on which the Spark Sink is running.
        :param storageLevel:  Storage level to use for storing the received objects
        :param maxBatchSize:  The maximum number of events to be pulled from the Spark sink
                              in a single RPC call
        :param parallelism:  Number of concurrent requests this stream should send to the sink.
                             Note that having a higher number of requests concurrently being pulled
                             will result in this stream using more threads
        :param bodyDecoder:  A function used to decode body (default is utf8_decoder)
        :return: A DStream object
        """
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
        hosts = []
        ports = []
        for (host, port) in addresses:
            hosts.append(host)
            ports.append(port)

        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.flume.FlumeUtilsPythonHelper")
            helper = helperClass.newInstance()
            jstream = helper.createPollingStream(
                ssc._jssc, hosts, ports, jlevel, maxBatchSize, parallelism)
        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                FlumeUtils._printErrorMsg(ssc.sparkContext)
            raise e

        return FlumeUtils._toPythonDStream(ssc, jstream, bodyDecoder)

    @staticmethod
    def _toPythonDStream(ssc, jstream, bodyDecoder):
        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
        stream = DStream(jstream, ssc, ser)

        def func(event):
            headersBytes = BytesIO(event[0]) if sys.version >= "3" else StringIO(event[0])
            headers = {}
            strSer = UTF8Deserializer()
            for i in range(0, read_int(headersBytes)):
                key = strSer.loads(headersBytes)
                value = strSer.loads(headersBytes)
                headers[key] = value
            body = bodyDecoder(event[1])
            return (headers, body)
        return stream.map(func)

    @staticmethod
    def _printErrorMsg(sc):
        print("""
________________________________________________________________________________________________

  Spark Streaming's Flume libraries not found in class path. Try one of the following.

  1. Include the Flume library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-streaming-flume:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-streaming-flume-assembly, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-streaming-flume-assembly.jar> ...

________________________________________________________________________________________________

""" % (sc.version, sc.version))
