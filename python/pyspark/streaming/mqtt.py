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


from py4j.java_gateway import java_import, Py4JError

from pyspark.storagelevel import StorageLevel
from pyspark.serializers import UTF8Deserializer
from pyspark.streaming import DStream

__all__ = ['MQTTUtils']


class MQTTUtils(object):

    @staticmethod
    def createStream(ssc, brokerUrl, topic, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2):
        """
        Create an input stream that receives messages pushed by a MQTT publisher.

        :param ssc:  StreamingContext object
        :param brokerUrl:  Url of remote MQTT publisher
        :param topic:  Topic name to subscribe to
        :param storageLevel:  RDD storage level.
        :return: A DStream object
        """
        java_import(ssc._jvm, "org.apache.spark.streaming.mqtt.MQTTUtils")
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
        try:
            jstream = ssc._jvm.MQTTUtils.createStream(ssc._jssc, brokerUrl, topic, jlevel)
        except Py4JError, e:
            if 'call a package' in e.message:
                print "No MQTT package, please build it and add it into classpath:"
                print " $ sbt/sbt streaming-mqtt/package"
                print " $ bin/submit --driver-class-path external/mqtt/target/scala-2.10/" \
                      "spark-streaming-mqtt_2.10-1.3.0-SNAPSHOT.jar"
                raise Exception("No mqtt package")
            raise e
        return DStream(jstream, ssc, UTF8Deserializer())
