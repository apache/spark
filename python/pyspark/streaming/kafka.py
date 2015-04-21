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

from py4j.java_gateway import Py4JJavaError

from pyspark.storagelevel import StorageLevel
from pyspark.serializers import PairDeserializer, NoOpSerializer
from pyspark.streaming import DStream

__all__ = ['KafkaUtils', 'utf8_decoder']


def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    return s and s.decode('utf-8')


class KafkaUtils(object):

    @staticmethod
    def createStream(ssc, zkQuorum, groupId, topics, kafkaParams={},
                     storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2,
                     keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        """
        Create an input stream that pulls messages from a Kafka Broker.

        :param ssc:  StreamingContext object
        :param zkQuorum:  Zookeeper quorum (hostname:port,hostname:port,..).
        :param groupId:  The group id for this consumer.
        :param topics:  Dict of (topic_name -> numPartitions) to consume.
                        Each partition is consumed in its own thread.
        :param kafkaParams: Additional params for Kafka
        :param storageLevel:  RDD storage level.
        :param keyDecoder:  A function used to decode key (default is utf8_decoder)
        :param valueDecoder:  A function used to decode value (default is utf8_decoder)
        :return: A DStream object
        """
        kafkaParams.update({
            "zookeeper.connect": zkQuorum,
            "group.id": groupId,
            "zookeeper.connection.timeout.ms": "10000",
        })
        if not isinstance(topics, dict):
            raise TypeError("topics should be dict")
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)

        try:
            # Use KafkaUtilsPythonHelper to access Scala's KafkaUtils (see SPARK-6027)
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader()\
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()
            jstream = helper.createStream(ssc._jssc, kafkaParams, topics, jlevel)
        except Py4JJavaError as e:
            # TODO: use --jar once it also work on driver
            if 'ClassNotFoundException' in str(e.java_exception):
                print("""
________________________________________________________________________________________________

  Spark Streaming's Kafka libraries not found in class path. Try one of the following.

  1. Include the Kafka library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-streaming-kafka:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-streaming-kafka-assembly, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-streaming-kafka-assembly.jar> ...

________________________________________________________________________________________________

""" % (ssc.sparkContext.version, ssc.sparkContext.version))
            raise e
        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
        stream = DStream(jstream, ssc, ser)
        return stream.map(lambda k_v: (keyDecoder(k_v[0]), valueDecoder(k_v[1])))
