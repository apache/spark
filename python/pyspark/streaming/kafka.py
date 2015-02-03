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

from py4j.java_collections import MapConverter
from py4j.java_gateway import java_import, Py4JError

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
        java_import(ssc._jvm, "org.apache.spark.streaming.kafka.KafkaUtils")

        kafkaParams.update({
            "zookeeper.connect": zkQuorum,
            "group.id": groupId,
            "zookeeper.connection.timeout.ms": "10000",
        })
        if not isinstance(topics, dict):
            raise TypeError("topics should be dict")
        jtopics = MapConverter().convert(topics, ssc.sparkContext._gateway._gateway_client)
        jparam = MapConverter().convert(kafkaParams, ssc.sparkContext._gateway._gateway_client)
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)

        def getClassByName(name):
            return ssc._jvm.org.apache.spark.util.Utils.classForName(name)

        try:
            array = getClassByName("[B")
            decoder = getClassByName("kafka.serializer.DefaultDecoder")
            jstream = ssc._jvm.KafkaUtils.createStream(ssc._jssc, array, array, decoder, decoder,
                                                       jparam, jtopics, jlevel)
        except Py4JError, e:
            # TODO: use --jar once it also work on driver
            if not e.message or 'call a package' in e.message:
                print "No kafka package, please put the assembly jar into classpath:"
                print " $ bin/spark-submit --driver-class-path external/kafka-assembly/target/" + \
                      "scala-*/spark-streaming-kafka-assembly-*.jar"
            raise e
        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
        stream = DStream(jstream, ssc, ser)
        return stream.map(lambda (k, v): (keyDecoder(k), valueDecoder(v)))
