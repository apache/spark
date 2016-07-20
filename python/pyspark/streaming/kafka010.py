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

from pyspark.rdd import RDD
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer
from pyspark.streaming import DStream
from pyspark.streaming.kafka import KafkaDStream, KafkaRDD, OffsetRange

__all__ = ['Assign', 'KafkaConsumerRecord', 'KafkaUtils', 'PreferBrokers', 'PreferConsistent',
           'PreferFixed', 'Subscribe', 'SubscribePattern', 'TopicPartition']


def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s.decode('utf-8')


class KafkaUtils(object):

    @staticmethod
    def createDirectStream(ssc, locationStrategy, consumerStrategy,
                           keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        helper = KafkaUtils._get_helper(ssc._sc)
        ser = AutoBatchedSerializer(PickleSerializer())

        jlocationStrategy = locationStrategy._jLocationStrategy(helper)
        jconsumerStrategy = consumerStrategy._jConsumerStrategy(helper)

        jstream = helper.createDirectStream(ssc._jssc, jlocationStrategy, jconsumerStrategy)

        def func(m):
            m._set_key_deserializer(keyDecoder)
            m._set_value_deserializer(valueDecoder)
            return m

        stream = DStream(jstream, ssc, ser).map(func)

        return KafkaDStream(stream._jdstream, ssc, stream._jrdd_deserializer)

    @staticmethod
    def createRDD(sc, kafkaParams, offsetRanges, locationStrategy,
                  keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):

        if not isinstance(kafkaParams, dict):
            raise TypeError("kafkaParams should be dict")

        helper = KafkaUtils._get_helper(sc)
        joffsetRanges = [o._jOffsetRange(helper) for o in offsetRanges]
        jlocationStrategy = locationStrategy._jLocationStrategy(helper)

        jrdd = helper.createRDD(sc._jsc, kafkaParams, joffsetRanges, jlocationStrategy)

        def func(m):
            m._set_key_deserializer(keyDecoder)
            m._set_value_deserializer(valueDecoder)
            return m

        rdd = RDD(jrdd, sc).map(func)

        return KafkaRDD(rdd._jrdd, sc, rdd._jrdd_deserializer)

    @staticmethod
    def _get_helper(sc):
        try:
            return sc._jvm.org.apache.spark.streaming.kafka010.KafkaUtilsPythonHelper()
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                KafkaUtils._printErrorMsg(sc)
            raise

    @staticmethod
    def _printErrorMsg(sc):
        print("""
________________________________________________________________________________________________

  Spark Streaming's Kafka libraries not found in class path. Try one of the following.

  1. Include the Kafka library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-streaming-kafka-0-10-assembly, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-streaming-kafka-0-10-assembly.jar> ...

________________________________________________________________________________________________

""" % (sc.version, sc.version))


class LocationStrategy(object):

    def _jLocationStrategy(self, helper):
        pass


class PreferBrokers(LocationStrategy):

    def _jLocationStrategy(self, helper):
        return helper.createPreferBrokers()


class PreferConsistent(LocationStrategy):

    def _jLocationStrategy(self, helper):
        return helper.createPreferConsistent()


class PreferFixed(LocationStrategy):

    def __init__(self, hostMap):
        self.hostMap = hostMap

    def _jLocationStrategy(self, helper):
        jhostMap = dict([(k._jTopicPartition(helper), v) for (k, v) in self.hostMap.items()])
        return helper.createPreferFixed(jhostMap)


class ConsumerStrategy(object):

    def _jConsumerStrategy(self, helper):
        pass


class Subscribe(ConsumerStrategy):

    def __init__(self, topics, kafkaParams, offsets=None):
        self.topics = set(topics)
        self.kafkaParams = kafkaParams
        self.offsets = dict() if offsets is None else offsets

    def _jConsumerStrategy(self, helper):
        jOffsets = dict([k._jTopicPartition(helper), v] for (k, v) in self.offsets.items())
        return helper.createSubscribe(self.topics, self.kafkaParams, jOffsets)


class SubscribePattern(ConsumerStrategy):

    def __init__(self, pattern, kafkaParams, offsets=None):
        self.pattern = pattern
        self.kafkaParams = kafkaParams
        self.offsets = dict() if offsets is None else offsets

    def _jConsumerStrategy(self, helper):
        jOffsets = dict([k._jTopicPartition(helper), v] for (k, v) in self.offsets.items())
        return helper.createSubscribePattern(self.pattern, self.kafkaParams, jOffsets)


class Assign(ConsumerStrategy):

    def __init__(self, topicPartitions, kafkaParams, offsets=None):
        self.topicPartitions = set(topicPartitions)
        self.kafkaParams = kafkaParams
        self.offsets = dict() if offsets is None else offsets

    def _jConsumerStrategy(self, helper):
        jOffsets = dict([k._jTopicPartition(helper), v] for (k, v) in self.offsets.items())
        return helper.createAssign(self.topicPartitions, self.kafkaParams, jOffsets)


class TopicPartition(object):
    """
    Represents a specific top and partition for Kafka.
    """

    def __init__(self, topic, partition):
        """
        Create a Python TopicPartition to map to the Java related object
        :param topic: Kafka topic name.
        :param partition: Kafka partition id.
        """
        self._topic = topic
        self._partition = partition

    def _jTopicPartition(self, helper):
        return helper.createTopicPartition(self._topic, self._partition)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self._topic == other._topic
                    and self._partition == other._partition)
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class KafkaConsumerRecord(object):

    def __init__(self, topic, partition, offset, timestamp, timestampType, checksum,
                 serializedKeySize, serializedValueSize, key, value):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp
        self.timestampType = timestampType
        self.checksum = checksum
        self.serializedKeySize = serializedKeySize
        self.serializedValueSize = serializedValueSize
        self._rawKey = key
        self._rawValue = value
        self._keyDecoder = utf8_decoder
        self._valueDecoder = utf8_decoder

    def __str__(self):
        return "Kafka ConsumerRecords(topic: %s, partition: %d, offset: %d, timestamp: %d, " \
               "key and value...)" % (self.topic, self.partition, self.offset, self.timestamp)

    def __repr__(self):
        return self.__str__()

    def __reduce__(self):
        return (self.__class__, (self.topic, self.partition, self.offset, self.timestamp,
            self.timestampType, self.checksum, self.serializedKeySize, self.serializedValueSize,
                self._rawKey, self._rawValue))

    def _set_key_deserializer(self, decoder):
        self._keyDecoder = decoder

    def _set_value_deserializer(self, decoder):
        self._valueDecoder = decoder

    @property
    def key(self):
        return self._keyDecoder(self._rawKey)

    @property
    def value(self):
        return self._valueDecoder(self._rawValue)
