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
import sys
from itertools import chain
import random

try:
    import xmlrunner
except ImportError:
    xmlrunner = None

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark.streaming.kafka import OffsetRange
from pyspark.streaming.kafka010 import Assign, KafkaUtils, PreferBrokers, PreferConsistent, \
    PreferFixed, Subscribe, SubscribePattern, TopicPartition
from pyspark.streaming.tests import PySparkStreamingTestCase, search_jar


class Kafka010StreamTests(PySparkStreamingTestCase):
    timeout = 20  # seconds
    duration = 1

    @classmethod
    def setUpClass(cls):
        super(Kafka010StreamTests, cls).setUpClass()
        cls._kafkaTestUtils = cls.sc._jvm.org.apache.spark.streaming.kafka010.KafkaTestUtils()
        cls._kafkaTestUtils.setup()

    @classmethod
    def tearDownClass(cls):
        super(Kafka010StreamTests, cls).tearDownClass()

        if cls._kafkaTestUtils is not None:
            cls._kafkaTestUtils.teardown()
            cls._kafkaTestUtils = None

    def _randomTopic(self):
        return "topic-%d" % random.randint(0, 100000)

    def _randomGroupId(self):
        return "test-consumer-%d" % random.randint(0, 100000)

    def _validateStreamResult(self, sendData, stream):
        result = {}
        for i in chain.from_iterable(self._collect(stream.map(lambda x: x[1]),
                                                   sum(sendData.values()))):
            result[i] = result.get(i, 0) + 1

        self.assertEqual(sendData, result)

    def _validateRddResult(self, sendData, rdd):
        result = {}
        for i in rdd.map(lambda x: x[1]).collect():
            result[i] = result.get(i, 0) + 1
        self.assertEqual(sendData, result)

    def _with_strategy(self, topic, locationStrategy, consumerStrategy):
        sendData = {"a": 3, "b": 5, "c": 10}
        self._kafkaTestUtils.createTopic(topic)
        self._kafkaTestUtils.sendMessages(topic, sendData)

        stream = KafkaUtils.createDirectStream(
            self.ssc, locationStrategy, consumerStrategy) \
            .map(lambda c: (c.key, c.value))
        self._validateStreamResult(sendData, stream)

    def test_kafka_direct_stream_prefer_brokers(self):
        """Test the Python Kafka direct stream API with PreferBrokers strategy."""
        topic = self._randomTopic()
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "auto.offset.reset": "earliest",
                       "group.id": self._randomGroupId()}
        consumerStrategy = Subscribe([topic], kafkaParams)

        self._with_strategy(topic, PreferBrokers(), consumerStrategy)

    def test_kafka_direct_stream_prefer_consistent(self):
        """Test the Python Kafka direct stream API with PreferConsistent strategy"""
        topic = self._randomTopic()
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "auto.offset.reset": "earliest",
                       "group.id": self._randomGroupId()}
        consumerStrategy = Subscribe([topic], kafkaParams)

        self._with_strategy(topic, PreferConsistent(), consumerStrategy)

    def test_kafka_direct_stream_prefer_fixed(self):
        """Test the Python Kafka direct stream API with PreferFixed strategy"""
        topic = self._randomTopic()
        hostMap = {TopicPartition(topic, 0): "localhost"}
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "auto.offset.reset": "earliest",
                       "group.id": self._randomGroupId()}
        consumerStrategy = Subscribe([topic], kafkaParams)

        self._with_strategy(topic, PreferFixed(hostMap), consumerStrategy)

    def test_kafka_direct_stream_subscribe_pattern(self):
        """Test the Python Kafka direct stream API with SubscribePattern strategy"""
        topic = self._randomTopic()
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "auto.offset.reset": "earliest",
                       "group.id": self._randomGroupId()}
        consumerStrategy = SubscribePattern(topic, kafkaParams)

        self._with_strategy(topic, PreferConsistent(), consumerStrategy)

    def test_kafka_direct_stream_assign(self):
        """Test the Python Kafka direct stream API with Assign strategy"""
        topic = self._randomTopic()
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "auto.offset.reset": "earliest",
                       "group.id": self._randomGroupId()}
        consumerStrategy = Assign([TopicPartition(topic, 0)], kafkaParams)

        self._with_strategy(topic, PreferConsistent(), consumerStrategy)

    @unittest.skipIf(sys.version >= "3", "long type not support")
    def test_kafka_direct_stream_specify_offset(self):
        """Test the Python direct Kafka stream API with start offset specified."""
        topic = self._randomTopic()
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "group.id": self._randomGroupId()}
        consumerStrategy = Subscribe([topic], kafkaParams, {TopicPartition(topic, 0): long(0)})

        self._with_strategy(topic, PreferConsistent(), consumerStrategy)

    def _with_location_strategy(self, topic, locationStrategy):
        sendData = {"a": 1, "b": 2}
        offsetRanges = [OffsetRange(topic, 0, long(0), long(sum(sendData.values())))]
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "group.id": self._randomGroupId()}

        self._kafkaTestUtils.createTopic(topic)
        self._kafkaTestUtils.sendMessages(topic, sendData)
        rdd = KafkaUtils.createRDD(self.sc, kafkaParams, offsetRanges, locationStrategy) \
            .map(lambda x: (x.key, x.value))
        self._validateRddResult(sendData, rdd)

    @unittest.skipIf(sys.version >= "3", "long type not support")
    def test_kafka_rdd_prefer_consistent(self):
        """Test the Python direct Kafka RDD API with PreferConsistent strategy."""
        topic = self._randomTopic()
        self._with_location_strategy(topic, PreferConsistent())

    @unittest.skipIf(sys.version >= "3", "long type not support")
    def test_kafka_rdd_prefer_fixed(self):
        """Test the Python direct Kafka RDD API with PreferFixed strategy."""
        topic = self._randomTopic()
        hostMap = {TopicPartition(topic, 0): "localhost"}
        self._with_location_strategy(topic, PreferFixed(hostMap))

    @unittest.skipIf(sys.version >= "3", "long type not support")
    def test_kafka_rdd_get_offsetRanges(self):
        """Test Python direct Kafka RDD get OffsetRanges."""
        topic = self._randomTopic()
        sendData = {"a": 3, "b": 4, "c": 5}
        offsetRanges = [OffsetRange(topic, 0, long(0), long(sum(sendData.values())))]
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "group.id": self._randomGroupId()}

        self._kafkaTestUtils.createTopic(topic)
        self._kafkaTestUtils.sendMessages(topic, sendData)
        rdd = KafkaUtils.createRDD(self.sc, kafkaParams, offsetRanges, PreferConsistent())
        self.assertEqual(offsetRanges, rdd.offsetRanges())

    @unittest.skipIf(sys.version >= "3", "long type not support")
    def test_kafka_direct_stream_foreach_get_offsetRanges(self):
        """Test the Python direct Kafka stream foreachRDD get offsetRanges."""
        topic = self._randomTopic()
        sendData = {"a": 1, "b": 2, "c": 3}
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "auto.offset.reset": "earliest",
                       "group.id": self._randomGroupId()}

        self._kafkaTestUtils.createTopic(topic)
        self._kafkaTestUtils.sendMessages(topic, sendData)

        stream = KafkaUtils.createDirectStream(self.ssc, PreferConsistent(),
                                               Subscribe([topic], kafkaParams))

        offsetRanges = []

        def getOffsetRanges(_, rdd):
            for o in rdd.offsetRanges():
                offsetRanges.append(o)

        stream.foreachRDD(getOffsetRanges)
        self.ssc.start()
        self.wait_for(offsetRanges, 1)

        self.assertEqual(offsetRanges, [OffsetRange(topic, 0, long(0), long(6))])

    @unittest.skipIf(sys.version >= "3", "long type not support")
    def test_kafka_direct_stream_transform_get_offsetRanges(self):
        """Test the Python direct Kafka stream transform get offsetRanges."""
        topic = self._randomTopic()
        sendData = {"a": 1, "b": 2, "c": 3}
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "auto.offset.reset": "earliest",
                       "group.id": self._randomGroupId()}

        self._kafkaTestUtils.createTopic(topic)
        self._kafkaTestUtils.sendMessages(topic, sendData)

        stream = KafkaUtils.createDirectStream(self.ssc, PreferConsistent(),
                                               Subscribe([topic], kafkaParams))

        offsetRanges = []

        def transformWithOffsetRanges(rdd):
            for o in rdd.offsetRanges():
                offsetRanges.append(o)
            return rdd

        # Test whether it is ok mixing KafkaTransformedDStream and TransformedDStream together,
        # only the TransformedDstreams can be folded together.
        stream.transform(transformWithOffsetRanges).count().pprint()
        self.ssc.start()
        self.wait_for(offsetRanges, 1)

        self.assertEqual(offsetRanges, [OffsetRange(topic, 0, long(0), long(6))])

    @unittest.skipIf(sys.version >= "3", "long type not support")
    def test_kafka_direct_stream_commit_offsets(self):
        """Test the Python direct kafka stream commit offsets"""
        topic = self._randomTopic()
        sendData = {"a": 1, "b": 2, "c": 3}
        kafkaParams = {"bootstrap.servers": self._kafkaTestUtils.brokerAddress(),
                       "auto.offset.reset": "earliest",
                       "group.id": self._randomGroupId()}

        self._kafkaTestUtils.createTopic(topic)
        self._kafkaTestUtils.sendMessages(topic, sendData)

        stream = KafkaUtils.createDirectStream(self.ssc, PreferConsistent(),
                                               Subscribe([topic], kafkaParams))

        offsetRanges = []

        def commitOffsets(rdd):
            stream.commitAsync(rdd.offsetRanges())
            for o in rdd.offsetRanges():
                offsetRanges.append(o)

        stream.foreachRDD(commitOffsets)
        self.ssc.start()
        self.wait_for(offsetRanges, 1)
        self.assertEqual(offsetRanges, [OffsetRange(topic, 0, long(0), long(6))])

    def test_topic_partition_equality(self):
        topic_partition_a = TopicPartition("foo", 0)
        topic_partition_b = TopicPartition("foo", 0)
        topic_partition_c = TopicPartition("bar", 0)
        topic_partition_d = TopicPartition("foo", 1)

        self.assertEqual(topic_partition_a, topic_partition_b)
        self.assertNotEqual(topic_partition_a, topic_partition_c)
        self.assertNotEqual(topic_partition_a, topic_partition_d)


# Search jar in the project dir using the jar name_prefix for both sbt build and maven build because
# the artifact jars are in different directories.
def search_kafka010_assembly_jar():
    SPARK_HOME = os.environ["SPARK_HOME"]
    kafka_assembly_dir = os.path.join(SPARK_HOME, "external/kafka-0-10-assembly")
    jars = search_jar(kafka_assembly_dir, "spark-streaming-kafka-0-10-assembly")
    if not jars:
        raise Exception(
            ("Failed to find Spark Streaming kafka-0-10 assembly jar in %s. " %
             kafka_assembly_dir) + "You need to build Spark with "
            "'build/sbt assembly/package streaming-kafka-0-10-assembly/assembly' or "
            "'build/mvn package' before running this test.")
    elif len(jars) > 1:
        raise Exception(("Found multiple Spark Streaming Kafka-0-10 assembly JARs: %s; please "
                         "remove all but one") % (", ".join(jars)))
    else:
        return jars[0]


if __name__ == "__main__":
    from pyspark.streaming.kafka010_tests import *
    kafka010_assembly_jar = search_kafka010_assembly_jar()
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars %s pyspark-shell" % kafka010_assembly_jar

    sys.stderr.write("Running tests: %s \n" % (str(Kafka010StreamTests)))
    failed = False
    tests = unittest.TestLoader().loadTestsFromTestCase(Kafka010StreamTests)
    if xmlrunner:
        result = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=3).run(tests)
        if not result.wasSuccessful():
            failed = True
    else:
        result = unittest.TextTestRunner(verbosity=3).run(tests)
        if not result.wasSuccessful():
            failed = True
    sys.exit(failed)
