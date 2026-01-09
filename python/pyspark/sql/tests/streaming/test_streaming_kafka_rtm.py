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

"""
PySpark tests for Kafka streaming integration using Docker test containers.

These tests demonstrate how to use KafkaUtils to test Spark streaming with Kafka.
Tests require Docker to be running and the following Python packages:
- testcontainers[kafka]
- kafka-python
"""

import os
import tempfile
import unittest
import uuid

from pyspark.testing.sqlutils import ReusedSQLTestCase, search_jar, read_classpath

# Setup Kafka JAR on classpath before SparkSession is created
# This follows the same pattern as streamingutils.py for Kinesis
kafka_sql_jar = search_jar(
    "connector/kafka-0-10-sql",
    "spark-sql-kafka-0-10_",
    "dfadfa",
)

if kafka_sql_jar is None:
    raise RuntimeError(
        "Kafka SQL connector JAR was not found. "
        "To run these tests, you need to build Spark with "
        "'build/mvn package' or 'build/sbt Test/package' "
        "before running this test."
    )

# Read the full classpath including all dependencies
# This works for both Maven builds (reads classpath.txt) and SBT builds (queries SBT)
kafka_classpath = read_classpath("connector/kafka-0-10-sql")
all_jars = f"{kafka_sql_jar},{kafka_classpath}"

# Add Kafka JAR to PYSPARK_SUBMIT_ARGS before SparkSession is created
existing_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
jars_args = "--jars %s" % all_jars

os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join([jars_args, existing_args])

from pyspark.sql.functions import col
from pyspark.sql.tests.streaming.kafka_utils import KafkaUtils


# Check if required Python dependencies are available
try:
    import testcontainers  # noqa: F401
    import kafka  # noqa: F401
except ImportError as e:
    raise ImportError(
        "Kafka test dependencies not available. "
        "Install with: pip install testcontainers[kafka] kafka-python"
    ) from e


class StreamingKafkaTestsMixin:
    """
    Base mixin for Kafka streaming tests that provides KafkaUtils setup/teardown
    and topic management.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Start Kafka container - this may take 10-30 seconds on first run
        cls.kafka_utils = KafkaUtils()
        cls.kafka_utils.setup()


    @classmethod
    def tearDownClass(cls):
        # Stop Kafka container and clean up resources
        if hasattr(cls, 'kafka_utils'):
            cls.kafka_utils.teardown()
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        # Create unique topics for each test to avoid interference
        self.source_topic = f"source-{uuid.uuid4().hex}"
        self.sink_topic = f"sink-{uuid.uuid4().hex}"
        self.kafka_utils.create_topics([self.source_topic, self.sink_topic])

    def tearDown(self):
        # Clean up topics after each test
        self.kafka_utils.delete_topics([self.source_topic, self.sink_topic])
        super().tearDown()


class StreamingKafkaTests(StreamingKafkaTestsMixin, ReusedSQLTestCase):
    """
    Tests for Kafka streaming integration with PySpark.
    """

    def test_streaming_stateless(self):
        """
        Test stateless rtm query with earliest offset.
        """

        # produce test data to source_topic
        for i in range(10):
            self.kafka_utils.producer.send(
                self.source_topic,
                key=i,
                value=i,
            ).get(timeout=10)
        self.kafka_utils.producer.flush()

        # Build streaming query for Kafka to Kafka.
        kafka_source = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_utils.broker)
            .option("subscribe", self.source_topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        checkpoint_dir = os.path.join(tempfile.mkdtemp(), "checkpoint")

        query = (
            kafka_source.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_utils.broker)
            .option("topic", self.sink_topic)
            .option("checkpointLocation", checkpoint_dir)
            .outputMode("update")
            .trigger(realTime="30 seconds")
            .start()
        )

        # Wait for the streaming to process data
        self.kafka_utils.wait_for_query_alive(query)

        # Validate results
        expected = {k: k for k in sorted([str(i) for i in range(10)])}
        try:
            self.kafka_utils.assert_eventually(
                result_func=lambda: self.kafka_utils.get_all_records(
                    self.spark, self.sink_topic
                ), expected=expected
            )
        finally:
            query.stop()

if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
