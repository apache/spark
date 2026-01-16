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
Utilities for running PySpark tests against a Kafka cluster using Docker containers.

This module provides KafkaUtils class that launches a single-broker Kafka cluster
via Docker using testcontainers-python library. It's designed to be used with
Python unittest-based PySpark tests.

Example usage:
    class MyKafkaTest(ReusedSQLTestCase):
        @classmethod
        def setUpClass(cls):
            super().setUpClass()
            cls.kafka_utils = KafkaUtils()
            cls.kafka_utils.setup()

        @classmethod
        def tearDownClass(cls):
            cls.kafka_utils.teardown()
            super().tearDownClass()

        def test_kafka_streaming(self):
            topic = "test-topic"
            self.kafka_utils.create_topics([topic])
            # ... use self.kafka_utils.broker for bootstrap.servers
"""

import time
from typing import List, Dict, Optional, Callable, Any


class KafkaUtils:
    """
    Utility class for managing a Kafka test cluster using Docker containers.

    This class provides methods to:
    - Start/stop a single-broker Kafka cluster in a Docker container
    - Create and delete topics
    - Send messages to topics
    - Query topic data using Spark
    - Helper methods for testing (assert_eventually, wait_for_query_alive)

    Attributes:
        broker (str): The bootstrap server address (e.g., "localhost:9093")
        initialized (bool): Whether the Kafka cluster has been started
    """

    def __init__(self, kafka_version: str = "7.4.0"):
        """
        Initialize KafkaUtils.

        Args:
            kafka_version: Version of Confluent Kafka to use (default: 7.4.0 for stability)
        """
        self.kafka_version = kafka_version
        self.initialized = False
        self._kafka_container = None
        self._admin_client = None
        self._producer = None
        self.broker = None

    def setup(self) -> None:
        """
        Start the Kafka container and initialize admin client and producer.

        This method:
        1. Starts a Kafka container using testcontainers
        2. Creates an admin client for topic management
        3. Creates a producer for sending test messages

        Raises:
            ImportError: If required dependencies (testcontainers, kafka-python) are not installed
            RuntimeError: If Kafka container fails to start
        """
        if self.initialized:
            return

        try:
            from testcontainers.kafka import KafkaContainer
        except ImportError as e:
            raise ImportError(
                "testcontainers is required for Kafka tests. "
                "Install it with: pip install testcontainers[kafka]"
            ) from e

        try:
            from kafka import KafkaProducer
            from kafka.admin import KafkaAdminClient
        except ImportError as e:
            raise ImportError(
                "kafka-python is required for Kafka tests. "
                "Install it with: pip install kafka-python"
            ) from e

        # Start Kafka container with specific version for test stability
        self._kafka_container = KafkaContainer(f"confluentinc/cp-kafka:{self.kafka_version}")
        self._kafka_container.start()

        # Get bootstrap server address
        self.broker = self._kafka_container.get_bootstrap_server()

        # Initialize admin client for topic management
        self._admin_client = KafkaAdminClient(
            bootstrap_servers=self.broker,
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=10000
        )

        # Initialize producer for sending test messages
        self._producer = KafkaProducer(
            bootstrap_servers=self.broker,
            key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
            value_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
            request_timeout_ms=10000,
            max_block_ms=10000
        )

        self.initialized = True

    def teardown(self) -> None:
        """
        Stop the Kafka container and clean up resources.

        This method closes the admin client, producer, and stops the Kafka container.
        It's safe to call multiple times.
        """
        if not self.initialized:
            return

        # Close admin client
        if self._admin_client is not None:
            try:
                self._admin_client.close()
            except Exception:
                pass
            self._admin_client = None

        # Close producer
        if self._producer is not None:
            try:
                self._producer.close(timeout=5)
            except Exception:
                pass
            self._producer = None

        # Stop Kafka container
        if self._kafka_container is not None:
            try:
                self._kafka_container.stop()
            except Exception:
                pass
            self._kafka_container = None

        self.broker = None
        self.initialized = False

    def _assert_initialized(self) -> None:
        """Check if KafkaUtils has been initialized, raise error if not."""
        if not self.initialized:
            raise RuntimeError(
                "KafkaUtils has not been initialized. Call setup() first."
            )

    def create_topics(self, topic_names: List[str], num_partitions: int = 1,
                     replication_factor: int = 1) -> None:
        """
        Create Kafka topics.

        Args:
            topic_names: List of topic names to create
            num_partitions: Number of partitions per topic (default: 1)
            replication_factor: Replication factor (default: 1, max: 1 for single broker)

        Note:
            If a topic already exists, it will be silently ignored.
        """
        self._assert_initialized()

        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError

        topics = [
            NewTopic(
                name=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            for name in topic_names
        ]

        try:
            self._admin_client.create_topics(new_topics=topics, validate_only=False)
        except TopicAlreadyExistsError:
            # Topic already exists, ignore
            pass

    def delete_topics(self, topic_names: List[str]) -> None:
        """
        Delete Kafka topics.

        Args:
            topic_names: List of topic names to delete

        Note:
            If a topic doesn't exist, it will be silently ignored.
        """
        self._assert_initialized()

        from kafka.errors import UnknownTopicOrPartitionError

        try:
            self._admin_client.delete_topics(topics=topic_names)
        except UnknownTopicOrPartitionError:
            # Topic doesn't exist, ignore
            pass

    def send_messages(self, topic: str, messages: List[tuple]) -> None:
        """
        Send messages to a Kafka topic.

        Args:
            topic: Topic name to send messages to
            messages: List of (key, value) tuples to send

        Example:
            kafka_utils.send_messages("test-topic", [
                ("key1", "value1"),
                ("key2", "value2"),
            ])
        """
        self._assert_initialized()

        for key, value in messages:
            future = self._producer.send(topic, key=key, value=value)
            future.get(timeout=10)  # Wait for send to complete

        self._producer.flush()

    def get_all_records(self, spark, topic: str,
                       key_deserializer: str = "STRING",
                       value_deserializer: str = "STRING") -> Dict[str, str]:
        """
        Read all records from a Kafka topic using Spark.

        Args:
            spark: SparkSession instance
            topic: Topic name to read from
            key_deserializer: How to deserialize keys (default: "STRING")
            value_deserializer: How to deserialize values (default: "STRING")

        Returns:
            Dictionary mapping keys to values

        Example:
            records = kafka_utils.get_all_records(self.spark, "test-topic")
            assert records == {"key1": "value1", "key2": "value2"}
        """
        self._assert_initialized()

        df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", self.broker)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        df = df.selectExpr(
            f"CAST(key AS {key_deserializer}) AS key_str",
            f"CAST(value AS {value_deserializer}) AS value_str"
        )

        rows = df.collect()
        return {row.key_str: row.value_str for row in rows}

    def assert_eventually(self, result_func: Callable[[], Any], expected: Any,
                         timeout: int = 60, interval: float = 1.0) -> None:
        """
        Assert that a condition becomes true within a timeout.

        This is useful for testing streaming queries where results are eventually consistent.

        Args:
            result_func: Function that returns the current result
            expected: Expected result value
            timeout: Maximum time to wait in seconds (default: 60)
            interval: Time between checks in seconds (default: 1.0)

        Raises:
            AssertionError: If the condition doesn't become true within timeout

        Example:
            kafka_utils.assert_eventually(
                lambda: kafka_utils.get_all_records(spark, topic),
                {"key1": "value1"}
            )
        """
        deadline = time.time() + timeout
        last_result = None

        while time.time() < deadline:
            last_result = result_func()
            if last_result == expected:
                return  # Success!
            time.sleep(interval)

        # Timeout reached, raise assertion error
        raise AssertionError(
            f"Condition not met within {timeout}s. "
            f"Expected: {expected}, Got: {last_result}"
        )

    def wait_for_query_alive(self, query, timeout: int = 60, interval: float = 1.0) -> None:
        """
        Wait for a streaming query to become active and ready to process data.

        Args:
            query: StreamingQuery instance
            timeout: Maximum time to wait in seconds (default: 60)
            interval: Time between checks in seconds (default: 1.0)

        Raises:
            AssertionError: If the query doesn't become active within timeout

        Example:
            query = df.writeStream.format("memory").start()
            kafka_utils.wait_for_query_alive(query)
        """
        deadline = time.time() + timeout

        while time.time() < deadline:
            status = query.status
            if status["isDataAvailable"] or status["isTriggerActive"]:
                return  # Query is alive!
            time.sleep(interval)

        raise AssertionError(f"Query did not become active within {timeout}s.")

    @property
    def producer(self):
        """Get the Kafka producer instance for advanced usage."""
        self._assert_initialized()
        return self._producer

    @property
    def admin_client(self):
        """Get the Kafka admin client instance for advanced usage."""
        self._assert_initialized()
        return self._admin_client
