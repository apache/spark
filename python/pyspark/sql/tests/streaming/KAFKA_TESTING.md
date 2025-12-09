# Kafka Testing with PySpark

This document explains how to write PySpark unit tests that interact with a Kafka cluster using Docker containers.

## Overview

The `kafka_utils.py` module provides `KafkaUtils` class that launches a single-broker Kafka cluster via Docker using the `testcontainers-python` library. This enables end-to-end integration testing of PySpark Kafka streaming applications without requiring manual Kafka setup.

## Prerequisites

### 1. Docker

Docker must be installed and running on your system. The Kafka test container requires Docker to launch the Kafka broker.

- **Install Docker**: https://docs.docker.com/get-docker/
- **Verify Docker is running**: `docker ps`

### 2. Python Dependencies

Install the required Python packages:

```bash
pip install testcontainers[kafka] kafka-python
```

Or install all dev dependencies:

```bash
cd $SPARK_HOME
pip install -r dev/requirements.txt
```

## Quick Start

Here's a minimal example of writing a Kafka test:

```python
import unittest
from pyspark.sql.tests.streaming.kafka_utils import KafkaUtils
from pyspark.testing.sqlutils import ReusedSQLTestCase

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

    def test_kafka_read_write(self):
        # Create a topic
        topic = "test-topic"
        self.kafka_utils.create_topics([topic])

        # Send test data
        messages = [("key1", "value1"), ("key2", "value2")]
        self.kafka_utils.send_messages(topic, messages)

        # Read with Spark
        df = (
            self.spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_utils.broker)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        # Verify data
        results = df.selectExpr(
            "CAST(key AS STRING) as key",
            "CAST(value AS STRING) as value"
        ).collect()

        self.assertEqual(len(results), 2)
```

## KafkaUtils API Reference

### Initialization and Lifecycle

#### `__init__(kafka_version="7.4.0")`

Create a new KafkaUtils instance.

- **kafka_version**: Version of Confluent Kafka to use (default: "7.4.0" for stability)

#### `setup()`

Start the Kafka container and initialize clients. This must be called before using any other methods.

**Raises:**
- `ImportError`: If required dependencies are not installed
- `RuntimeError`: If Kafka container fails to start

**Note:** Container startup can take 10-30 seconds on first run while Docker pulls the image.

#### `teardown()`

Stop the Kafka container and clean up resources. Safe to call multiple times.

### Topic Management

#### `create_topics(topic_names, num_partitions=1, replication_factor=1)`

Create one or more Kafka topics.

**Parameters:**
- **topic_names** (List[str]): List of topic names to create
- **num_partitions** (int): Number of partitions per topic (default: 1)
- **replication_factor** (int): Replication factor (default: 1, max: 1 for single broker)

**Example:**
```python
# Create single partition topics
kafka_utils.create_topics(["topic1", "topic2"])

# Create multi-partition topic
kafka_utils.create_topics(["multi-partition-topic"], num_partitions=3)
```

#### `delete_topics(topic_names)`

Delete one or more Kafka topics.

**Parameters:**
- **topic_names** (List[str]): List of topic names to delete

### Producing Data

#### `send_messages(topic, messages)`

Send messages to a Kafka topic.

**Parameters:**
- **topic** (str): Topic name
- **messages** (List[tuple]): List of (key, value) tuples

**Example:**
```python
kafka_utils.send_messages("test-topic", [
    ("user1", "login"),
    ("user2", "logout"),
    ("user1", "purchase"),
])
```

### Reading Data

#### `get_all_records(spark, topic, key_deserializer="STRING", value_deserializer="STRING")`

Read all records from a Kafka topic using Spark batch read.

**Parameters:**
- **spark**: SparkSession instance
- **topic** (str): Topic name
- **key_deserializer** (str): How to deserialize keys (default: "STRING")
- **value_deserializer** (str): How to deserialize values (default: "STRING")

**Returns:** Dictionary mapping keys to values

**Example:**
```python
records = kafka_utils.get_all_records(self.spark, "test-topic")
assert records == {"key1": "value1", "key2": "value2"}
```

### Testing Utilities

#### `assert_eventually(result_func, expected, timeout=60, interval=1.0)`

Assert that a condition becomes true within a timeout. Useful for testing streaming queries with eventually consistent results.

**Parameters:**
- **result_func** (Callable): Function that returns the current result
- **expected**: Expected result value
- **timeout** (int): Maximum time to wait in seconds (default: 60)
- **interval** (float): Time between checks in seconds (default: 1.0)

**Raises:** `AssertionError` if condition doesn't become true within timeout

**Example:**
```python
kafka_utils.assert_eventually(
    lambda: kafka_utils.get_all_records(self.spark, "sink-topic"),
    {"key1": "processed-value1"},
    timeout=30
)
```

#### `wait_for_query_alive(query, timeout=60, interval=1.0)`

Wait for a streaming query to become active and ready to process data.

**Parameters:**
- **query**: StreamingQuery instance
- **timeout** (int): Maximum time to wait in seconds (default: 60)
- **interval** (float): Time between checks in seconds (default: 1.0)

**Raises:** `AssertionError` if query doesn't become active within timeout

**Example:**
```python
query = df.writeStream.format("memory").start()
kafka_utils.wait_for_query_alive(query, timeout=30)
```

### Properties

#### `broker`

Get the Kafka bootstrap server address (e.g., "localhost:9093").

#### `producer`

Get the underlying KafkaProducer instance for advanced usage.

#### `admin_client`

Get the underlying KafkaAdminClient instance for advanced usage.

## Common Testing Patterns

### Pattern 1: Batch Read/Write

Test basic Kafka read and write with Spark batch processing:

```python
def test_kafka_batch(self):
    topic = "test-topic"
    self.kafka_utils.create_topics([topic])

    # Write with Spark DataFrame
    df = self.spark.createDataFrame([("key1", "value1")], ["key", "value"])
    (
        df.selectExpr("CAST(key AS BINARY)", "CAST(value AS BINARY)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", self.kafka_utils.broker)
        .option("topic", topic)
        .save()
    )

    # Read back
    records = self.kafka_utils.get_all_records(self.spark, topic)
    assert records == {"key1": "value1"}
```

### Pattern 2: Streaming Queries

Test streaming queries with checkpoint management:

```python
def test_kafka_streaming(self):
    import tempfile
    import os

    # Setup topics
    source_topic = "source"
    sink_topic = "sink"
    self.kafka_utils.create_topics([source_topic, sink_topic])

    # Produce initial data
    self.kafka_utils.send_messages(source_topic, [("k1", "v1")])

    # Start streaming query
    df = (
        self.spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", self.kafka_utils.broker)
        .option("subscribe", source_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    checkpoint_dir = os.path.join(tempfile.mkdtemp(), "checkpoint")
    query = (
        df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", self.kafka_utils.broker)
        .option("topic", sink_topic)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    try:
        self.kafka_utils.wait_for_query_alive(query)
        self.kafka_utils.assert_eventually(
            lambda: self.kafka_utils.get_all_records(self.spark, sink_topic),
            {"k1": "v1"}
        )
    finally:
        query.stop()
```

### Pattern 3: Stateful Aggregations

Test streaming aggregations:

```python
def test_kafka_aggregation(self):
    # Send data for aggregation
    self.kafka_utils.send_messages("source", [
        ("user1", "1"),
        ("user2", "1"),
        ("user1", "1"),
    ])

    # Aggregate by key
    df = (
        self.spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", self.kafka_utils.broker)
        .option("subscribe", "source")
        .load()
        .groupBy(col("key"))
        .count()
        .selectExpr("CAST(key AS BINARY)", "CAST(count AS STRING) AS value")
    )

    query = df.writeStream.format("kafka") # ... start query

    # Verify aggregated results
    self.kafka_utils.assert_eventually(
        lambda: self.kafka_utils.get_all_records(self.spark, "sink"),
        {"user1": "2", "user2": "1"}
    )
```

### Pattern 4: Multiple Topics

Test writing to multiple topics based on data:

```python
def test_multiple_topics(self):
    topic1, topic2 = "topic1", "topic2"
    self.kafka_utils.create_topics([topic1, topic2])

    # Write with topic column
    df = self.spark.createDataFrame([
        (topic1, "key1", "value1"),
        (topic2, "key2", "value2"),
    ], ["topic", "key", "value"])

    (
        df.selectExpr("topic", "CAST(key AS BINARY)", "CAST(value AS BINARY)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", self.kafka_utils.broker)
        .save()
    )

    # Verify data in each topic
    assert self.kafka_utils.get_all_records(self.spark, topic1) == {"key1": "value1"}
    assert self.kafka_utils.get_all_records(self.spark, topic2) == {"key2": "value2"}
```

## Running Tests

### Run All Kafka Tests

```bash
cd $SPARK_HOME/python
python -m pytest pyspark/sql/tests/streaming/test_streaming_kafka_rtm.py -v
```

### Run Specific Test

```bash
python -m pytest pyspark/sql/tests/streaming/test_streaming_kafka_rtm.py::StreamingKafkaTests::test_kafka_batch_read -v
```

### Run with unittest

```bash
cd $SPARK_HOME/python
python -m unittest pyspark.sql.tests.streaming.test_streaming_kafka
```

## Troubleshooting

### Docker Not Running

**Error:**
```
Cannot connect to the Docker daemon at unix:///var/run/docker.sock
```

**Solution:** Start Docker Desktop or Docker daemon

### Container Startup Timeout

**Error:**
```
Kafka container failed to start within timeout
```

**Solutions:**
1. Increase timeout in test code
2. Check Docker resource allocation (CPU/memory)
3. Check Docker logs: `docker logs <container-id>`

### Port Conflicts

**Error:**
```
Port 9093 already in use
```

**Solution:** testcontainers automatically allocates random ports. Ensure no manual port binding in tests.

### Missing Dependencies

**Error:**
```
ImportError: testcontainers is required for Kafka tests
```

**Solution:**
```bash
pip install testcontainers[kafka] kafka-python
```
