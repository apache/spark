..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

===========================================
Streaming Admission Control and Parallelism
===========================================

.. currentmodule:: pyspark.sql

Overview
--------
This guide demonstrates how to implement admission control and parallel partitioning in custom Python streaming data sources.
Admission control allows you to limit the amount of data processed per batch, while parallel partitioning enables efficient distributed processing across executors.

These features are essential for:

- **Controlling data ingestion rate**: Prevent overwhelming the system with too much data at once
- **Backpressure management**: Handle incoming data at a sustainable rate
- **Parallel processing**: Distribute work across multiple executors efficiently
- **Resource optimization**: Balance throughput with available compute resources

Core Streaming Functions
------------------------
Every streaming data source requires implementing these 5 essential functions:

1. **initialOffset()**: Defines where to start reading data
2. **latestOffset(start, limit)**: Determines what new data is available (admission control happens here)
3. **partitions(start, end)**: Divides data into parallel tasks (parallelism happens here)
4. **read(partition)**: Fetches the actual data for each partition
5. **commit(end)**: Performs cleanup after successful batch processing

Additional Functions for Trigger.AvailableNow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To support batching with ``Trigger.AvailableNow``, implement these additional functions:

- **getDefaultReadLimit()**: Returns the default batch size as ``ReadMaxRows(batch_size)``
- **prepareForTriggerAvailableNow()**: Captures the target offset at query start
- **SupportsTriggerAvailableNow mixin**: Enables deterministic batch processing

Example: Blockchain Streaming with Admission Control
----------------------------------------------------
This example demonstrates a simulated blockchain streaming source that processes blocks with controlled batch sizes and parallel execution.

Key Features
~~~~~~~~~~~~
- **Admission Control**: Limits batch size to 20 blocks (configurable via ``batchSize`` option)
- **Parallel Partitioning**: Divides each batch into 4 partitions of 5 blocks each (configurable via ``blocksPerPartition`` option)
- **Two Processing Modes**: Continuous processing and Trigger.AvailableNow

Step 1: Define the Partition Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Create a partition class to represent a range of blocks:

.. code-block:: python

    from pyspark.sql.datasource import InputPartition

    class BlockPartition(InputPartition):
        """Partition representing a range of blockchain blocks to read."""

        def __init__(self, start_block: int, end_block: int):
            self.start_block = start_block
            self.end_block = end_block

Step 2: Implement the Stream Reader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Implement the 5 core streaming functions plus Trigger.AvailableNow support:

.. code-block:: python

    from pyspark.sql.datasource import DataSourceStreamReader
    from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadLimit, ReadMaxRows, SupportsTriggerAvailableNow
    import hashlib

    class BlockchainStreamReader(DataSourceStreamReader, SupportsTriggerAvailableNow):
        CHAIN_HEIGHT = 1000  # Total blocks available

        def __init__(self, batch_size=20, blocks_per_partition=5):
            self.batch_size = batch_size
            self.blocks_per_partition = blocks_per_partition
            self._trigger_available_now_target = None

        def initialOffset(self):
            """Define the starting point."""
            return {"block_number": 0}

        def getDefaultReadLimit(self):
            """Return the default batch size."""
            return ReadMaxRows(self.batch_size)

        def latestOffset(self, start, limit):
            """Control batch size (admission control)."""
            start_block = start["block_number"]

            # Cap at target offset if using Trigger.AvailableNow
            max_available = self.CHAIN_HEIGHT
            if self._trigger_available_now_target is not None:
                max_available = self._trigger_available_now_target

            if start_block >= max_available:
                return start  # No more data

            # Respect the ReadLimit from Spark
            if isinstance(limit, ReadMaxRows):
                end_block = min(start_block + limit.max_rows, max_available)
            elif isinstance(limit, ReadAllAvailable):
                end_block = max_available
            else:
                end_block = min(start_block + self.batch_size, max_available)

            return {"block_number": end_block}

        def prepareForTriggerAvailableNow(self):
            """Capture the target offset for Trigger.AvailableNow."""
            self._trigger_available_now_target = self.CHAIN_HEIGHT

        def partitions(self, start, end):
            """Divide batch into parallel partitions."""
            start_block = start["block_number"]
            end_block = end["block_number"]

            if start_block >= end_block:
                return []

            # Create multiple partitions for parallel processing
            partitions = []
            current = start_block
            while current < end_block:
                partition_end = min(current + self.blocks_per_partition, end_block)
                partitions.append(BlockPartition(current, partition_end))
                current = partition_end

            return partitions

        def read(self, partition):
            """Generate blockchain data for this partition."""
            for block_num in range(partition.start_block, partition.end_block):
                block_hash = hashlib.sha256(str(block_num).encode()).hexdigest()[:16]
                timestamp = 1700000000 + (block_num * 12)
                tx_count = (block_num % 100) + 1
                yield (block_num, block_hash, timestamp, tx_count)

        def commit(self, end):
            """Cleanup after batch completion."""
            pass  # No cleanup needed for this example

Step 3: Define the Data Source
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Create the data source class:

.. code-block:: python

    from pyspark.sql.datasource import DataSource
    from pyspark.sql.types import StructType

    class BlockchainDataSource(DataSource):
        @classmethod
        def name(cls):
            return "blockchain"

        def schema(self):
            return "block_number INT, block_hash STRING, timestamp LONG, transaction_count INT"

        def streamReader(self, schema):
            batch_size = int(self.options.get("batchSize", "20"))
            blocks_per_partition = int(self.options.get("blocksPerPartition", "5"))
            return BlockchainStreamReader(batch_size, blocks_per_partition)

Step 4: Register and Use the Data Source
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Register the data source and create streaming queries:

.. code-block:: python

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("BlockchainStream").getOrCreate()
    spark.dataSource.register(BlockchainDataSource)

    # Continuous processing example
    blocks = (
        spark.readStream
        .format("blockchain")
        .option("batchSize", "20")              # 20 blocks per batch
        .option("blocksPerPartition", "5")      # 5 blocks per partition
        .load()
    )

    query = (
        blocks.writeStream
        .format("console")
        .trigger(processingTime="2 seconds")    # Continuous processing
        .start()
    )

Processing Modes
----------------

Continuous Processing
~~~~~~~~~~~~~~~~~~~~~
Processes data continuously at regular intervals:

.. code-block:: python

    query = (
        blocks.writeStream
        .format("console")
        .trigger(processingTime="2 seconds")    # Process every 2 seconds
        .start()
    )

- Runs indefinitely until manually stopped
- Processes new batches at regular intervals
- Suitable for real-time streaming applications

Trigger.AvailableNow
~~~~~~~~~~~~~~~~~~~~
Processes all available data in batches, then stops:

.. code-block:: python

    query = (
        blocks.writeStream
        .format("console")
        .trigger(availableNow=True)             # Process all data, then stop
        .start()
    )

    query.awaitTermination()  # Wait for completion

- Processes all available data as fast as possible
- Stops automatically when done
- Suitable for batch catch-up processing

Admission Control Benefits
---------------------------
With ``batchSize=20`` and ``blocksPerPartition=5``:

- **Total blocks**: 1,000
- **Batches**: 50 batches (1,000 ÷ 20)
- **Partitions per batch**: 4 partitions (20 ÷ 5)
- **Total parallel tasks**: 200 tasks (50 batches × 4 partitions)

Benefits:

1. **Controlled ingestion**: Prevents processing all 1,000 blocks at once
2. **Parallel execution**: Each batch processes 4 partitions in parallel
3. **Resource management**: Limits memory and CPU usage per batch
4. **Scalability**: Add more executors to process more partitions

Complete Example
----------------
For a complete working example with both continuous and Trigger.AvailableNow modes, see:

``examples/src/main/python/sql/streaming/structured_blockchain_admission_control.py``

Run the example:

.. code-block:: bash

    $ bin/spark-submit \\
        examples/src/main/python/sql/streaming/structured_blockchain_admission_control.py

See Also
--------
- :doc:`python_data_source` - General Python Data Source API documentation
- :class:`DataSourceStreamReader` - Stream reader base class
- :class:`InputPartition` - Partition representation
