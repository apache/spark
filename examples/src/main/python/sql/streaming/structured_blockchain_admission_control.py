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
Demonstrates admission control using a simulated blockchain data source.

This example shows how to build a custom streaming data source that
simulates reading blockchain blocks while respecting admission control
limits using getDefaultReadLimit() and ReadMaxRows(20).

Key concepts demonstrated:
- getDefaultReadLimit() returning ReadMaxRows(20) to limit blocks per micro-batch
- latestOffset(start, limit) respecting the ReadLimit parameter
- Controlled data ingestion rate for backpressure management

Usage:
    bin/spark-submit examples/src/main/python/sql/streaming/\\
        structured_blockchain_admission_control.py

Expected output:
    Each micro-batch processes up to 20 blocks (controlled by admission control):
        Batch 0: blocks 0-19
        Batch 1: blocks 20-39
        Batch 2: blocks 40-59
        ...
    The final batch may contain fewer than 20 blocks when the chain is exhausted.
"""

import hashlib
import time
from typing import Iterator, Sequence, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadLimit, ReadMaxRows
from pyspark.sql.types import StructType


class BlockPartition(InputPartition):
    """Partition representing a range of blockchain blocks to read."""

    def __init__(self, start_block: int, end_block: int):
        self.start_block = start_block
        self.end_block = end_block


class BlockchainStreamReader(DataSourceStreamReader):
    """
    A streaming reader that simulates reading blockchain blocks.

    Demonstrates admission control via getDefaultReadLimit() which limits
    the number of blocks processed per micro-batch.
    """

    CHAIN_HEIGHT = 10000  # Total blocks available

    def initialOffset(self) -> dict:
        """Return the starting block number for new queries."""
        return {"block_number": 0}

    def getDefaultReadLimit(self) -> ReadLimit:
        """
        Limit each micro-batch to 20 blocks.

        This controls the data ingestion rate, useful for:
        - Preventing memory issues with large batches
        - Rate limiting when reading from external APIs
        - Backpressure management
        """
        return ReadMaxRows(20)

    def latestOffset(self, start: dict, limit: ReadLimit) -> dict:
        """
        Compute the ending block number respecting admission control.

        Parameters
        ----------
        start : dict
            Current offset with 'block_number' key
        limit : ReadLimit
            Engine-provided limit on data consumption

        Returns
        -------
        dict
            Ending offset for this micro-batch
        """
        start_block = start["block_number"]

        if start_block >= self.CHAIN_HEIGHT:
            return start  # No more data

        if isinstance(limit, ReadMaxRows):
            end_block = min(start_block + limit.max_rows, self.CHAIN_HEIGHT)
        elif isinstance(limit, ReadAllAvailable):
            end_block = self.CHAIN_HEIGHT
        else:
            raise ValueError(f"Unexpected ReadLimit type: {type(limit)}")

        return {"block_number": end_block}

    def partitions(self, start: dict, end: dict) -> Sequence[InputPartition]:
        """Create a single partition for the block range."""
        start_block = start["block_number"]
        end_block = end["block_number"]

        if start_block >= end_block:
            return []

        return [BlockPartition(start_block, end_block)]

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        """
        Generate simulated blockchain block data.

        Each block contains:
        - block_number: Sequential block identifier
        - block_hash: Simulated hash based on block number
        - timestamp: Simulated timestamp
        - transaction_count: Simulated transaction count
        """
        assert isinstance(partition, BlockPartition)

        for block_num in range(partition.start_block, partition.end_block):
            block_hash = hashlib.sha256(str(block_num).encode()).hexdigest()[:16]
            timestamp = 1700000000 + (block_num * 12)
            tx_count = (block_num % 100) + 1

            yield (block_num, block_hash, timestamp, tx_count)

    def commit(self, end: dict) -> None:
        """Cleanup after batch completion."""
        pass


class BlockchainDataSource(DataSource):
    """Data source that creates BlockchainStreamReader instances."""

    @classmethod
    def name(cls) -> str:
        return "blockchain_example"

    def schema(self) -> str:
        return "block_number INT, block_hash STRING, timestamp LONG, transaction_count INT"

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        return BlockchainStreamReader()


def main() -> None:
    """Run blockchain streaming example demonstrating admission control."""
    spark = SparkSession.builder.appName("BlockchainAdmissionControl").getOrCreate()

    spark.dataSource.register(BlockchainDataSource)

    print("\n" + "=" * 70)
    print("BLOCKCHAIN STREAMING WITH ADMISSION CONTROL")
    print("=" * 70)
    print("\nData Source: Simulated blockchain with 10000 blocks")
    print("Admission Control: getDefaultReadLimit() returns ReadMaxRows(20)")
    print("Expected: Each batch processes up to 20 blocks")
    print()

    df = spark.readStream.format("blockchain_example").load()

    query = (
        df.writeStream
        .format("console")
        .queryName("admission_control_test")
        .start()
    )

    print("Streaming query started. Check the Streaming UI to verify:")
    print("- Each full batch should process 20 blocks")
    print("- The final batch may be smaller when fewer than 20 blocks remain")
    print()

    time.sleep(30)

    query.stop()
    print("\nQuery stopped - check Streaming UI for results")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
