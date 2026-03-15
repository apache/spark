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

This example shows how to build a custom streaming data source that simulates
reading blockchain blocks while respecting admission control limits. It also
demonstrates the SupportsTriggerAvailableNow mixin for Trigger.AvailableNow support.

Key concepts demonstrated:
- DataSourceStreamReader with latestOffset(start, limit) for admission control
- ReadMaxRows to limit blocks consumed per micro-batch
- SupportsTriggerAvailableNow for deterministic batch processing
- reportLatestOffset() for progress monitoring

Usage:
    bin/spark-submit examples/src/main/python/sql/streaming/\\
        structured_blockchain_admission_control.py

Expected output:
    Batch 0: 2 blocks (block_number: 0, 1)
    Batch 1: 2 blocks (block_number: 2, 3)
    ...
"""

import hashlib
import time
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.streaming.datasource import (
    ReadAllAvailable,
    ReadLimit,
    ReadMaxRows,
    SupportsTriggerAvailableNow,
)
from pyspark.sql.types import StructType


class BlockPartition(InputPartition):
    """Partition representing a range of blockchain blocks to read."""

    def __init__(self, start_block: int, end_block: int):
        self.start_block = start_block
        self.end_block = end_block


class BlockchainStreamReader(DataSourceStreamReader, SupportsTriggerAvailableNow):
    """
    A streaming reader that simulates reading blockchain blocks.

    This reader generates simulated blockchain data while demonstrating:
    - Admission control via ReadMaxRows limiting blocks per batch
    - SupportsTriggerAvailableNow for finite processing with AvailableNow trigger
    """

    # Simulated chain height (total blocks available)
    CHAIN_HEIGHT = 1000000

    def __init__(self) -> None:
        self._trigger_available_now_target: Optional[int] = None

    def initialOffset(self) -> dict:
        """Return the starting block number for new queries."""
        return {"block_number": 0}

    def getDefaultReadLimit(self) -> ReadLimit:
        """
        Limit each micro-batch to 2 blocks by default.

        This simulates a scenario where we want to process blocks slowly,
        perhaps due to rate limits on a real blockchain API.
        """
        return ReadMaxRows(2)

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

        # Determine the maximum block we can read up to
        max_available = self.CHAIN_HEIGHT
        if self._trigger_available_now_target is not None:
            # SupportsTriggerAvailableNow: don't exceed the target set at query start
            max_available = self._trigger_available_now_target

        # Edge case: Already at end - return same offset to signal completion
        if start_block >= max_available:
            return start

        if isinstance(limit, ReadAllAvailable):
            # Trigger.Once or Trigger.AvailableNow - read ALL available blocks
            # Per SupportsAdmissionControl contract, must ignore source options
            end_block = max_available
        elif isinstance(limit, ReadMaxRows):
            # Respect the row/block limit
            end_block = min(start_block + limit.max_rows, max_available)
        else:
            # Fallback
            end_block = min(start_block + 2, max_available)

        return {"block_number": end_block}

    def prepareForTriggerAvailableNow(self) -> None:
        """
        Capture the current chain height for AvailableNow trigger.

        When using Trigger.AvailableNow, we record the chain height at query
        start and won't process blocks beyond that point, even if new blocks
        arrive during processing.
        """
        # In a real implementation, you would query the actual chain height
        # For demo, we simulate having 20 blocks to process
        self._trigger_available_now_target = 20

    def reportLatestOffset(self) -> Optional[dict]:
        """Report the highest known block for progress monitoring."""
        return {"block_number": self.CHAIN_HEIGHT}

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
        - transaction_count: Random transaction count
        """
        assert isinstance(partition, BlockPartition)

        for block_num in range(partition.start_block, partition.end_block):
            # Generate deterministic "hash" for reproducibility
            block_hash = hashlib.sha256(str(block_num).encode()).hexdigest()[:16]
            # Simulated timestamp (increasing with block number)
            timestamp = 1700000000 + (block_num * 12)
            # Simulated transaction count
            tx_count = (block_num % 100) + 1

            yield (block_num, block_hash, timestamp, tx_count)


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
    """Run the blockchain admission control streaming example."""
    spark: SparkSession = SparkSession.builder.appName("BlockchainAdmissionControl").getOrCreate()

    # Register custom data source
    spark.dataSource.register(BlockchainDataSource)

    # Create streaming DataFrame
    df = spark.readStream.format("blockchain_example").load()

    # Track blocks processed
    blocks_processed: List[Dict[str, Any]] = []

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        """Process each micro-batch of blocks."""
        count = batch_df.count()
        blocks = [row.asDict() for row in batch_df.collect()]
        blocks_processed.extend(blocks)

        print(f"\nBatch {batch_id}: {count} blocks processed")
        for block in blocks:
            print(f"  Block {block['block_number']}: hash={block['block_hash']}, "
                  f"tx_count={block['transaction_count']}")

    # Start streaming query
    query = df.writeStream.foreachBatch(process_batch).start()

    try:
        # Let it run for a few seconds to process some batches
        time.sleep(5)
    finally:
        query.stop()
        query.awaitTermination()

    # Summary
    print("\n--- Blockchain Stream Summary ---")
    print(f"Total blocks processed: {len(blocks_processed)}")
    if blocks_processed:
        first_block = blocks_processed[0]["block_number"]
        last_block = blocks_processed[-1]["block_number"]
        print(f"Block range: {first_block} to {last_block}")

    spark.stop()


if __name__ == "__main__":
    main()
