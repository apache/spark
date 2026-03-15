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
reading blockchain blocks while respecting admission control limits. It demonstrates
the SupportsTriggerAvailableNow mixin for deterministic batch processing.

Key concepts demonstrated:
- DataSourceStreamReader with latestOffset(start, limit) for admission control
- ReadMaxRows to limit blocks consumed per micro-batch
- SupportsTriggerAvailableNow mixin for Trigger.AvailableNow support
- prepareForTriggerAvailableNow() capturing target offset at query start
- reportLatestOffset() for progress monitoring
- Comparing behavior between default trigger and Trigger.AvailableNow

The example runs TWO streaming queries:

1. **Default Trigger**: Processes 2 blocks per micro-batch continuously
   - Simulates slowly processing new blocks as they arrive
   - Could run indefinitely in a real blockchain scenario

2. **Trigger.AvailableNow with SupportsTriggerAvailableNow**:
   - prepareForTriggerAvailableNow() captures current chain height (20 blocks)
   - Processes ALL 20 blocks even though new blocks keep arriving
   - Stops automatically when reaching the captured target offset
   - Demonstrates deterministic catch-up processing

Usage:
    bin/spark-submit examples/src/main/python/sql/streaming/\\
        structured_blockchain_admission_control.py

Expected output:
    DEFAULT TRIGGER - 2 blocks per batch:
        Batch 0: blocks 0-1
        Batch 1: blocks 2-3
        ...

    TRIGGER.AVAILABLENOW - All 20 blocks, then stop:
        prepareForTriggerAvailableNow() called, target = block 20
        Batch 0: blocks 0-19 (or multiple batches depending on limit)
        Query stopped at block 20 (ignoring blocks 21+)
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

    # Simulated chain height (total blocks available in the "live" chain)
    # In reality, this keeps growing, but we keep it finite for demo purposes
    CHAIN_HEIGHT = 100

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
        # For demo, we simulate having 20 blocks to process at query start
        self._trigger_available_now_target = 20
        target = self._trigger_available_now_target
        print(f"[prepareForTriggerAvailableNow] Captured target offset: block {target}")
        print(f"[prepareForTriggerAvailableNow] Will stop at block {target}, "
              f"even if chain grows beyond it")

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


def run_with_default_trigger(spark: SparkSession) -> None:
    """
    Demonstrate blockchain streaming with DEFAULT trigger.

    Processes blocks continuously, 2 blocks per micro-batch as defined by
    getDefaultReadLimit().
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 1: DEFAULT TRIGGER (Continuous Block Processing)")
    print("=" * 70)
    print("Expected: Process 2 blocks per batch, run continuously")
    print()

    df = spark.readStream.format("blockchain_example").load()

    blocks_processed: List[Dict[str, Any]] = []

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        blocks = [row.asDict() for row in batch_df.collect()]
        blocks_processed.extend(blocks)

        print(f"  Batch {batch_id}: {count} blocks")
        if blocks:
            block_nums = [b["block_number"] for b in blocks]
            print(f"    Block numbers: {block_nums}")

    # Default trigger - processes continuously
    query = df.writeStream.foreachBatch(process_batch).start()

    try:
        # Run for a few batches
        time.sleep(3)
    finally:
        query.stop()
        query.awaitTermination()

    print("\n--- Default Trigger Summary ---")
    print(f"Total blocks processed: {len(blocks_processed)}")
    if blocks_processed:
        first = blocks_processed[0]["block_number"]
        last = blocks_processed[-1]["block_number"]
        print(f"Block range: {first} to {last}")
        print("✓ Each batch limited to 2 blocks (admission control working)")


def run_with_trigger_available_now(spark: SparkSession) -> None:
    """
    Demonstrate SupportsTriggerAvailableNow with TRIGGER.AVAILABLENOW.

    This demonstrates the key behavior:
    1. prepareForTriggerAvailableNow() is called at query start
    2. It captures the target offset (block 20)
    3. Processing continues until block 20, then stops
    4. Even if the chain has more blocks (up to 100), we stop at 20
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: TRIGGER.AVAILABLENOW with SupportsTriggerAvailableNow")
    print("=" * 70)
    print("Expected: prepareForTriggerAvailableNow() captures block 20 as target")
    print("          Process all blocks up to block 20, then stop")
    print("          Ignore blocks beyond 20 even though chain has 100 blocks")
    print()

    df = spark.readStream.format("blockchain_example").load()

    blocks_processed: List[Dict[str, Any]] = []

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        blocks = [row.asDict() for row in batch_df.collect()]
        blocks_processed.extend(blocks)

        print(f"  Batch {batch_id}: {count} blocks")
        if blocks:
            block_nums = [b["block_number"] for b in blocks]
            print(f"    Block numbers: {block_nums}")
            # Show some block details
            for block in blocks[:3]:  # Show first 3
                print(f"      Block {block['block_number']}: "
                      f"hash={block['block_hash']}, tx={block['transaction_count']}")
            if len(blocks) > 3:
                print(f"      ... and {len(blocks) - 3} more blocks")

    # Trigger.AvailableNow - will call prepareForTriggerAvailableNow()
    print("Starting query with Trigger.AvailableNow...")
    query = df.writeStream \
        .trigger(availableNow=True) \
        .foreachBatch(process_batch) \
        .start()

    # Wait for completion - stops automatically
    query.awaitTermination()

    print("\n--- Trigger.AvailableNow Summary ---")
    print(f"Total blocks processed: {len(blocks_processed)}")
    if blocks_processed:
        first = blocks_processed[0]["block_number"]
        last = blocks_processed[-1]["block_number"]
        print(f"Block range: {first} to {last}")
        print(f"\n✓ Stopped at block {last} (target from prepareForTriggerAvailableNow)")
        print(f"✓ Did NOT process blocks beyond {last}, even though chain has 100 blocks")
        print("✓ Demonstrates deterministic catch-up processing")


def main() -> None:
    """Run blockchain streaming examples with both trigger types."""
    spark: SparkSession = SparkSession.builder \
        .appName("BlockchainAdmissionControl") \
        .getOrCreate()

    # Register custom data source
    spark.dataSource.register(BlockchainDataSource)

    print("\n" + "=" * 70)
    print("BLOCKCHAIN ADMISSION CONTROL WITH SUPPORTSTRIGGERAVAILABLENOW")
    print("=" * 70)
    print("\nThis example demonstrates SupportsTriggerAvailableNow mixin for")
    print("deterministic batch processing of blockchain data.")
    print("\nData Source: Simulated blockchain with 100 blocks")
    print("Default Read Limit: ReadMaxRows(2)")
    print("AvailableNow Target: First 20 blocks (set in prepareForTriggerAvailableNow)")

    # Run both examples
    run_with_default_trigger(spark)
    run_with_trigger_available_now(spark)

    print("\n" + "=" * 70)
    print("KEY TAKEAWAYS")
    print("=" * 70)
    print("1. DEFAULT TRIGGER: Processes 2 blocks/batch continuously")
    print("2. TRIGGER.AVAILABLENOW: Calls prepareForTriggerAvailableNow() at start")
    print("3. Target offset (block 20) captured and enforced in latestOffset()")
    print("4. Query stops at block 20, ignoring blocks 21-100")
    print("5. Useful for catch-up processing with deterministic boundaries")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
