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
Demonstrates admission control in Python streaming data sources.

This example implements a simple blockchain-like streaming source that generates
sequential blocks and shows how to use admission control to limit batch sizes.

Usage: structured_blockchain_admission_control.py [<max-blocks-per-batch>]
  <max-blocks-per-batch> Maximum number of blocks to process per microbatch
                         (default: 10)

Run the example:
   `$ bin/spark-submit examples/src/main/python/sql/streaming/\\
structured_blockchain_admission_control.py 5`

The example will process blocks in controlled batches of 5,
demonstrating admission control.
"""
import sys
import time
from typing import Dict, Iterator, List, Optional, Tuple, Union

from pyspark.sql import SparkSession
from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamReader,
    InputPartition,
)
from pyspark.sql.types import StructType


class SimpleBlockchainReader(DataSourceStreamReader):
    """A simple streaming source that generates sequential blockchain blocks."""

    def __init__(self, max_block: int = 1000, max_blocks_per_batch: int = 0) -> None:
        self.max_block = max_block
        self.max_blocks_per_batch = max_blocks_per_batch
        self.current_block = 0

    def initialOffset(self) -> Dict[str, int]:
        """Start from block 0."""
        return {"block": self.current_block}

    def latestOffset(
        self,
        start: Optional[Dict[str, int]] = None,
    ) -> Union[Dict[str, int], Tuple[Dict[str, int], Dict[str, int]]]:
        """
        Return the latest offset, respecting admission control limits.

        This demonstrates the key admission control pattern:
        - Without admission control: process all available blocks
        - With maxRecordsPerBatch: cap the end block to respect batch size
        """
        # Determine where we are now
        if start is None:
            start_block = self.current_block
        else:
            start_block = start["block"]

        # Simulate blockchain growth - advance by 20 blocks each time
        latest_available = min(start_block + 20, self.max_block)

        # Apply admission control if configured
        if self.max_blocks_per_batch > 0:
            # Cap at the configured limit
            end_block = min(start_block + self.max_blocks_per_batch, latest_available)
            print(
                f"  [Admission Control] Start: {start_block}, "
                f"Available: {latest_available}, Capped: {end_block} "
                f"(maxRecordsPerBatch: {self.max_blocks_per_batch})"
            )
            # Return tuple: (capped_offset, true_latest_offset)
            return ({"block": end_block}, {"block": latest_available})
        else:
            # No limit - process all available
            end_block = latest_available
            print(f"  [No Limit] Start: {start_block}, End: {end_block}")
            return {"block": end_block}

    def partitions(
        self, start: Dict[str, int], end: Dict[str, int]
    ) -> List[InputPartition]:
        """Create a single partition for the block range."""
        start_block = start["block"]
        end_block = end["block"]
        return [InputPartition(f"{start_block}:{end_block}".encode())]

    def read(self, partition: InputPartition) -> Iterator[Tuple[int, int, str]]:
        """Generate block data for the partition."""
        # Parse the block range
        range_str = partition.value.decode()
        start_block, end_block = map(int, range_str.split(":"))

        # Generate block data
        for block_num in range(start_block, end_block):
            # Simulate block data: block number, timestamp, simple hash
            yield (
                block_num,
                int(time.time() * 1000),
                f"0x{'0' * 60}{block_num:04x}",
            )

    def commit(self, end: Dict[str, int]) -> None:
        """Mark this offset as committed."""
        pass


class SimpleBlockchainSource(DataSource):
    """Data source for simple blockchain streaming."""

    @classmethod
    def name(cls) -> str:
        return "simple_blockchain"

    def schema(self) -> str:
        return "block_number INT, timestamp LONG, block_hash STRING"

    def streamReader(self, schema: StructType) -> SimpleBlockchainReader:
        value = self.options.get("maxRecordsPerBatch")
        try:
            max_blocks_per_batch = int(value) if value is not None else 0
        except ValueError:
            max_blocks_per_batch = 0
        return SimpleBlockchainReader(max_block=1000, max_blocks_per_batch=max_blocks_per_batch)


if __name__ == "__main__":
    max_blocks_per_batch = int(sys.argv[1]) if len(sys.argv) > 1 else 10

    print(
        f"""
=================================================================
Blockchain Streaming with Admission Control
=================================================================
Configuration:
  - Max blocks per batch: {max_blocks_per_batch}
  - Total blocks to generate: 1000

Watch how admission control limits each microbatch to process
only {max_blocks_per_batch} blocks at a time, even when more data is available.
=================================================================
"""
    )
    # fmt: off
    spark = (
        SparkSession.builder.appName("StructuredBlockchainAdmissionControl").getOrCreate()
    )
    # fmt: on

    # Register the custom data source
    spark.dataSource.register(SimpleBlockchainSource)

    # Create streaming DataFrame with admission control
    blocks = (
        spark.readStream.format("simple_blockchain")
        .option("maxRecordsPerBatch", str(max_blocks_per_batch))
        .load()
    )

    # Show block statistics per microbatch
    query = (
        blocks.writeStream.outputMode("append")
        .format("console")
        .option("numRows", "20")
        .option("truncate", "false")
        .trigger(processingTime="3 seconds")
        .start()
    )

    query.awaitTermination()
