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
Demonstrates admission control in custom Python streaming data sources.

This example shows how to implement a custom streaming data source that respects
admission control limits using the PySpark DataSource API introduced in Spark 4.0.

Key concepts demonstrated:
- Implementing DataSourceStreamReader with latestOffset(start, limit)
- Using ReadMaxRows to limit data consumption per micro-batch
- Implementing getDefaultReadLimit() for default rate limiting
- Using reportLatestOffset() for monitoring
- Comparing behavior between default trigger and Trigger.AvailableNow

The example runs TWO streaming queries to demonstrate the difference:

1. **Default Trigger**: Each micro-batch processes only 2 rows (limited by getDefaultReadLimit)
   - Runs continuously until manually stopped
   - Demonstrates throttled data consumption

2. **Trigger.AvailableNow**: Processes ALL available data then stops automatically
   - latestOffset receives ReadAllAvailable instead of ReadMaxRows
   - Ignores the default read limit per SupportsAdmissionControl contract
   - Useful for one-time catch-up processing

Usage:
    bin/spark-submit examples/src/main/python/sql/streaming/\\
        python_datasource_admission_control.py

Expected output:
    DEFAULT TRIGGER - Batches limited to 2 rows each:
        Batch 0: 2 rows (0, 1)
        Batch 1: 2 rows (2, 3)
        ...

    TRIGGER.AVAILABLENOW - Processes all 10 rows then stops:
        Batch 0: 10 rows (0-9)
        Query stopped automatically
"""

import time
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadLimit, ReadMaxRows
from pyspark.sql.types import StructType


class RangePartition(InputPartition):
    """A partition representing a range of integer values to read."""

    def __init__(self, start: int, end: int):
        self.start = start
        self.end = end


class AdmissionControlStreamReader(DataSourceStreamReader):
    """
    A streaming reader that demonstrates admission control.

    This reader generates sequential integers but respects ReadLimit constraints,
    allowing Spark to control how much data is consumed per micro-batch.
    """

    # Simulated "total available" data - kept small for demo purposes
    TOTAL_AVAILABLE = 10

    def initialOffset(self) -> dict:
        """Return the starting offset for new queries."""
        return {"offset": 0}

    def getDefaultReadLimit(self) -> ReadLimit:
        """
        Return the default read limit for this source.

        By returning ReadMaxRows(2), we limit each micro-batch to 2 rows
        unless the engine explicitly requests more (e.g., with Trigger.Once).
        """
        return ReadMaxRows(2)

    def latestOffset(self, start: dict, limit: ReadLimit) -> dict:
        """
        Compute the end offset for the current micro-batch.

        Parameters
        ----------
        start : dict
            The starting offset from the previous micro-batch
        limit : ReadLimit
            The maximum amount of data to include in this batch

        Returns
        -------
        dict
            The ending offset for this micro-batch
        """
        start_idx = start["offset"]

        # Edge case: Already at end - return same offset to signal completion
        if start_idx >= self.TOTAL_AVAILABLE:
            return start

        if isinstance(limit, ReadAllAvailable):
            # Trigger.Once or Trigger.AvailableNow - read ALL available data
            # Per SupportsAdmissionControl contract, must ignore source options
            end_offset = self.TOTAL_AVAILABLE
        elif isinstance(limit, ReadMaxRows):
            # Normal processing - respect the row limit
            end_offset = min(start_idx + limit.max_rows, self.TOTAL_AVAILABLE)
        else:
            # Fallback for other limit types
            end_offset = min(start_idx + 2, self.TOTAL_AVAILABLE)

        return {"offset": end_offset}

    def reportLatestOffset(self) -> Optional[dict]:
        """
        Report the latest available offset for monitoring.

        This is used by Spark for progress reporting and does not affect
        actual data consumption.
        """
        return {"offset": self.TOTAL_AVAILABLE}

    def partitions(self, start: dict, end: dict) -> Sequence[InputPartition]:
        """
        Create partitions for the given offset range.

        For simplicity, we create one partition per row. In production,
        you might batch multiple rows into fewer partitions.
        """
        start_offset = start["offset"]
        end_offset = end["offset"]

        if start_offset >= end_offset:
            return []

        return [RangePartition(start_offset, end_offset)]

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        """
        Read data for a given partition.

        Yields tuples of (id, value) for each integer in the partition range.
        """
        assert isinstance(partition, RangePartition)
        for i in range(partition.start, partition.end):
            yield (i, f"value_{i}")


class AdmissionControlDataSource(DataSource):
    """
    A data source that creates AdmissionControlStreamReader instances.
    """

    @classmethod
    def name(cls) -> str:
        return "admission_control_example"

    def schema(self) -> str:
        return "id INT, value STRING"

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        return AdmissionControlStreamReader()


def run_with_default_trigger(spark: SparkSession) -> None:
    """
    Demonstrate admission control with DEFAULT trigger.

    With default trigger, getDefaultReadLimit() returns ReadMaxRows(2),
    so each micro-batch processes only 2 rows at a time.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 1: DEFAULT TRIGGER (Continuous Processing)")
    print("=" * 70)
    print("Expected behavior: Each batch processes max 2 rows (from getDefaultReadLimit)")
    print()

    df = spark.readStream.format("admission_control_example").load()

    batch_stats: List[Dict[str, Any]] = []

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        rows = [row.asDict() for row in batch_df.collect()]
        batch_stats.append({"batch_id": batch_id, "count": count})

        print(f"  Batch {batch_id}: {count} rows")
        if rows:
            ids = [r["id"] for r in rows]
            print(f"    IDs: {ids}")

    # Use default trigger - will respect getDefaultReadLimit()
    query = df.writeStream.foreachBatch(process_batch).start()

    try:
        # Let it run for a few batches
        time.sleep(3)
    finally:
        query.stop()
        query.awaitTermination()

    print("\n--- Default Trigger Summary ---")
    print(f"Total batches: {len(batch_stats)}")
    for stat in batch_stats:
        print(f"  Batch {stat['batch_id']}: {stat['count']} rows")

    if batch_stats:
        row_counts = [s["count"] for s in batch_stats]
        all_limited = all(c <= 2 for c in row_counts)
        print(f"\nAll batches limited to 2 rows: {all_limited}")
        print("✓ Admission control working - each batch throttled to 2 rows")


def run_with_trigger_available_now(spark: SparkSession) -> None:
    """
    Demonstrate admission control with TRIGGER.AVAILABLENOW.

    With Trigger.AvailableNow, latestOffset receives ReadAllAvailable,
    which tells the source to process ALL available data and then stop.
    This ignores getDefaultReadLimit() per the SupportsAdmissionControl contract.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: TRIGGER.AVAILABLENOW (Finite Processing)")
    print("=" * 70)
    print("Expected behavior: Process ALL 10 rows in one batch, then stop")
    print()

    df = spark.readStream.format("admission_control_example").load()

    batch_stats: List[Dict[str, Any]] = []

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        rows = [row.asDict() for row in batch_df.collect()]
        batch_stats.append({"batch_id": batch_id, "count": count})

        print(f"  Batch {batch_id}: {count} rows")
        if rows:
            ids = [r["id"] for r in rows]
            print(f"    IDs: {ids}")

    # Use Trigger.AvailableNow - will receive ReadAllAvailable
    query = df.writeStream \
        .trigger(availableNow=True) \
        .foreachBatch(process_batch) \
        .start()

    # Wait for completion - AvailableNow stops automatically
    query.awaitTermination()

    print("\n--- Trigger.AvailableNow Summary ---")
    print(f"Total batches: {len(batch_stats)}")
    for stat in batch_stats:
        print(f"  Batch {stat['batch_id']}: {stat['count']} rows")

    total_rows = sum(s["count"] for s in batch_stats)
    print(f"\nTotal rows processed: {total_rows}")
    print("✓ Processed all available data (10 rows) then stopped automatically")
    print("✓ Ignored getDefaultReadLimit() as required by SupportsAdmissionControl")


def main() -> None:
    """Run admission control examples with both trigger types."""
    spark: SparkSession = SparkSession.builder \
        .appName("AdmissionControlExample") \
        .getOrCreate()

    # Register the custom data source
    spark.dataSource.register(AdmissionControlDataSource)

    print("\n" + "=" * 70)
    print("ADMISSION CONTROL AND TRIGGER.AVAILABLENOW DEMONSTRATION")
    print("=" * 70)
    print("\nThis example demonstrates how admission control interacts with")
    print("different trigger types in PySpark streaming data sources.")
    print("\nData Source: 10 sequential integers (0-9)")
    print("Default Read Limit: ReadMaxRows(2)")

    # Run both examples
    run_with_default_trigger(spark)
    run_with_trigger_available_now(spark)

    print("\n" + "=" * 70)
    print("KEY TAKEAWAYS")
    print("=" * 70)
    print("1. DEFAULT TRIGGER: Respects getDefaultReadLimit() - processes 2 rows/batch")
    print("2. TRIGGER.AVAILABLENOW: Receives ReadAllAvailable - processes ALL data")
    print("3. latestOffset(start, limit) receives different ReadLimit types")
    print("4. Sources can throttle continuous streams while supporting batch catch-up")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
