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

The data source generates sequential integers, but admission control limits
each micro-batch to only 2 rows, demonstrating how streaming sources can
throttle data consumption.

Usage:
    bin/spark-submit examples/src/main/python/sql/streaming/\\
        python_datasource_admission_control.py

Expected output shows batches limited to 2 rows each:
    Batch 0: 2 rows (0, 1)
    Batch 1: 2 rows (2, 3)
    ...
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

    # Simulated "total available" data - in practice this could be unbounded
    TOTAL_AVAILABLE = 1000000

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


def main() -> None:
    """Run the admission control streaming example."""
    spark: SparkSession = SparkSession.builder.appName("AdmissionControlExample").getOrCreate()

    # Register the custom data source
    spark.dataSource.register(AdmissionControlDataSource)

    # Create streaming DataFrame from our custom source
    df = spark.readStream.format("admission_control_example").load()

    # Track batch statistics
    batch_stats: List[Dict[str, Any]] = []

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        """Process each micro-batch and track statistics."""
        count = batch_df.count()
        rows = [row.asDict() for row in batch_df.collect()]
        batch_stats.append({"batch_id": batch_id, "count": count, "rows": rows})

        print(f"Batch {batch_id}: {count} rows processed")
        if rows:
            ids = [r["id"] for r in rows]
            print(f"  IDs: {ids}")

    # Start streaming query with foreachBatch
    query = df.writeStream.foreachBatch(process_batch).start()

    # Wait for a few batches to complete
    try:
        # Process batches for a short time to demonstrate admission control
        time.sleep(5)
    finally:
        query.stop()
        query.awaitTermination()

    # Print summary
    print("\n--- Admission Control Summary ---")
    print(f"Total batches processed: {len(batch_stats)}")
    for stat in batch_stats:
        print(f"  Batch {stat['batch_id']}: {stat['count']} rows")

    if batch_stats:
        row_counts = [s["count"] for s in batch_stats]
        print(f"\nAll batches limited to max 2 rows: {all(c <= 2 for c in row_counts)}")

    spark.stop()


if __name__ == "__main__":
    main()
