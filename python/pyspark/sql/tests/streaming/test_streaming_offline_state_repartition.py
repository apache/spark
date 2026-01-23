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

import os
import tempfile
import unittest

from pyspark import SparkConf
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import LongType, StringType, StructType, StructField
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_pandas:
    import pandas as pd

if have_pyarrow:
    import pyarrow as pa  # noqa: F401


class StreamingOfflineStateRepartitionTests(ReusedSQLTestCase):
    """
    Test suite for Offline state repartitioning.
    """
    NUM_SHUFFLE_PARTITIONS = 3

    @classmethod
    def conf(cls):
        cfg = SparkConf()
        cfg.set("spark.sql.shuffle.partitions", str(cls.NUM_SHUFFLE_PARTITIONS))
        cfg.set(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
        )
        return cfg

    def test_fail_if_empty_checkpoint_directory(self):
        """Test that repartition fails if checkpoint directory is empty."""
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            with self.assertRaisesRegex(
                Exception,
                "STATE_REPARTITION_INVALID_CHECKPOINT.NO_COMMITTED_BATCH"
            ):
                self.spark._streamingCheckpointManager.repartition(checkpoint_dir, 5)

    def test_fail_if_no_batch_found_in_checkpoint_directory(self):
        """Test that repartition fails if no batch found in checkpoint directory."""
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            # Write commit log but no offset log
            commits_dir = os.path.join(checkpoint_dir, "commits")
            os.makedirs(commits_dir)
            # Create a minimal commit file for batch 0
            with open(os.path.join(commits_dir, "0"), "w") as f:
                f.write("v1\n{}")

            with self.assertRaisesRegex(
                Exception,
                "STATE_REPARTITION_INVALID_CHECKPOINT.NO_BATCH_FOUND"
            ):
                self.spark._streamingCheckpointManager.repartition(checkpoint_dir, 5)

    def test_fail_if_repartition_parameter_is_invalid(self):
        """Test that repartition fails with invalid parameters."""
        # Test null checkpoint location
        with self.assertRaisesRegex(
            Exception,
            "STATE_REPARTITION_INVALID_PARAMETER.IS_NULL"
        ):
            self.spark._streamingCheckpointManager.repartition(None, 5)

        # Test empty checkpoint location
        with self.assertRaisesRegex(
            Exception,
            "STATE_REPARTITION_INVALID_PARAMETER.IS_EMPTY"
        ):
            self.spark._streamingCheckpointManager.repartition("", 5)

        # Test numPartitions <= 0
        with self.assertRaisesRegex(
            Exception,
            "STATE_REPARTITION_INVALID_PARAMETER.IS_NOT_GREATER_THAN_ZERO"
        ):
            self.spark._streamingCheckpointManager.repartition("test", 0)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_repartition_with_apply_in_pandas_with_state(self):
        """
        Test repartition for a streaming query using applyInPandasWithState.

        1. Run streaming query to generate state (count per key)
        2. Stop the query
        3. Repartition to a different number of partitions
        4. Restart the query with new data
        5. Validate state is preserved (counts continue from where they left off)
        """
        with tempfile.TemporaryDirectory() as input_dir, \
             tempfile.TemporaryDirectory() as checkpoint_dir:

            # Collect results from foreachBatch
            collected_results = []

            def collect_batch(batch_df, batch_id):
                """ForeachBatch function to collect batch results for verification."""
                rows = batch_df.collect()
                collected_results.extend(rows)

            # Define the stateful function that tracks count per key
            def stateful_count_func(key, pdf_iter, state):
                """
                Stateful function that counts occurrences per key.
                State stores the running count.
                """
                # Get existing count from state or start at 0
                existing_count = state.getOption
                if existing_count is None:
                    count = 0
                else:
                    count = existing_count[0]

                # Count new rows
                new_count = 0
                for pdf in pdf_iter:
                    new_count += len(pdf)

                # Update state with new total
                total_count = count + new_count
                state.update((total_count,))

                # Output the key and current count
                yield pd.DataFrame({"key": [key[0]], "count": [total_count]})

            output_schema = StructType([
                StructField("key", StringType()),
                StructField("count", LongType())
            ])
            state_schema = StructType([StructField("count", LongType())])

            def run_streaming_query():
                """Helper to create and run the streaming query."""
                df = self.spark.readStream.format("text").load(input_dir)
                query = (
                    df.groupBy(df["value"])
                    .applyInPandasWithState(
                        stateful_count_func,
                        output_schema,
                        state_schema,
                        "Update",
                        GroupStateTimeout.NoTimeout
                    )
                    .writeStream
                    .foreachBatch(collect_batch)
                    .option("checkpointLocation", checkpoint_dir)
                    .outputMode("update")
                    .start()
                )
                query.processAllAvailable()
                query.stop()

            # Step 1: Write initial data and run streaming query
            with open(os.path.join(input_dir, "batch1.txt"), "w") as f:
                f.write("a\nb\na\nc\n")  # a:2, b:1, c:1

            run_streaming_query()

            # Verify initial state
            initial_counts = {row.key: row["count"] for row in collected_results}
            self.assertEqual(initial_counts.get("a"), 2)
            self.assertEqual(initial_counts.get("b"), 1)
            self.assertEqual(initial_counts.get("c"), 1)

            # Step 2: Repartition to a different number of partitions
            new_num_partitions = self.NUM_SHUFFLE_PARTITIONS * 2
            self.spark._streamingCheckpointManager.repartition(
                checkpoint_dir, new_num_partitions
            )

            # Step 3: Add more data and restart query
            with open(os.path.join(input_dir, "batch2.txt"), "w") as f:
                f.write("a\nb\nd\n")  # a:+1, b:+1, d:1 (new key)

            # Clear collected results for restart
            collected_results.clear()

            run_streaming_query()

            # Step 4: Validate state was preserved after repartition
            # After restart with new data:
            # - a should have count 3 (2 from before + 1 new)
            # - b should have count 2 (1 from before + 1 new)
            # - c should have count 1 (no new data, but state preserved)
            # - d should have count 1 (new key)
            final_counts = {row.key: row["count"] for row in collected_results}

            # Only updated keys will be in the output (a, b, d)
            self.assertEqual(final_counts.get("a"), 3, "State for 'a' should be preserved: 2 + 1 = 3")
            self.assertEqual(final_counts.get("b"), 2, "State for 'b' should be preserved: 1 + 1 = 2")
            self.assertEqual(final_counts.get("d"), 1, "New key 'd' should have count 1")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
