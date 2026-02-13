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
from typing import cast

from pyspark import SparkConf
from pyspark.sql.functions import split
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.tests.pandas.helper.helper_pandas_transform_with_state import (
    SimpleStatefulProcessorWithInitialStateFactory,
    StatefulProcessorCompositeTypeFactory,
)
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


class OfflineStateRepartitionTestUtils:
    """Utility class for repartition tests."""

    @staticmethod
    def run_repartition_test(
        spark,
        num_shuffle_partitions,
        create_streaming_df,
        output_mode,
        batch1_data,
        batch2_data,
        batch3_data,
        verify_initial,
        verify_after_increase,
        verify_after_decrease,
    ):
        """
        Common helper to run repartition tests with different streaming operators.

        Steps:
        1. Run streaming query to generate initial state
        2. Repartition to more partitions
        3. Add new data and restart query, verify state preserved
        4. Repartition to fewer partitions
        5. Add new data and restart query, verify state preserved

        Parameters
        ----------
        spark : SparkSession
            The active Spark session.
        num_shuffle_partitions : int
            The initial number of shuffle partitions.
        create_streaming_df : callable
            Function(df) -> DataFrame that applies the streaming transformation.
        output_mode : str
            Output mode for the streaming query ("update" or "append").
        batch1_data, batch2_data, batch3_data : str
            Data to write for each batch (newline-separated values).
        verify_initial, verify_after_increase, verify_after_decrease : callable
            Functions(collected_results) to verify results at each stage.
        """
        with tempfile.TemporaryDirectory() as input_dir, tempfile.TemporaryDirectory() as checkpoint_dir:
            collected_results = []

            def collect_batch(batch_df, batch_id):
                rows = batch_df.collect()
                collected_results.extend(rows)

            def run_streaming_query():
                df = spark.readStream.format("text").load(input_dir)
                transformed_df = create_streaming_df(df)
                query = (
                    transformed_df.writeStream.foreachBatch(collect_batch)
                    .option("checkpointLocation", checkpoint_dir)
                    .outputMode(output_mode)
                    .start()
                )
                query.processAllAvailable()
                query.stop()

            # Step 1: Write initial data and run streaming query
            with open(os.path.join(input_dir, "batch1.txt"), "w") as f:
                f.write(batch1_data)

            run_streaming_query()
            verify_initial(collected_results)

            # Step 2: Repartition to more partitions
            spark._streamingCheckpointManager.repartition(
                checkpoint_dir, num_shuffle_partitions * 2
            )

            # Step 3: Add more data and restart query
            with open(os.path.join(input_dir, "batch2.txt"), "w") as f:
                f.write(batch2_data)

            collected_results.clear()
            run_streaming_query()
            verify_after_increase(collected_results)

            # Step 4: Repartition to fewer partitions
            spark._streamingCheckpointManager.repartition(
                checkpoint_dir, num_shuffle_partitions - 1
            )

            # Step 5: Add more data and restart query
            with open(os.path.join(input_dir, "batch3.txt"), "w") as f:
                f.write(batch3_data)

            collected_results.clear()
            run_streaming_query()
            verify_after_decrease(collected_results)


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
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        return cfg

    def test_fail_if_empty_checkpoint_directory(self):
        """Test that repartition fails if checkpoint directory is empty."""
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            with self.assertRaisesRegex(
                Exception, "STATE_REPARTITION_INVALID_CHECKPOINT.NO_COMMITTED_BATCH"
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
                Exception, "STATE_REPARTITION_INVALID_CHECKPOINT.NO_BATCH_FOUND"
            ):
                self.spark._streamingCheckpointManager.repartition(checkpoint_dir, 5)

    def test_fail_if_repartition_parameter_is_invalid(self):
        """Test that repartition fails with invalid parameters."""
        # Test null checkpoint location
        with self.assertRaisesRegex(Exception, "STATE_REPARTITION_INVALID_PARAMETER.IS_NULL"):
            self.spark._streamingCheckpointManager.repartition(None, 5)

        # Test empty checkpoint location
        with self.assertRaisesRegex(Exception, "STATE_REPARTITION_INVALID_PARAMETER.IS_EMPTY"):
            self.spark._streamingCheckpointManager.repartition("", 5)

        # Test numPartitions <= 0
        with self.assertRaisesRegex(
            Exception, "STATE_REPARTITION_INVALID_PARAMETER.IS_NOT_GREATER_THAN_ZERO"
        ):
            self.spark._streamingCheckpointManager.repartition("test", 0)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_repartition_with_apply_in_pandas_with_state(self):
        """Test repartition for a streaming query using applyInPandasWithState."""

        # Define the stateful function that tracks count per key
        def stateful_count_func(key, pdf_iter, state):
            existing_count = state.getOption
            count = 0 if existing_count is None else existing_count[0]

            new_count = 0
            for pdf in pdf_iter:
                new_count += len(pdf)

            total_count = count + new_count
            state.update((total_count,))
            yield pd.DataFrame({"key": [key[0]], "count": [total_count]})

        output_schema = StructType(
            [StructField("key", StringType()), StructField("count", LongType())]
        )
        state_schema = StructType([StructField("count", LongType())])

        def create_streaming_df(df):
            return df.groupBy(df["value"]).applyInPandasWithState(
                stateful_count_func,
                output_schema,
                state_schema,
                "Update",
                GroupStateTimeout.NoTimeout,
            )

        def verify_initial(results):
            counts = {row.key: row["count"] for row in results}
            self.assertEqual(counts.get("a"), 2)
            self.assertEqual(counts.get("b"), 1)
            self.assertEqual(counts.get("c"), 1)

        def verify_after_increase(results):
            counts = {row.key: row["count"] for row in results}
            self.assertEqual(counts.get("a"), 3, "State for 'a': 2 + 1 = 3")
            self.assertEqual(counts.get("b"), 2, "State for 'b': 1 + 1 = 2")
            self.assertEqual(counts.get("d"), 1, "New key 'd' should have count 1")

        def verify_after_decrease(results):
            counts = {row.key: row["count"] for row in results}
            self.assertEqual(counts.get("a"), 4, "State for 'a': 3 + 1 = 4")
            self.assertEqual(counts.get("c"), 2, "State for 'c': 1 + 1 = 2")
            self.assertEqual(counts.get("e"), 1, "New key 'e' should have count 1")

        OfflineStateRepartitionTestUtils.run_repartition_test(
            spark=self.spark,
            num_shuffle_partitions=self.NUM_SHUFFLE_PARTITIONS,
            create_streaming_df=create_streaming_df,
            output_mode="update",
            batch1_data="a\nb\na\nc\n",  # a:2, b:1, c:1
            batch2_data="a\nb\nd\n",  # a:+1, b:+1, d:1 (new)
            batch3_data="a\nc\ne\n",  # a:+1, c:+1, e:1 (new)
            verify_initial=verify_initial,
            verify_after_increase=verify_after_increase,
            verify_after_decrease=verify_after_decrease,
        )

    def test_repartition_with_streaming_aggregation(self):
        """Test repartition for a streaming aggregation query (groupBy + count)."""

        def create_streaming_df(df):
            return df.groupBy("value").count()

        def verify_initial(results):
            counts = {row.value: row["count"] for row in results}
            self.assertEqual(counts.get("a"), 2)
            self.assertEqual(counts.get("b"), 1)
            self.assertEqual(counts.get("c"), 1)

        def verify_after_increase(results):
            counts = {row.value: row["count"] for row in results}
            self.assertEqual(counts.get("a"), 3, "State for 'a': 2 + 1 = 3")
            self.assertEqual(counts.get("b"), 2, "State for 'b': 1 + 1 = 2")
            self.assertEqual(counts.get("d"), 1, "New key 'd' should have count 1")

        def verify_after_decrease(results):
            counts = {row.value: row["count"] for row in results}
            self.assertEqual(counts.get("a"), 4, "State for 'a': 3 + 1 = 4")
            self.assertEqual(counts.get("c"), 2, "State for 'c': 1 + 1 = 2")
            self.assertEqual(counts.get("e"), 1, "New key 'e' should have count 1")

        OfflineStateRepartitionTestUtils.run_repartition_test(
            spark=self.spark,
            num_shuffle_partitions=self.NUM_SHUFFLE_PARTITIONS,
            create_streaming_df=create_streaming_df,
            output_mode="update",
            batch1_data="a\nb\na\nc\n",  # a:2, b:1, c:1
            batch2_data="a\nb\nd\n",  # a:+1, b:+1, d:1 (new)
            batch3_data="a\nc\ne\n",  # a:+1, c:+1, e:1 (new)
            verify_initial=verify_initial,
            verify_after_increase=verify_after_increase,
            verify_after_decrease=verify_after_decrease,
        )

    def test_repartition_with_streaming_dedup(self):
        """Test repartition for a streaming deduplication query (dropDuplicates)."""

        def create_streaming_df(df):
            return df.dropDuplicates(["value"])

        def verify_initial(results):
            values = {row.value for row in results}
            self.assertEqual(values, {"a", "b", "c"})

        def verify_after_increase(results):
            values = {row.value for row in results}
            self.assertEqual(values, {"d", "e"}, "Only new keys after repartition")

        def verify_after_decrease(results):
            values = {row.value for row in results}
            self.assertEqual(values, {"f", "g"}, "Only new keys after repartition")

        OfflineStateRepartitionTestUtils.run_repartition_test(
            spark=self.spark,
            num_shuffle_partitions=self.NUM_SHUFFLE_PARTITIONS,
            create_streaming_df=create_streaming_df,
            output_mode="append",
            batch1_data="a\nb\na\nc\n",  # unique: a, b, c
            batch2_data="a\nb\nd\ne\n",  # a, b duplicates; d, e new
            batch3_data="a\nc\nf\ng\n",  # a, c duplicates; f, g new
            verify_initial=verify_initial,
            verify_after_increase=verify_after_increase,
            verify_after_decrease=verify_after_decrease,
        )

    def _run_tws_repartition_test(self, is_pandas):
        """Helper method to run repartition test with a given processor factory method"""

        def create_streaming_df(df):
            # Parse text input format "id,temperature" into structured columns
            split_df = split(df["value"], ",")
            parsed_df = df.select(
                split_df.getItem(0).alias("id"),
                split_df.getItem(1).cast("integer").alias("temperature"),
            )

            output_schema = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("value", StringType(), True),
                ]
            )
            processor = (
                SimpleStatefulProcessorWithInitialStateFactory().pandas()
                if is_pandas
                else SimpleStatefulProcessorWithInitialStateFactory().row()
            )
            return (
                parsed_df.groupBy("id").transformWithStateInPandas(
                    statefulProcessor=processor,
                    outputStructType=output_schema,
                    outputMode="Update",
                    timeMode="None",
                    initialState=None,
                )
                if is_pandas
                else parsed_df.groupBy("id").transformWithState(
                    statefulProcessor=processor,
                    outputStructType=output_schema,
                    outputMode="Update",
                    timeMode="None",
                    initialState=None,
                )
            )

        def verify_initial(results):
            # SimpleStatefulProcessorWithInitialState accumulates temperature values
            values = {row.id: row.value for row in results}
            self.assertEqual(values.get("a"), "270", "a: 120 + 150 = 270")
            self.assertEqual(values.get("b"), "50", "b: 50")
            self.assertEqual(values.get("c"), "30", "c: 30")

        def verify_after_increase(results):
            # After repartition, state should be preserved and accumulated
            values = {row.id: row.value for row in results}
            self.assertEqual(values.get("a"), "371", "State for 'a': 270 + 101 = 371")
            self.assertEqual(values.get("b"), "152", "State for 'b': 50 + 102 = 152")
            self.assertEqual(values.get("d"), "103", "New key 'd' should have value 103")

        def verify_after_decrease(results):
            # After repartition, state should still be preserved
            values = {row.id: row.value for row in results}
            self.assertEqual(values.get("a"), "475", "State for 'a': 371 + 104 = 475")
            self.assertEqual(values.get("c"), "135", "State for 'c': 30 + 105 = 135")
            self.assertEqual(values.get("e"), "106", "New key 'e' should have value 106")

        OfflineStateRepartitionTestUtils.run_repartition_test(
            spark=self.spark,
            num_shuffle_partitions=self.NUM_SHUFFLE_PARTITIONS,
            create_streaming_df=create_streaming_df,
            output_mode="update",
            batch1_data="a,120\na,150\nb,50\nc,30\n",  # a:270, b:50, c:30
            batch2_data="a,101\nb,102\nd,103\n",  # a:371, b:152, d:103 (new)
            batch3_data="a,104\nc,105\ne,106\n",  # a:475, c:135, e:106 (new)
            verify_initial=verify_initial,
            verify_after_increase=verify_after_increase,
            verify_after_decrease=verify_after_decrease,
        )

    @unittest.skipIf(
        not have_pyarrow or os.environ.get("PYTHON_GIL", "?") == "0",
        pyarrow_requirement_message or "Not supported in no-GIL mode",
    )
    def test_repartition_with_streaming_tws(self):
        """Test repartition for streaming transformWithState"""

        self._run_tws_repartition_test(is_pandas=False)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow or os.environ.get("PYTHON_GIL", "?") == "0",
        cast(
            str,
            pandas_requirement_message
            or pyarrow_requirement_message
            or "Not supported in no-GIL mode",
        ),
    )
    def test_repartition_with_streaming_tws_in_pandas(self):
        """Test repartition for streaming transformWithStateInPandas."""

        self._run_tws_repartition_test(is_pandas=True)

    def _run_repartition_with_streaming_tws_multiple_state_vars_test(self, is_pandas):
        """Test repartition with processor using multiple state variable types (value + list + map)."""

        def create_streaming_df(df):
            # Parse text input format "id,temperature" into structured columns
            split_df = split(df["value"], ",")
            parsed_df = df.select(
                split_df.getItem(0).alias("id"),
                split_df.getItem(1).cast("integer").alias("temperature"),
            )

            output_schema = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("value_arr", StringType(), True),
                    StructField("list_state_arr", StringType(), True),
                    StructField("map_state_arr", StringType(), True),
                    StructField("nested_map_state_arr", StringType(), True),
                ]
            )
            processor = (
                StatefulProcessorCompositeTypeFactory().pandas()
                if is_pandas
                else StatefulProcessorCompositeTypeFactory().row()
            )
            return (
                parsed_df.groupBy("id").transformWithStateInPandas(
                    statefulProcessor=processor,
                    outputStructType=output_schema,
                    outputMode="Update",
                    timeMode="None",
                    initialState=None,
                )
                if is_pandas
                else parsed_df.groupBy("id").transformWithState(
                    statefulProcessor=processor,
                    outputStructType=output_schema,
                    outputMode="Update",
                    timeMode="None",
                    initialState=None,
                )
            )

        def verify_initial(results):
            rows = {row.id: row for row in results}
            # StatefulProcessorCompositeType initializes to "0" on first batch
            # (this is how the processor is designed - see line 1853 in helper file)
            self.assertEqual(rows["a"].value_arr, "0")
            self.assertEqual(rows["a"].list_state_arr, "0")
            # Map state initialized with default ATTRIBUTES_MAP and CONFS_MAP
            self.assertEqual(rows["a"].map_state_arr, '{"key1": [1], "key2": [10]}')
            self.assertEqual(rows["a"].nested_map_state_arr, '{"e1": {"e2": 5, "e3": 10}}')

        def verify_after_increase(results):
            # After repartition, state should be preserved
            rows = {row.id: row for row in results}
            # Key 'a': value state accumulated
            self.assertEqual(rows["a"].value_arr, "100")
            self.assertEqual(rows["a"].list_state_arr, "0,100")
            # Map state updated with key "a" and temperature 100
            self.assertEqual(rows["a"].map_state_arr, '{"a": [100], "key1": [1], "key2": [10]}')
            self.assertEqual(
                rows["a"].nested_map_state_arr, '{"e1": {"a": 100, "e2": 5, "e3": 10}}'
            )
            # New key 'd' - first batch for this key, outputs "0"
            self.assertEqual(rows["d"].value_arr, "0")
            self.assertEqual(rows["d"].list_state_arr, "0")
            # Map state for 'd' initialized with defaults
            self.assertEqual(rows["d"].map_state_arr, '{"key1": [1], "key2": [10]}')
            self.assertEqual(rows["d"].nested_map_state_arr, '{"e1": {"e2": 5, "e3": 10}}')

        def verify_after_decrease(results):
            # After another repartition, state should still be preserved
            rows = {row.id: row for row in results}
            # Key 'd' gets 102, state was [0], so accumulated: [0+102] = [102]
            self.assertEqual(rows["d"].value_arr, "102")
            self.assertEqual(rows["d"].list_state_arr, "0,102")
            # Map state for 'd' updated with temperature 102
            self.assertEqual(rows["d"].map_state_arr, '{"d": [102], "key1": [1], "key2": [10]}')
            self.assertEqual(
                rows["d"].nested_map_state_arr, '{"e1": {"d": 102, "e2": 5, "e3": 10}}'
            )
            # Key 'a' gets 101, state was [100], so accumulated: [100+101] = [201]
            self.assertEqual(rows["a"].value_arr, "201")
            self.assertEqual(rows["a"].list_state_arr, "0,100,101")
            # Map state for 'a' updated with temperature 101 (replaces previous value)
            self.assertEqual(rows["a"].map_state_arr, '{"a": [101], "key1": [1], "key2": [10]}')
            self.assertEqual(
                rows["a"].nested_map_state_arr, '{"e1": {"a": 101, "e2": 5, "e3": 10}}'
            )

        OfflineStateRepartitionTestUtils.run_repartition_test(
            spark=self.spark,
            num_shuffle_partitions=self.NUM_SHUFFLE_PARTITIONS,
            create_streaming_df=create_streaming_df,
            output_mode="update",
            batch1_data="a,100\n",  # a:0
            batch2_data="a,100\nd,100\n",  # a:100, d:0 (new)
            batch3_data="d,102\na,101\n",  # d:102, a:201 (new)
            verify_initial=verify_initial,
            verify_after_increase=verify_after_increase,
            verify_after_decrease=verify_after_decrease,
        )

    @unittest.skipIf(
        not have_pyarrow or os.environ.get("PYTHON_GIL", "?") == "0",
        pyarrow_requirement_message or "Not supported in no-GIL mode",
    )
    def test_repartition_with_streaming_tws_multiple_state_vars(self):
        """Test repartition with transformWithState using multiple state variable types (value + list + map)."""

        self._run_repartition_with_streaming_tws_multiple_state_vars_test(is_pandas=False)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow or os.environ.get("PYTHON_GIL", "?") == "0",
        cast(
            str,
            pandas_requirement_message
            or pyarrow_requirement_message
            or "Not supported in no-GIL mode",
        ),
    )
    def test_repartition_with_streaming_tws_in_pandas_multiple_state_vars(self):
        """Test repartition with transformWithStateInPandas using multiple state variable types (value + list + map)."""

        self._run_repartition_with_streaming_tws_multiple_state_vars_test(is_pandas=True)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
