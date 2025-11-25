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

from pyspark.sql.streaming.query import StreamingQuery
import random
import unittest
import tempfile
import sys
import os
from typing import Iterator, List, Tuple, Any

import pandas as pd
import pandas.testing as pdt

from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql.functions import split
from pyspark.sql.streaming import StatefulProcessor
from pyspark.sql.streaming.tws_tester import TwsTester
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    Row,
    DoubleType,
)
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

from pyspark.sql.tests.pandas.helper.helper_pandas_transform_with_state import (
    TopKProcessorFactory,
    RunningCountStatefulProcessorFactory,
    StatefulProcessorFactory,
    WordFrequencyProcessorFactory,
)


def LOG(s):
    with open("/tmp/log.txt", "a") as f:
        f.write(s + "\n")


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message or "",
)
class TwsTesterTests(ReusedSQLTestCase):
    def test_running_count_processor(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)

        ans1 = tester.test(
            [
                Row(key="key1", value="a"),
                Row(key="key2", value="b"),
                Row(key="key1", value="c"),
                Row(key="key2", value="b"),
                Row(key="key1", value="c"),
                Row(key="key1", value="c"),
                Row(key="key3", value="q"),
            ]
        )
        self.assertEqual(
            ans1,
            [
                Row(key="key1", count=4),
                Row(key="key2", count=2),
                Row(key="key3", count=1),
            ],
        )

        ans2 = tester.test(
            [
                Row(key="key1", value="q"),
            ]
        )
        self.assertEqual(ans2, [Row(key="key1", count=5)])

    def test_running_count_processor_pandas(self):
        processor = RunningCountStatefulProcessorFactory().pandas()
        tester = TwsTester(processor)

        input_df1 = pd.DataFrame(
            {
                "key": ["key1", "key2", "key1", "key2", "key1", "key1", "key3"],
                "value": ["a", "b", "c", "b", "c", "c", "q"],
            }
        )
        ans1 = tester.testInPandas(input_df1)
        expected1 = pd.DataFrame({"key": ["key1", "key2", "key3"], "count": [4, 2, 1]})
        pdt.assert_frame_equal(ans1, expected1, check_like=True)

        input_df2 = pd.DataFrame({"key": ["key1"], "value": ["q"]})
        ans2 = tester.testInPandas(input_df2)
        expected2 = pd.DataFrame({"key": ["key1"], "count": [5]})
        pdt.assert_frame_equal(ans2, expected2, check_like=True)

    def test_direct_access_to_value_state(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        tester.setValueState("count", "foo", (5,))
        tester.test([Row(key="foo", value="q")])
        self.assertEqual(tester.peekValueState("count", "foo"), (6,))

    def test_topk_processor(self):
        processor = TopKProcessorFactory(k=2).row()
        tester = TwsTester(processor)

        ans1 = tester.test(
            [
                Row(key="key2", score=30.0),
                Row(key="key2", score=40.0),
                Row(key="key1", score=2.0),
                Row(key="key1", score=3.0),
                Row(key="key2", score=10.0),
                Row(key="key2", score=20.0),
                Row(key="key3", score=100.0),
                Row(key="key1", score=1.0),
            ]
        )
        assert ans1 == [
            Row(key="key1", score=3.0),
            Row(key="key1", score=2.0),
            Row(key="key2", score=40.0),
            Row(key="key2", score=30.0),
            Row(key="key3", score=100.0),
        ]

        ans2 = tester.test([Row(key="key1", score=10.0)])
        self.assertEqual(
            ans2, [Row(key="key1", score=10.0), Row(key="key1", score=3.0)]
        )

    def test_topk_processor_pandas(self):
        processor = TopKProcessorFactory(k=2).pandas()
        tester = TwsTester(processor)

        input_df1 = pd.DataFrame(
            {
                "key": ["key2", "key2", "key1", "key1", "key2", "key2", "key3", "key1"],
                "score": [30.0, 40.0, 2.0, 3.0, 10.0, 20.0, 100.0, 1.0],
            }
        )
        ans1 = tester.testInPandas(input_df1)
        expected1 = pd.DataFrame(
            {
                "key": ["key1", "key1", "key2", "key2", "key3"],
                "score": [3.0, 2.0, 40.0, 30.0, 100.0],
            }
        )
        pdt.assert_frame_equal(ans1, expected1, check_like=True)

        input_df2 = pd.DataFrame({"key": ["key1"], "score": [10.0]})
        ans2 = tester.testInPandas(input_df2)
        expected2 = pd.DataFrame({"key": ["key1", "key1"], "score": [10.0, 3.0]})
        pdt.assert_frame_equal(ans2, expected2, check_like=True)

    def test_direct_access_to_list_state(self):
        processor = TopKProcessorFactory(k=2).row()
        tester = TwsTester(processor)

        tester.setListState("topK", "a", [(6.0,), (5.0,)])
        tester.setListState("topK", "b", [(8.0,), (7.0,)])
        tester.test(
            [
                Row(key="a", score=10.0),
                Row(key="b", score=7.5),
                Row(key="c", score=1.0),
            ]
        )

        assert tester.peekListState("topK", "a") == [(10.0,), (6.0,)]
        assert tester.peekListState("topK", "b") == [(8.0,), (7.5,)]
        assert tester.peekListState("topK", "c") == [(1.0,)]
        assert tester.peekListState("topK", "d") == []

    # TODO: Add test for key column name different than key.

    def test_word_frequency_processor(self):
        processor = WordFrequencyProcessorFactory().row()
        tester = TwsTester(processor)

        ans1 = tester.test(
            [
                Row(key="user1", word="hello"),
                Row(key="user1", word="world"),
                Row(key="user1", word="hello"),
                Row(key="user2", word="hello"),
                Row(key="user2", word="spark"),
                Row(key="user1", word="world"),
            ]
        )
        self.assertEqual(
            ans1,
            [
                Row(key="user1", word="hello", count=1),
                Row(key="user1", word="world", count=1),
                Row(key="user1", word="hello", count=2),
                Row(key="user1", word="world", count=2),
                Row(key="user2", word="hello", count=1),
                Row(key="user2", word="spark", count=1),
            ],
        )

        ans2 = tester.test(
            [
                Row(key="user1", word="hello"),
                Row(key="user1", word="test"),
            ]
        )
        self.assertEqual(
            ans2,
            [
                Row(key="user1", word="hello", count=3),
                Row(key="user1", word="test", count=1),
            ],
        )

    def test_word_frequency_processor_pandas(self):
        processor = WordFrequencyProcessorFactory().pandas()
        tester = TwsTester(processor)

        input_df1 = pd.DataFrame(
            {
                "key": ["user1", "user1", "user1", "user2", "user2", "user1"],
                "word": ["hello", "world", "hello", "hello", "spark", "world"],
            }
        )
        ans1 = tester.testInPandas(input_df1)
        expected1 = pd.DataFrame(
            {
                "key": ["user1", "user1", "user1", "user1", "user2", "user2"],
                "word": ["hello", "world", "hello", "world", "hello", "spark"],
                "count": [1, 1, 2, 2, 1, 1],
            }
        )
        pdt.assert_frame_equal(ans1, expected1, check_like=True)

        input_df2 = pd.DataFrame({"key": ["user1", "user1"], "word": ["hello", "test"]})
        ans2 = tester.testInPandas(input_df2)
        expected2 = pd.DataFrame(
            {
                "key": ["user1", "user1"],
                "word": ["hello", "test"],
                "count": [3, 1],
            }
        )
        pdt.assert_frame_equal(ans2, expected2, check_like=True)

    def test_direct_access_to_map_state(self):
        processor = WordFrequencyProcessorFactory().row()
        tester = TwsTester(processor)

        tester.setMapState("frequencies", "user1", {("hello",): (5,), ("world",): (3,)})
        tester.setMapState("frequencies", "user2", {("spark",): (10,)})

        tester.test([
            Row(key="user1", word="hello"),
            Row(key="user1", word="goodbye"),
            Row(key="user2", word="spark"),
            Row(key="user3", word="new"),
        ])

        self.assertEqual(
            tester.peekMapState("frequencies", "user1"),
            {("hello",): (6,), ("world",): (3,), ("goodbye",): (1,)},
        )
        self.assertEqual(tester.peekMapState("frequencies", "user2"), {("spark",): (11,)})
        self.assertEqual(tester.peekMapState("frequencies", "user3"), {("new",): (1,)})
        self.assertEqual(tester.peekMapState("frequencies", "user4"), {})


def _get_processor(factory: StatefulProcessorFactory, use_pandas: bool = False):
    return factory.pandas() if use_pandas else factory.row()


@unittest.skip("a")
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message or "",
)
class TwsTesterFuzzTests(ReusedSQLTestCase):
    @classmethod
    def conf(cls):
        cfg = SparkConf()
        cfg.set("spark.sql.shuffle.partitions", "1")
        cfg.set(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        return cfg

    # Helper that runs real streaming query with TWS and compares results with TwsTester output.
    # Supports both row mode (use_pandas=False) and Pandas mode (use_pandas=True).
    # Supports multiple bacthes.
    # Assumes that the first column is the key column.
    def _check_tws_tester(
        self,
        processor: StatefulProcessor,
        batches: list[list[tuple]],
        input_columns: list[str],
        input_types: list[str],
        output_schema: StructType,
        use_pandas: bool = False,
    ):
        num_batches = len(batches)
        key_column: str = input_columns[0]  # Assume first column is key column.

        num_input_columns = len(input_columns)
        assert len(input_types) == num_input_columns

        # Use TwsTester to compute expected results.
        tester = TwsTester(processor, key_column_name=key_column)

        def _run_tester(batch: list[tuple]) -> list[Row]:
            if use_pandas:
                input_to_tester = pd.DataFrame(batch, columns=input_columns)
                return self.spark.createDataFrame(
                    tester.testInPandas(input_to_tester)
                ).collect()
            else:
                input_to_tester: list[Row] = [
                    Row(**dict(zip(input_columns, r))) for r in batch
                ]
                return tester.test(input_to_tester)

        expected_results: list[list[Row]] = [_run_tester(batch) for batch in batches]

        # Create streaming DataFrame, it will read input from files.
        input_path = tempfile.mkdtemp()
        df = self.spark.readStream.format("text").load(input_path)
        df_split = df.withColumn("split_values", split(df["value"], ","))
        df_final = df_split.select(
            *[
                df_split.split_values.getItem(i).alias(col_name).cast(input_types[i])
                for i, col_name in enumerate(input_columns)
            ]
        )
        self.assertTrue(df_final.isStreaming)

        # Apply TransformWithState.
        df_grouped = df_final.groupBy(key_column)
        if use_pandas:
            tws_df = df_grouped.transformWithStateInPandas(
                statefulProcessor=processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
            )
        else:
            tws_df = df_grouped.transformWithState(
                statefulProcessor=processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
            )

        # Start streaming query collecting TWS results.
        actual_results: list[list[Row]] = []

        def _collect_results(batch_df: DataFrame, batch_id):
            rows: list[Row] = batch_df.collect()
            actual_results.append(rows)

        query: StreamingQuery = (
            tws_df.writeStream.queryName("test_tmp_query")
            .foreachBatch(_collect_results)
            .outputMode("update")
            .start()
        )

        # Process each input batch.
        try:
            for batch_id, batch in enumerate(batches):
                with open(input_path + f"/test{batch_id}.txt", "w") as f:
                    for row in batch:
                        assert len(row) == num_input_columns
                        f.write(",".join(str(value) for value in row) + "\n")
                query.processAllAvailable()
            query.awaitTermination(1)
            self.assertTrue(query.exception() is None)
        finally:
            query.stop()

        # Assert actual results are equal to expected results.
        LOG(f"actual_results={actual_results}")
        LOG(f"expected_results={expected_results}")

        self.assertEqual(len(actual_results), num_batches)
        self.assertEqual(len(expected_results), num_batches)
        for batch_id in range(num_batches):
            self.assertEqual(actual_results[batch_id], expected_results[batch_id])

    # @unittest.skip("a")
    def test_fuzz_running_count(self):
        output_schema = StructType(
            [
                StructField("key", StringType(), True),
                StructField("count", IntegerType(), True),
            ]
        )

        input_data: list[tuple[str, str]] = []
        for _ in range(1000):
            key = f"key{random.randint(0, 9)}"
            value = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5))
            input_data.append((key, value))
        input_columns: list[str] = ["key", "value"]
        input_types: list[str] = ["string", "string"]
        batches = [input_data, input_data]

        for use_pandas in [False, True]:
            LOG(f"**** use_pandas={use_pandas}")
            processor = _get_processor(
                RunningCountStatefulProcessorFactory(), use_pandas=use_pandas
            )
            self._check_tws_tester(
                processor,
                batches,
                input_columns,
                input_types,
                output_schema,
                use_pandas=use_pandas,
            )

    def test_fuzz_topk(self):
        output_schema = StructType(
            [
                StructField("key", StringType(), True),
                StructField("score", DoubleType(), True),
            ]
        )

        input_data: list[tuple[str, float]] = []
        for _ in range(1000):
            key: str = f"key{random.randint(0, 9)}"
            score: float = random.randint(0, 1000000) / 1000
            input_data.append((key, score))
        input_columns: list[str] = ["key", "score"]
        input_types: list[str] = ["string", "double"]
        batches = [input_data]

        for use_pandas in [False, True]:
            LOG(f"**** use_pandas={use_pandas}")
            processor = _get_processor(
                TopKProcessorFactory(k=10), use_pandas=use_pandas
            )
            self._check_tws_tester(
                processor,
                batches,
                input_columns,
                input_types,
                output_schema,
                use_pandas=use_pandas,
            )


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.streaming.test_tws_tester import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
