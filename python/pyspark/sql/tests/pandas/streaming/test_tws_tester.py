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

import random
import tempfile
import unittest

import pandas as pd
import pandas.testing as pdt

from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql.functions import split
from pyspark.sql.streaming import StatefulProcessor
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.streaming.tws_tester import TwsTester
from pyspark.sql.tests.pandas.helper.helper_pandas_transform_with_state import (
    AllMethodsTestProcessorFactory,
    RunningCountStatefulProcessorFactory,
    TopKProcessorFactory,
    WordFrequencyProcessorFactory,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    Row,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


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

        tester.test(
            [
                Row(key="user1", word="hello"),
                Row(key="user1", word="goodbye"),
                Row(key="user2", word="spark"),
                Row(key="user3", word="new"),
            ]
        )

        self.assertEqual(
            tester.peekMapState("frequencies", "user1"),
            {("hello",): (6,), ("world",): (3,), ("goodbye",): (1,)},
        )
        self.assertEqual(
            tester.peekMapState("frequencies", "user2"), {("spark",): (11,)}
        )
        self.assertEqual(tester.peekMapState("frequencies", "user3"), {("new",): (1,)})
        self.assertEqual(tester.peekMapState("frequencies", "user4"), {})

    # Example of how TwsTester can be used to test step function.
    def test_step_function(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)

        # Example of helper function using TwsTester to inspect how processing a single row changes
        # state.
        def step_function(key: str, input_row: str, state_in: int) -> int:
            tester.setValueState("count", key, (state_in,))
            tester.test([Row(key=key, value=input_row)])
            return tester.peekValueState("count", key)[0]

        self.assertEqual(step_function("key1", "a", 10), 11)

    # Example of how TwsTester can be used to simulate real-time mode.
    def test_row_by_row(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)

        # Example of helper function to test how TransformWithState processes rows one-by-one,
        # which can be used to simulate real-time mode.
        def test_row_by_row_helper(input_rows: list[Row]) -> list[Row]:
            result: list[Row] = []
            for row in input_rows:
                result += tester.test([row])
            return result

        ans = test_row_by_row_helper(
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
            ans,
            [
                Row(key="key1", count=1),
                Row(key="key2", count=1),
                Row(key="key1", count=2),
                Row(key="key2", count=2),
                Row(key="key1", count=3),
                Row(key="key1", count=4),
                Row(key="key3", count=1),
            ],
        )

    # Tests that TwsTester calls handleInitialState.
    def test_initial_state_row(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(
            processor,
            initialStateRow=[
                Row(key="a", initial_count=10),
                Row(key="b", initial_count=20),
            ],
        )
        self.assertEqual(tester.peekValueState("count", "a"), (10,))
        self.assertEqual(tester.peekValueState("count", "b"), (20,))

        ans = tester.test([Row(key="a", value="a"), Row(key="c", value="c")])
        self.assertEqual(ans, [Row(key="a", count=11), Row(key="c", count=1)])

    def test_initial_state_pandas(self):
        processor = RunningCountStatefulProcessorFactory().pandas()
        tester = TwsTester(
            processor,
            initialStatePandas=pd.DataFrame(
                {"key": ["a", "b"], "initial_count": [10, 20]}
            ),
        )
        self.assertEqual(tester.peekValueState("count", "a"), (10,))
        self.assertEqual(tester.peekValueState("count", "b"), (20,))

        ans = tester.testInPandas(
            pd.DataFrame({"key": ["a", "c"], "value": ["a", "c"]})
        )
        expected = pd.DataFrame({"key": ["a", "c"], "count": [11, 1]})
        pdt.assert_frame_equal(ans, expected, check_like=True)

    def test_all_methods_processor(self):
        """Test that TwsTester exercises all state methods."""
        processor = AllMethodsTestProcessorFactory().row()
        tester = TwsTester(processor)

        results = tester.test(
            [
                Row(key="k", cmd="value-exists"),  # false
                Row(key="k", cmd="value-set"),  # set to 42
                Row(key="k", cmd="value-exists"),  # true
                Row(key="k", cmd="value-clear"),  # clear
                Row(key="k", cmd="value-exists"),  # false again
                Row(key="k", cmd="list-exists"),  # false
                Row(key="k", cmd="list-append"),  # append a, b
                Row(key="k", cmd="list-exists"),  # true
                Row(key="k", cmd="list-append-array"),  # append c, d
                Row(key="k", cmd="list-get"),  # a,b,c,d
                Row(key="k", cmd="map-exists"),  # false
                Row(key="k", cmd="map-add"),  # add x=1, y=2, z=3
                Row(key="k", cmd="map-exists"),  # true
                Row(key="k", cmd="map-keys"),  # x,y,z
                Row(key="k", cmd="map-values"),  # 1,2,3
                Row(key="k", cmd="map-iterator"),  # x=1,y=2,z=3
                Row(key="k", cmd="map-remove"),  # remove y
                Row(key="k", cmd="map-keys"),  # x,z
                Row(key="k", cmd="map-clear"),  # clear map
                Row(key="k", cmd="map-exists"),  # false
            ]
        )

        self.assertEqual(
            results,
            [
                Row(key="k", result="value-exists:False"),
                Row(key="k", result="value-set:done"),
                Row(key="k", result="value-exists:True"),
                Row(key="k", result="value-clear:done"),
                Row(key="k", result="value-exists:False"),
                Row(key="k", result="list-exists:False"),
                Row(key="k", result="list-append:done"),
                Row(key="k", result="list-exists:True"),
                Row(key="k", result="list-append-array:done"),
                Row(key="k", result="list-get:a,b,c,d"),
                Row(key="k", result="map-exists:False"),
                Row(key="k", result="map-add:done"),
                Row(key="k", result="map-exists:True"),
                Row(key="k", result="map-keys:x,y,z"),
                Row(key="k", result="map-values:1,2,3"),
                Row(key="k", result="map-iterator:x=1,y=2,z=3"),
                Row(key="k", result="map-remove:done"),
                Row(key="k", result="map-keys:x,z"),
                Row(key="k", result="map-clear:done"),
                Row(key="k", result="map-exists:False"),
            ],
        )

    # This test shows that key column in input data can have arbitrary name.
    def test_key_column_name(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor, key_column_name="id")

        ans1 = tester.test([Row(id="key1", value="a"), Row(id="key1", value="b")])
        self.assertEqual(ans1, [Row(key="key1", count=2)])


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
    # Output rows are checked ignoring order.
    # Correct only if results do not depend on reordering rows within batch.
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
        def _write_batch(batch_id: int, batch: list[tuple]):
            with open(input_path + f"/test{batch_id}.txt", "w") as f:
                for row in batch:
                    assert len(row) == num_input_columns
                    f.write(",".join(str(value) for value in row) + "\n")

        try:
            for batch_id, batch in enumerate(batches):
                _write_batch(batch_id, batch)
                query.processAllAvailable()
            query.awaitTermination(1)
            self.assertTrue(query.exception() is None)
        finally:
            query.stop()

        # Assert actual results are equal to expected results.
        self.assertEqual(len(actual_results), num_batches)
        self.assertEqual(len(expected_results), num_batches)
        for batch_id in range(num_batches):
            self.assertCountEqual(actual_results[batch_id], expected_results[batch_id])

    def test_fuzz_running_count(self):
        output_schema = StructType(
            [
                StructField("key", StringType(), True),
                StructField("count", IntegerType(), True),
            ]
        )
        batches: list[list[tuple[str, str]]] = []
        for batch_id in range(5):
            batch: list[tuple[str, str]] = []
            for _ in range(200):
                key: str = f"key{random.randint(0, 9)}"
                value: str = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5))
                batch.append((key, value))
            batches.append(batch)
        input_columns: list[str] = ["key", "value"]
        input_types: list[str] = ["string", "string"]

        for use_pandas in [False, True]:
            self._check_tws_tester(
                RunningCountStatefulProcessorFactory().get(use_pandas=use_pandas),
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
        batches: list[list[tuple[str, float]]] = []
        for batch_id in range(5):
            batch: list[tuple[str, float]] = []
            for _ in range(200):
                key: str = f"key{random.randint(0, 9)}"
                score: float = random.randint(0, 1000000) / 1000
                batch.append((key, score))
            batches.append(batch)
        input_columns: list[str] = ["key", "score"]
        input_types: list[str] = ["string", "double"]

        for use_pandas in [False, True]:
            self._check_tws_tester(
                TopKProcessorFactory(k=10).get(use_pandas=use_pandas),
                batches,
                input_columns,
                input_types,
                output_schema,
                use_pandas=use_pandas,
            )

    def test_fuzz_word_frequency(self):
        output_schema = StructType(
            [
                StructField("key", StringType(), True),
                StructField("word", StringType(), True),
                StructField("count", IntegerType(), True),
            ]
        )
        batches: list[list[tuple[str, str]]] = []
        for batch_id in range(5):
            batch: list[tuple[str, str]] = []
            for _ in range(200):
                key = f"key{random.randint(0, 9)}"
                word = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=1))
                batch.append((key, word))
            batches.append(batch)
        input_columns: list[str] = ["key", "word"]
        input_types: list[str] = ["string", "string"]

        for use_pandas in [False, True]:
            self._check_tws_tester(
                WordFrequencyProcessorFactory().get(use_pandas=use_pandas),
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
