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
from pyspark.sql.streaming import StatefulProcessor, TwsTester
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.tests.pandas.helper.helper_pandas_transform_with_state import (
    AllMethodsTestProcessorFactory,
    EventTimeCountProcessorFactory,
    EventTimeSessionProcessorFactory,
    RunningCountStatefulProcessorFactory,
    SessionTimeoutProcessorFactory,
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
from pyspark.errors import PySparkValueError, PySparkAssertionError
from pyspark.errors.exceptions.base import IllegalArgumentException
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

        self.assertEqual(tester.test("key1", [Row(value="a")]), [Row(key="key1", count=1)])
        self.assertEqual(
            tester.test("key2", [Row(value="a"), Row(value="a")]),
            [Row(key="key2", count=2)],
        )
        self.assertEqual(tester.test("key3", [Row(value="a")]), [Row(key="key3", count=1)])
        self.assertEqual(
            tester.test("key1", [Row(value="a"), Row(value="a"), Row(value="a")]),
            [Row(key="key1", count=4)],
        )

        self.assertEqual(tester.peekValueState("count", "key1"), (4,))
        self.assertEqual(tester.peekValueState("count", "key2"), (2,))
        self.assertEqual(tester.peekValueState("count", "key3"), (1,))
        self.assertIsNone(tester.peekValueState("count", "key4"))

    def test_running_count_processor_pandas(self):
        processor = RunningCountStatefulProcessorFactory().pandas()
        tester = TwsTester(processor)

        ans1 = tester.testInPandas("key1", pd.DataFrame({"value": ["a"]}))
        expected1 = pd.DataFrame({"key": ["key1"], "count": [1]})
        pdt.assert_frame_equal(ans1, expected1, check_like=True)

        ans2 = tester.testInPandas("key2", pd.DataFrame({"value": ["a", "a"]}))
        expected2 = pd.DataFrame({"key": ["key2"], "count": [2]})
        pdt.assert_frame_equal(ans2, expected2, check_like=True)

        ans3 = tester.testInPandas("key3", pd.DataFrame({"value": ["a"]}))
        expected3 = pd.DataFrame({"key": ["key3"], "count": [1]})
        pdt.assert_frame_equal(ans3, expected3, check_like=True)

        ans4 = tester.testInPandas("key1", pd.DataFrame({"value": ["a", "a", "a"]}))
        expected4 = pd.DataFrame({"key": ["key1"], "count": [4]})
        pdt.assert_frame_equal(ans4, expected4, check_like=True)

    def test_direct_access_to_value_state(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        tester.updateValueState("count", "foo", (5,))
        tester.test("foo", [Row(value="q")])
        self.assertEqual(tester.peekValueState("count", "foo"), (6,))

    def test_topk_processor(self):
        processor = TopKProcessorFactory(k=2).row()
        tester = TwsTester(processor)

        ans1 = tester.test("key1", [Row(score=2.0), Row(score=3.0), Row(score=1.0)])
        self.assertEqual(ans1, [Row(key="key1", score=3.0), Row(key="key1", score=2.0)])

        ans2 = tester.test(
            "key2", [Row(score=10.0), Row(score=20.0), Row(score=30.0), Row(score=40.0)]
        )
        self.assertEqual(ans2, [Row(key="key2", score=40.0), Row(key="key2", score=30.0)])

        ans3 = tester.test("key3", [Row(score=100.0)])
        self.assertEqual(ans3, [Row(key="key3", score=100.0)])

        self.assertEqual(tester.peekListState("topK", "key1"), [(3.0,), (2.0,)])
        self.assertEqual(tester.peekListState("topK", "key2"), [(40.0,), (30.0,)])
        self.assertEqual(tester.peekListState("topK", "key3"), [(100.0,)])
        self.assertEqual(tester.peekListState("topK", "key4"), [])

        ans4 = tester.test("key1", [Row(score=10.0)])
        self.assertEqual(ans4, [Row(key="key1", score=10.0), Row(key="key1", score=3.0)])
        self.assertEqual(tester.peekListState("topK", "key1"), [(10.0,), (3.0,)])

    def test_topk_processor_pandas(self):
        processor = TopKProcessorFactory(k=2).pandas()
        tester = TwsTester(processor)

        ans1 = tester.testInPandas("key1", pd.DataFrame({"score": [2.0, 3.0, 1.0]}))
        expected1 = pd.DataFrame({"key": ["key1", "key1"], "score": [3.0, 2.0]})
        pdt.assert_frame_equal(ans1, expected1, check_like=True)

        ans2 = tester.testInPandas("key2", pd.DataFrame({"score": [10.0, 20.0, 30.0, 40.0]}))
        expected2 = pd.DataFrame({"key": ["key2", "key2"], "score": [40.0, 30.0]})
        pdt.assert_frame_equal(ans2, expected2, check_like=True)

        ans3 = tester.testInPandas("key3", pd.DataFrame({"score": [100.0]}))
        expected3 = pd.DataFrame({"key": ["key3"], "score": [100.0]})
        pdt.assert_frame_equal(ans3, expected3, check_like=True)

        ans4 = tester.testInPandas("key1", pd.DataFrame({"score": [10.0]}))
        expected4 = pd.DataFrame({"key": ["key1", "key1"], "score": [10.0, 3.0]})
        pdt.assert_frame_equal(ans4, expected4, check_like=True)

    def test_direct_access_to_list_state(self):
        processor = TopKProcessorFactory(k=2).row()
        tester = TwsTester(processor)

        tester.updateListState("topK", "a", [(6.0,), (5.0,)])
        tester.updateListState("topK", "b", [(8.0,), (7.0,)])
        tester.test("a", [Row(score=10.0)])
        tester.test("b", [Row(score=7.5)])
        tester.test("c", [Row(score=1.0)])

        assert tester.peekListState("topK", "a") == [(10.0,), (6.0,)]
        assert tester.peekListState("topK", "b") == [(8.0,), (7.5,)]
        assert tester.peekListState("topK", "c") == [(1.0,)]
        assert tester.peekListState("topK", "d") == []

    def test_word_frequency_processor(self):
        processor = WordFrequencyProcessorFactory().row()
        tester = TwsTester(processor)

        ans1 = tester.test(
            "user1",
            [
                Row(word="hello"),
                Row(word="world"),
                Row(word="hello"),
                Row(word="world"),
            ],
        )
        self.assertEqual(
            ans1,
            [
                Row(key="user1", word="hello", count=1),
                Row(key="user1", word="world", count=1),
                Row(key="user1", word="hello", count=2),
                Row(key="user1", word="world", count=2),
            ],
        )

        ans2 = tester.test("user2", [Row(word="hello"), Row(word="spark")])
        self.assertEqual(
            ans2,
            [
                Row(key="user2", word="hello", count=1),
                Row(key="user2", word="spark", count=1),
            ],
        )

        # Check state using peekMapState.
        self.assertEqual(
            tester.peekMapState("frequencies", "user1"),
            {("hello",): (2,), ("world",): (2,)},
        )
        self.assertEqual(
            tester.peekMapState("frequencies", "user2"),
            {("hello",): (1,), ("spark",): (1,)},
        )
        self.assertEqual(tester.peekMapState("frequencies", "user3"), {})

        # Process more data for user1.
        ans3 = tester.test("user1", [Row(word="hello"), Row(word="test")])
        self.assertEqual(
            ans3,
            [
                Row(key="user1", word="hello", count=3),
                Row(key="user1", word="test", count=1),
            ],
        )
        self.assertEqual(
            tester.peekMapState("frequencies", "user1"),
            {("hello",): (3,), ("world",): (2,), ("test",): (1,)},
        )

    def test_word_frequency_processor_pandas(self):
        processor = WordFrequencyProcessorFactory().pandas()
        tester = TwsTester(processor)

        input_df1 = pd.DataFrame({"word": ["hello", "world", "hello", "world"]})
        ans1 = tester.testInPandas("user1", input_df1)
        expected1 = pd.DataFrame(
            {
                "key": ["user1", "user1", "user1", "user1"],
                "word": ["hello", "world", "hello", "world"],
                "count": [1, 1, 2, 2],
            }
        )
        pdt.assert_frame_equal(ans1, expected1, check_like=True)

        input_df2 = pd.DataFrame({"word": ["hello", "spark"]})
        ans2 = tester.testInPandas("user2", input_df2)
        expected2 = pd.DataFrame(
            {
                "key": ["user2", "user2"],
                "word": ["hello", "spark"],
                "count": [1, 1],
            }
        )
        pdt.assert_frame_equal(ans2, expected2, check_like=True)

        input_df3 = pd.DataFrame({"word": ["hello", "test"]})
        ans3 = tester.testInPandas("user1", input_df3)
        expected3 = pd.DataFrame(
            {
                "key": ["user1", "user1"],
                "word": ["hello", "test"],
                "count": [3, 1],
            }
        )
        pdt.assert_frame_equal(ans3, expected3, check_like=True)

    def test_direct_access_to_map_state(self):
        processor = WordFrequencyProcessorFactory().row()
        tester = TwsTester(processor)

        tester.updateMapState("frequencies", "user1", {("hello",): (5,), ("world",): (3,)})
        tester.updateMapState("frequencies", "user2", {("spark",): (10,)})

        tester.test("user1", [Row(word="hello"), Row(word="goodbye")])
        tester.test("user2", [Row(word="spark")])
        tester.test("user3", [Row(word="new")])

        self.assertEqual(
            tester.peekMapState("frequencies", "user1"),
            {("hello",): (6,), ("world",): (3,), ("goodbye",): (1,)},
        )
        self.assertEqual(tester.peekMapState("frequencies", "user2"), {("spark",): (11,)})
        self.assertEqual(tester.peekMapState("frequencies", "user3"), {("new",): (1,)})
        self.assertEqual(tester.peekMapState("frequencies", "user4"), {})

    # Example of how TwsTester can be used to test step function.
    def test_step_function(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)

        # Example of helper function using TwsTester to inspect how processing a single row changes
        # state.
        def step_function(key: str, input_row: str, state_in: int) -> int:
            tester.updateValueState("count", key, (state_in,))
            tester.test(key, [Row(value=input_row)])
            return tester.peekValueState("count", key)[0]

        self.assertEqual(step_function("key1", "a", 10), 11)

    # Example of how TwsTester can be used to simulate real-time mode (row by row processing).
    def test_row_by_row(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)

        # Example of helper function to test how TransformWithState processes rows one-by-one,
        # which can be used to simulate real-time mode.
        def test_row_by_row_helper(input_rows: list[tuple[str, Row]]) -> list[Row]:
            result: list[Row] = []
            for key, row in input_rows:
                result += tester.test(key, [row])
            return result

        ans = test_row_by_row_helper(
            [
                ("key1", Row(value="a")),
                ("key2", Row(value="b")),
                ("key1", Row(value="c")),
                ("key2", Row(value="b")),
                ("key1", Row(value="c")),
                ("key1", Row(value="c")),
                ("key3", Row(value="q")),
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
                ("a", Row(initial_count=10)),
                ("b", Row(initial_count=20)),
            ],
        )
        self.assertEqual(tester.peekValueState("count", "a"), (10,))
        self.assertEqual(tester.peekValueState("count", "b"), (20,))

        ans1 = tester.test("a", [Row(value="a")])
        self.assertEqual(ans1, [Row(key="a", count=11)])

        ans2 = tester.test("c", [Row(value="c")])
        self.assertEqual(ans2, [Row(key="c", count=1)])

    def test_initial_state_pandas(self):
        processor = RunningCountStatefulProcessorFactory().pandas()
        tester = TwsTester(
            processor,
            initialStatePandas=[
                ("a", pd.DataFrame({"initial_count": [10]})),
                ("b", pd.DataFrame({"initial_count": [20]})),
            ],
        )
        self.assertEqual(tester.peekValueState("count", "a"), (10,))
        self.assertEqual(tester.peekValueState("count", "b"), (20,))

        ans1 = tester.testInPandas("a", pd.DataFrame({"value": ["a"]}))
        expected1 = pd.DataFrame({"key": ["a"], "count": [11]})
        pdt.assert_frame_equal(ans1, expected1, check_like=True)

        ans2 = tester.testInPandas("c", pd.DataFrame({"value": ["c"]}))
        expected2 = pd.DataFrame({"key": ["c"], "count": [1]})
        pdt.assert_frame_equal(ans2, expected2, check_like=True)

    def test_all_methods_processor(self):
        """Test that TwsTester exercises all state methods."""
        processor = AllMethodsTestProcessorFactory().row()
        tester = TwsTester(processor)

        results = tester.test(
            "k",
            [
                Row(cmd="value-exists"),  # false
                Row(cmd="value-set"),  # set to 42
                Row(cmd="value-exists"),  # true
                Row(cmd="value-clear"),  # clear
                Row(cmd="value-exists"),  # false again
                Row(cmd="list-exists"),  # false
                Row(cmd="list-append"),  # append a, b
                Row(cmd="list-exists"),  # true
                Row(cmd="list-append-array"),  # append c, d
                Row(cmd="list-get"),  # a,b,c,d
                Row(cmd="map-exists"),  # false
                Row(cmd="map-add"),  # add x=1, y=2, z=3
                Row(cmd="map-exists"),  # true
                Row(cmd="map-keys"),  # x,y,z
                Row(cmd="map-values"),  # 1,2,3
                Row(cmd="map-iterator"),  # x=1,y=2,z=3
                Row(cmd="map-remove"),  # remove y
                Row(cmd="map-keys"),  # x,z
                Row(cmd="map-clear"),  # clear map
                Row(cmd="map-exists"),  # false
            ],
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

    def test_both_initial_states_specified(self):
        processor = RunningCountStatefulProcessorFactory().row()
        with self.assertRaises(AssertionError):
            TwsTester(
                processor,
                initialStateRow=[("a", Row(initial_count=10))],
                initialStatePandas=[("a", pd.DataFrame({"initial_count": [10]}))],
            )

    def test_timer_registration_raises_error_in_none_mode(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor, timeMode="None")
        with self.assertRaisesRegex(PySparkValueError, "UNSUPPORTED_OPERATION"):
            tester.handle.registerTimer(12345)

    def test_delete_timer_raises_error_in_none_mode(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor, timeMode="None")
        with self.assertRaisesRegex(PySparkValueError, "UNSUPPORTED_OPERATION"):
            tester.handle.deleteTimer(12345)

    def test_list_timers_raises_error_in_none_mode(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor, timeMode="None")
        with self.assertRaisesRegex(PySparkValueError, "UNSUPPORTED_OPERATION"):
            list(tester.handle.listTimers())

    def test_empty_input_row(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        result = tester.test("key1", [])
        self.assertEqual(result, [Row(key="key1", count=0)])

    def test_empty_input_pandas(self):
        processor = RunningCountStatefulProcessorFactory().pandas()
        tester = TwsTester(processor)
        input_df = pd.DataFrame({"value": []})
        result = tester.testInPandas("key1", input_df)
        expected = pd.DataFrame({"key": ["key1"], "count": [0]})
        pdt.assert_frame_equal(result, expected, check_like=True)

    def test_empty_initial_state_row(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor, initialStateRow=[])
        result = tester.test("key1", [Row(value="a")])
        self.assertEqual(result, [Row(key="key1", count=1)])

    def test_empty_initial_state_pandas(self):
        processor = RunningCountStatefulProcessorFactory().pandas()
        tester = TwsTester(processor, initialStatePandas=[])
        result = tester.testInPandas("key1", pd.DataFrame({"value": ["a"]}))
        expected = pd.DataFrame({"key": ["key1"], "count": [1]})
        pdt.assert_frame_equal(result, expected, check_like=True)

    def test_none_values_in_input(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        result = tester.test("key1", [Row(value=None), Row(value="a")])
        self.assertEqual(result, [Row(key="key1", count=2)])

    def test_single_row_input(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        result = tester.test("key1", [Row(value="a")])
        self.assertEqual(result, [Row(key="key1", count=1)])

    def test_peek_nonexistent_state(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        result = tester.peekValueState("count", "nonexistent_key")
        self.assertIsNone(result)

    def test_update_nonexistent_state(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        with self.assertRaises(AssertionError):
            tester.updateValueState("nonexistent_state", "key1", (5,))

    def test_peek_state_before_initialization(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        with self.assertRaises(AssertionError):
            tester.peekValueState("nonexistent_state", "key1")

    def test_clear_nonexistent_value_state(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        tester.handle.setGroupingKey("key1")
        value_state = tester.handle.getValueState("count", "int")
        value_state.clear()

    def test_clear_nonexistent_list_state(self):
        processor = TopKProcessorFactory(k=2).row()
        tester = TwsTester(processor)
        tester.handle.setGroupingKey("key1")
        list_state = tester.handle.getListState("topK", "double")
        list_state.clear()

    def test_clear_nonexistent_map_state(self):
        processor = WordFrequencyProcessorFactory().row()
        tester = TwsTester(processor)
        tester.handle.setGroupingKey("key1")
        map_state = tester.handle.getMapState("frequencies", "string", "int")
        map_state.clear()

    def test_delete_if_exists_nonexistent(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        tester.handle.deleteIfExists("nonexistent_state")

    def test_numeric_key(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        self.assertEqual(tester.test(1, [Row(value="a"), Row(value="c")]), [Row(key=1, count=2)])
        self.assertEqual(tester.test(2, [Row(value="b")]), [Row(key=2, count=1)])

    def test_tuple_key(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        result = tester.test((1, 2), [Row(value="a"), Row(value="b")])
        self.assertEqual(result, [Row(key=(1, 2), count=2)])

    def test_special_characters_in_key(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        result = tester.test("key@#$%", [Row(value="a"), Row(value="b")])
        self.assertEqual(result, [Row(key="key@#$%", count=2)])

    def test_null_key_value_row(self):
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)
        result = tester.test(None, [Row(value="a"), Row(value="b")])
        self.assertEqual(result, [Row(key=None, count=2)])

    def test_null_key_value_pandas(self):
        processor = RunningCountStatefulProcessorFactory().pandas()
        tester = TwsTester(processor)
        input_df = pd.DataFrame({"value": ["a", "b"]})
        result = tester.testInPandas(None, input_df)
        expected = pd.DataFrame({"key": [None], "count": [2]})
        pdt.assert_frame_equal(result, expected, check_like=True)

    def test_pandas_with_multiple_rows(self):
        processor = RunningCountStatefulProcessorFactory().pandas()
        tester = TwsTester(processor)
        input_df = pd.DataFrame({"value": ["a", "b", "c"]})
        result = tester.testInPandas("key1", input_df)
        expected = pd.DataFrame({"key": ["key1"], "count": [3]})
        pdt.assert_frame_equal(result, expected, check_like=True)

    def test_pandas_dtype_preservation(self):
        processor = RunningCountStatefulProcessorFactory().pandas()
        tester = TwsTester(processor)
        input_df = pd.DataFrame({"value": ["a"]})
        result = tester.testInPandas("key1", input_df)
        self.assertEqual(result["count"].dtype, "int64")

    def test_processor_init_called_once(self):
        from pyspark.sql.streaming.stateful_processor import (
            StatefulProcessor,
            StatefulProcessorHandle,
        )
        from typing import Iterator

        init_call_count = [0]

        class InitCountingProcessor(StatefulProcessor):
            def init(self, handle: StatefulProcessorHandle) -> None:
                init_call_count[0] += 1
                self.handle = handle
                self.state = handle.getValueState("count", "int")

            def handleInputRows(self, key, rows, timerValues) -> Iterator:
                val = self.state.get() if self.state.exists() else None
                count = val[0] if val else 0
                for row in rows:
                    count += 1
                self.state.update((count,))
                yield Row(key=key[0], count=count)

        processor = InitCountingProcessor()
        tester = TwsTester(processor)
        tester.test("key1", [Row(value="a")])
        tester.test("key1", [Row(value="b")])
        self.assertEqual(init_call_count[0], 1)

    def test_delete_value_state(self):
        """Test that deleteState correctly deletes value state for a given key."""
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)

        tester.updateValueState("count", "key1", (10,))
        tester.updateValueState("count", "key2", (20,))
        self.assertEqual(tester.peekValueState("count", "key1"), (10,))

        tester.deleteState("count", "key1")
        self.assertIsNone(tester.peekValueState("count", "key1"))
        self.assertEqual(tester.peekValueState("count", "key2"), (20,))

    def test_delete_list_state(self):
        """Test that deleteState correctly deletes list state for a given key."""
        processor = TopKProcessorFactory(k=3).row()
        tester = TwsTester(processor)

        tester.updateListState("topK", "key1", [(1.0,), (2.0,), (3.0,)])
        tester.updateListState("topK", "key2", [(4.0,), (5.0,)])
        self.assertEqual(tester.peekListState("topK", "key1"), [(1.0,), (2.0,), (3.0,)])

        tester.deleteState("topK", "key1")
        self.assertEqual(tester.peekListState("topK", "key1"), [])
        self.assertEqual(tester.peekListState("topK", "key2"), [(4.0,), (5.0,)])

    def test_delete_map_state(self):
        """Test that deleteState correctly deletes map state for a given key."""
        processor = WordFrequencyProcessorFactory().row()
        tester = TwsTester(processor)

        tester.updateMapState("frequencies", "user1", {("hello",): (5,), ("world",): (3,)})
        tester.updateMapState("frequencies", "user2", {("spark",): (10,)})
        self.assertEqual(
            tester.peekMapState("frequencies", "user1"),
            {("hello",): (5,), ("world",): (3,)},
        )

        tester.deleteState("frequencies", "user1")
        self.assertEqual(tester.peekMapState("frequencies", "user1"), {})
        self.assertEqual(tester.peekMapState("frequencies", "user2"), {("spark",): (10,)})

    def test_delete_nonexistent_state_raises_error(self):
        """Test that deleteState raises an error for non-existent state."""
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor)

        with self.assertRaisesRegex(PySparkAssertionError, "STATE_NOT_EXISTS"):
            tester.deleteState("nonexistent_state", "key1")

    # Timer tests.

    def test_processing_time_timers(self):
        """Test that TwsTester supports ProcessingTime timers."""
        processor = SessionTimeoutProcessorFactory().row()
        tester = TwsTester(processor, timeMode="ProcessingTime")

        # Process input for key1 - should register a timer at t=10000.
        result1 = tester.test("key1", [Row(value="hello")])
        self.assertEqual(result1, [Row(key="key1", result="received:hello")])

        # Set processing time to 5000 - timer should NOT fire yet.
        expired1 = tester.setProcessingTime(5000)
        self.assertEqual(expired1, [])

        # Process input for key2 at t=5000 - should register timer at t=15000.
        result2 = tester.test("key2", [Row(value="world")])
        self.assertEqual(result2, [Row(key="key2", result="received:world")])

        # Set processing time to 11000 - key1's timer should fire.
        expired2 = tester.setProcessingTime(11000)
        self.assertEqual(expired2, [Row(key="key1", result="session-expired")])

        # Set processing time to 16000 - key2's timer should fire.
        expired3 = tester.setProcessingTime(16000)
        self.assertEqual(expired3, [Row(key="key2", result="session-expired")])

        # Verify state is cleared after session expiry.
        self.assertIsNone(tester.peekValueState("lastSeen", "key1"))
        self.assertIsNone(tester.peekValueState("lastSeen", "key2"))

    def test_processing_time_timers_pandas(self):
        """Test that TwsTester supports ProcessingTime timers in Pandas mode."""
        processor = SessionTimeoutProcessorFactory().pandas()
        tester = TwsTester(processor, timeMode="ProcessingTime")

        # Process input for key1 - should register a timer at t=10000.
        result1 = tester.testInPandas("key1", pd.DataFrame({"value": ["hello"]}))
        expected1 = pd.DataFrame({"key": ["key1"], "result": ["received:hello"]})
        pdt.assert_frame_equal(result1, expected1, check_like=True)

        # Set processing time to 5000 - timer should NOT fire yet.
        expired1 = tester.setProcessingTime(5000)
        self.assertEqual(len(expired1), 0)

        # Process input for key2 at t=5000 - should register timer at t=15000.
        result2 = tester.testInPandas("key2", pd.DataFrame({"value": ["world"]}))
        expected2 = pd.DataFrame({"key": ["key2"], "result": ["received:world"]})
        pdt.assert_frame_equal(result2, expected2, check_like=True)

        # Set processing time to 11000 - key1's timer should fire.
        expired2 = tester.setProcessingTime(11000)
        expected_expired2 = pd.DataFrame({"key": ["key1"], "result": ["session-expired"]})
        pdt.assert_frame_equal(expired2, expected_expired2, check_like=True)

        # Set processing time to 16000 - key2's timer should fire.
        expired3 = tester.setProcessingTime(16000)
        expected_expired3 = pd.DataFrame({"key": ["key2"], "result": ["session-expired"]})
        pdt.assert_frame_equal(expired3, expected_expired3, check_like=True)

        # Verify state is cleared after session expiry.
        self.assertIsNone(tester.peekValueState("lastSeen", "key1"))
        self.assertIsNone(tester.peekValueState("lastSeen", "key2"))

    def test_event_time_timers_manual_watermark(self):
        """Test that TwsTester supports EventTime timers fired by manual watermark advance."""
        processor = EventTimeSessionProcessorFactory().row()

        def event_time_extractor(row: Row) -> int:
            return row.event_time_ms

        tester = TwsTester(
            processor,
            timeMode="EventTime",
            eventTimeExtractor=event_time_extractor,
        )

        # Process event at t=10000 for key1 - registers timer at t=15000.
        result1 = tester.test("key1", [Row(event_time_ms=10000, value="hello")])
        self.assertEqual(result1, [Row(key="key1", result="received:hello@10000")])

        # Process event at t=11000 for key2 - registers timer at t=16000.
        result2 = tester.test("key2", [Row(event_time_ms=11000, value="world")])
        self.assertEqual(result2, [Row(key="key2", result="received:world@11000")])

        # Set watermark to 15000 - key1's timer should fire.
        expired1 = tester.setWatermark(15000)
        self.assertEqual(len(expired1), 1)
        self.assertEqual(expired1[0], Row(key="key1", result="session-expired@watermark=15000"))

        # Set watermark to 16000 - key2's timer should fire.
        expired2 = tester.setWatermark(16000)
        self.assertEqual(len(expired2), 1)
        self.assertEqual(expired2[0], Row(key="key2", result="session-expired@watermark=16000"))

        # Verify state is cleared.
        self.assertIsNone(tester.peekValueState("lastEventTime", "key1"))
        self.assertIsNone(tester.peekValueState("lastEventTime", "key2"))

    def test_late_event_filtering(self):
        """Test that TwsTester filters late events in EventTime mode."""
        processor = EventTimeCountProcessorFactory().row()

        def event_time_extractor(row: Row) -> int:
            return row.event_time_ms

        tester = TwsTester(
            processor,
            timeMode="EventTime",
            eventTimeExtractor=event_time_extractor,
        )

        # Initially watermark is 0, so all events should be processed.
        result1 = tester.test(
            "key1",
            [
                Row(event_time_ms=5000, value="a"),
                Row(event_time_ms=6000, value="b"),
                Row(event_time_ms=7000, value="c"),
            ],
        )
        self.assertEqual(result1, [Row(key="key1", count=3)])
        self.assertEqual(tester.peekValueState("count", "key1"), (3,))

        # Set watermark to 6000.
        tester.setWatermark(6000)

        # Now watermark is 6000. Send events with mixed event times:
        # - (4000, "late1") -> event time 4000 <= watermark 6000, should be FILTERED.
        # - (5000, "late2") -> event time 5000 <= watermark 6000, should be FILTERED.
        # - (6000, "late3") -> event time 6000 <= watermark 6000, should be FILTERED.
        # - (8000, "ontime") -> event time 8000 > watermark 6000, should be PROCESSED.
        result2 = tester.test(
            "key1",
            [
                Row(event_time_ms=4000, value="late1"),
                Row(event_time_ms=5000, value="late2"),
                Row(event_time_ms=6000, value="late3"),
                Row(event_time_ms=8000, value="ontime"),
            ],
        )
        # Only 1 event should be processed (the on-time one)
        self.assertEqual(result2, [Row(key="key1", count=4)])  # 3 + 1 = 4
        self.assertEqual(tester.peekValueState("count", "key1"), (4,))

    def test_registers_on_time_events(self):
        "TwsTester with eventTimeExtractor should allow timer registration from on-time events"
        # With eventTimeExtractor, late events are filtered BEFORE reaching the processor,
        # so timers can only be registered from on-time events (no exception).
        tester = TwsTester(
            EventTimeSessionProcessorFactory().row(),
            timeMode="EventTime",
            eventTimeExtractor=lambda row: row.event_time_ms,
        )

        # Set watermark to 20 seconds.
        tester.setWatermark(20000)

        # Process a mix of late and on-time events.
        # Late event (10000) is filtered out, on-time event (25000) is processed.
        # Timer registered at 25000 + 5000 = 30000, which is > watermark, so no exception.
        result = tester.test(
            "key1",
            [Row(event_time_ms=10000, value="late"), Row(event_time_ms=25000, value="ontime")],
        )
        # Only the on-time event produces output.
        self.assertEqual(result, [Row(key="key1", result="received:ontime@25000")])

    def test_timer_registration_in_none_mode_raises_error(self):
        """Test that timer operations raise error in TimeMode.None."""
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor, timeMode="None")

        with self.assertRaisesRegex(PySparkValueError, "UNSUPPORTED_OPERATION"):
            tester.handle.registerTimer(12345)

    def test_set_processing_time_in_wrong_mode_raises_error(self):
        """Test that setProcessingTime raises error in non-ProcessingTime mode."""
        processor = RunningCountStatefulProcessorFactory().row()

        # Test in None mode.
        tester_none = TwsTester(processor, timeMode="None")
        with self.assertRaisesRegex(PySparkValueError, "UNSUPPORTED_OPERATION"):
            tester_none.setProcessingTime(1000)

    def test_set_watermark_in_wrong_mode_raises_error(self):
        """Test that setWatermark raises error in non-EventTime mode."""
        processor = RunningCountStatefulProcessorFactory().row()

        # Test in None mode.
        tester_none = TwsTester(processor, timeMode="None")
        with self.assertRaisesRegex(PySparkValueError, "UNSUPPORTED_OPERATION"):
            tester_none.setWatermark(1000)

        # Test in ProcessingTime mode.
        tester_pt = TwsTester(processor, timeMode="ProcessingTime")
        with self.assertRaisesRegex(PySparkValueError, "UNSUPPORTED_OPERATION"):
            tester_pt.setWatermark(1000)

    def test_eventtime_mode_requires_extractor(self):
        """Test that EventTime mode requires eventTimeExtractor."""
        processor = RunningCountStatefulProcessorFactory().row()

        with self.assertRaisesRegex(PySparkValueError, "ARGUMENT_REQUIRED"):
            TwsTester(processor, timeMode="EventTime")

    def test_processing_time_must_move_forward(self):
        """Test that setProcessingTime requires time to move forward."""
        processor = RunningCountStatefulProcessorFactory().row()
        tester = TwsTester(processor, timeMode="ProcessingTime")

        tester.setProcessingTime(1000)

        # Same time should raise error.
        with self.assertRaises(IllegalArgumentException):
            tester.setProcessingTime(1000)

        # Earlier time should raise error.
        with self.assertRaises(IllegalArgumentException):
            tester.setProcessingTime(500)

    def test_watermark_must_move_forward(self):
        """Test that setWatermark requires watermark to move forward."""
        processor = RunningCountStatefulProcessorFactory().row()

        def event_time_extractor(row: Row) -> int:
            return row.event_time_ms

        tester = TwsTester(
            processor,
            timeMode="EventTime",
            eventTimeExtractor=event_time_extractor,
        )

        tester.setWatermark(1000)

        # Same watermark should raise error.
        with self.assertRaises(IllegalArgumentException):
            tester.setWatermark(1000)

        # Earlier watermark should raise error.
        with self.assertRaises(IllegalArgumentException):
            tester.setWatermark(500)

    def test_timer_cannot_be_registered_in_past(self):
        """Test that timer cannot be registered at or before current watermark in EventTime mode."""
        processor = EventTimeSessionProcessorFactory().row()

        def event_time_extractor(row: Row) -> int:
            return row.event_time_ms

        tester = TwsTester(
            processor,
            timeMode="EventTime",
            eventTimeExtractor=event_time_extractor,
        )

        # Set watermark to 10000.
        tester.setWatermark(10000)

        # Try to register a timer at or before watermark - should raise error.
        tester.handle.setGroupingKey("key1")
        with self.assertRaises(IllegalArgumentException):
            tester.handle.registerTimer(10000)  # exactly at watermark

        with self.assertRaises(IllegalArgumentException):
            tester.handle.registerTimer(5000)  # before watermark


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
    # Supports multiple batches.
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
        key_column: str = input_columns[0]  # Assume first column is the key column.
        value_columns: list[str] = input_columns[1:]  # All columns except the key column.

        num_input_columns: int = len(input_columns)
        assert len(input_types) == num_input_columns

        # Use TwsTester to compute expected results.
        tester = TwsTester(processor)

        def _run_tester(batch: list[tuple]) -> list[Row]:
            # Group batch by key.
            grouped = {}
            for row in batch:
                key = row[0]
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(row[1:])

            results: list[Row] = []
            for key, values in grouped.items():
                if use_pandas:
                    input_to_tester = pd.DataFrame(values, columns=value_columns)
                    result_df = tester.testInPandas(key, input_to_tester)
                    results.extend(self.spark.createDataFrame(result_df).collect())
                else:
                    input_to_tester: list[Row] = [
                        Row(**dict(zip(value_columns, v))) for v in values
                    ]
                    results.extend(tester.test(key, input_to_tester))
            return results

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
            # This checks that collections are the same up to order, not just compares lengths.
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
