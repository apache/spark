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
import time
import tempfile
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from typing import Iterator

import unittest
from typing import cast

from pyspark import SparkConf
from pyspark.sql.functions import split
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    Row,
    IntegerType,
)
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_pandas:
    import pandas as pd


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class TransformWithStateInPandasTestsMixin:
    @classmethod
    def conf(cls):
        cfg = SparkConf()
        cfg.set("spark.sql.shuffle.partitions", "5")
        cfg.set(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        cfg.set("spark.sql.execution.arrow.transformWithStateInPandas.maxRecordsPerBatch", "2")
        return cfg

    def _prepare_input_data(self, input_path, col1, col2):
        with open(input_path, "w") as fw:
            for e1, e2 in zip(col1, col2):
                fw.write(f"{e1}, {e2}\n")

    def _prepare_test_resource1(self, input_path):
        self._prepare_input_data(input_path + "/text-test1.txt", [0, 0, 1, 1], [123, 46, 146, 346])

    def _prepare_test_resource2(self, input_path):
        self._prepare_input_data(
            input_path + "/text-test2.txt", [0, 0, 0, 1, 1], [123, 223, 323, 246, 6]
        )

    def _prepare_test_resource3(self, input_path):
        with open(input_path + "/text-test2.txt", "w") as fw:
            fw.write("3, 12\n")
            fw.write("0, 67\n")

    def _build_test_df(self, input_path):
        df = self.spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(input_path)
        df_split = df.withColumn("split_values", split(df["value"], ","))
        df_final = df_split.select(
            df_split.split_values.getItem(0).alias("id").cast("string"),
            df_split.split_values.getItem(1).alias("temperature").cast("int"),
        )
        return df_final

    def _test_transform_with_state_in_pandas_basic(
        self, stateful_processor, check_results, single_batch=False, timeMode="None"
    ):
        input_path = tempfile.mkdtemp()
        self._prepare_test_resource1(input_path)
        if not single_batch:
            self._prepare_test_resource2(input_path)

        df = self._build_test_df(input_path)

        for q in self.spark.streams.active:
            q.stop()
        self.assertTrue(df.isStreaming)

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("countAsString", StringType(), True),
            ]
        )

        q = (
            df.groupBy("id")
            .transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode=timeMode,
            )
            .writeStream.queryName("this_query")
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, "this_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_in_pandas_basic(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", countAsString="2"),
                    Row(id="1", countAsString="2"),
                }
            else:
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", countAsString="3"),
                    Row(id="1", countAsString="2"),
                }

        self._test_transform_with_state_in_pandas_basic(SimpleStatefulProcessor(), check_results)

    def test_transform_with_state_in_pandas_non_exist_value_state(self):
        def check_results(batch_df, _):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="0"),
                Row(id="1", countAsString="0"),
            }

        self._test_transform_with_state_in_pandas_basic(
            InvalidSimpleStatefulProcessor(), check_results, True
        )

    def test_transform_with_state_in_pandas_query_restarts(self):
        root_path = tempfile.mkdtemp()
        input_path = root_path + "/input"
        os.makedirs(input_path, exist_ok=True)
        checkpoint_path = root_path + "/checkpoint"
        output_path = root_path + "/output"

        self._prepare_test_resource1(input_path)

        df = self._build_test_df(input_path)

        for q in self.spark.streams.active:
            q.stop()
        self.assertTrue(df.isStreaming)

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("countAsString", StringType(), True),
            ]
        )

        base_query = (
            df.groupBy("id")
            .transformWithStateInPandas(
                statefulProcessor=SimpleStatefulProcessor(),
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
            )
            .writeStream.queryName("this_query")
            .format("parquet")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("path", output_path)
        )
        q = base_query.start()
        self.assertEqual(q.name, "this_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

        q.stop()

        self._prepare_test_resource2(input_path)

        q = base_query.start()
        self.assertEqual(q.name, "this_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)
        result_df = self.spark.read.parquet(output_path)
        assert set(result_df.sort("id").collect()) == {
            Row(id="0", countAsString="2"),
            Row(id="0", countAsString="3"),
            Row(id="1", countAsString="2"),
            Row(id="1", countAsString="2"),
        }

    def test_transform_with_state_in_pandas_list_state(self):
        def check_results(batch_df, _):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="2"),
                Row(id="1", countAsString="2"),
            }

        self._test_transform_with_state_in_pandas_basic(ListStateProcessor(), check_results, True)

    # test list state with ttl has the same behavior as list state when state doesn't expire.
    def test_transform_with_state_in_pandas_list_state_large_ttl(self):
        def check_results(batch_df, _):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="2"),
                Row(id="1", countAsString="2"),
            }

        self._test_transform_with_state_in_pandas_basic(
            ListStateLargeTTLProcessor(), check_results, True, "processingTime"
        )

    # test value state with ttl has the same behavior as value state when
    # state doesn't expire.
    def test_value_state_ttl_basic(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", countAsString="2"),
                    Row(id="1", countAsString="2"),
                }
            else:
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", countAsString="3"),
                    Row(id="1", countAsString="2"),
                }

        self._test_transform_with_state_in_pandas_basic(
            SimpleTTLStatefulProcessor(), check_results, False, "processingTime"
        )

    def test_value_state_ttl_expiration(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                assertDataFrameEqual(
                    batch_df,
                    [
                        Row(id="ttl-count-0", count=1),
                        Row(id="count-0", count=1),
                        Row(id="ttl-list-state-count-0", count=1),
                        Row(id="ttl-count-1", count=1),
                        Row(id="count-1", count=1),
                        Row(id="ttl-list-state-count-1", count=1),
                    ],
                )
            elif batch_id == 1:
                assertDataFrameEqual(
                    batch_df,
                    [
                        Row(id="ttl-count-0", count=2),
                        Row(id="count-0", count=2),
                        Row(id="ttl-list-state-count-0", count=3),
                        Row(id="ttl-count-1", count=2),
                        Row(id="count-1", count=2),
                        Row(id="ttl-list-state-count-1", count=3),
                    ],
                )
            elif batch_id == 2:
                # ttl-count-0 expire and restart from count 0.
                # The TTL for value state ttl_count_state gets reset in batch 1 because of the
                # update operation and ttl-count-1 keeps the state.
                # ttl-list-state-count-0 expire and restart from count 0.
                # The TTL for list state ttl_list_state gets reset in batch 1 because of the
                # put operation and ttl-list-state-count-1 keeps the state.
                # non-ttl state never expires
                assertDataFrameEqual(
                    batch_df,
                    [
                        Row(id="ttl-count-0", count=1),
                        Row(id="count-0", count=3),
                        Row(id="ttl-list-state-count-0", count=1),
                        Row(id="ttl-count-1", count=3),
                        Row(id="count-1", count=3),
                        Row(id="ttl-list-state-count-1", count=7),
                    ],
                )
            if batch_id == 0 or batch_id == 1:
                time.sleep(6)

        input_dir = tempfile.TemporaryDirectory()
        input_path = input_dir.name
        try:
            df = self._build_test_df(input_path)
            self._prepare_input_data(input_path + "/batch1.txt", [1, 0], [0, 0])
            self._prepare_input_data(input_path + "/batch2.txt", [1, 0], [0, 0])
            self._prepare_input_data(input_path + "/batch3.txt", [1, 0], [0, 0])
            for q in self.spark.streams.active:
                q.stop()
            output_schema = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("count", IntegerType(), True),
                ]
            )

            q = (
                df.groupBy("id")
                .transformWithStateInPandas(
                    statefulProcessor=TTLStatefulProcessor(),
                    outputStructType=output_schema,
                    outputMode="Update",
                    timeMode="processingTime",
                )
                .writeStream.foreachBatch(check_results)
                .outputMode("update")
                .start()
            )
            self.assertTrue(q.isActive)
            q.processAllAvailable()
            q.stop()
            q.awaitTermination()
            self.assertTrue(q.exception() is None)
        finally:
            input_dir.cleanup()

    def _test_transform_with_state_init_state_in_pandas(
            self, stateful_processor, check_results
    ):
        input_path = tempfile.mkdtemp()
        self._prepare_test_resource1(input_path)
        self._prepare_test_resource3(input_path)

        df = self._build_test_df(input_path)

        for q in self.spark.streams.active:
            q.stop()
        self.assertTrue(df.isStreaming)

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )

        from pyspark.sql import GroupedData

        data = [("0", 789), ("3", 987)]
        initial_state = \
            self.spark.createDataFrame(data, "id string, initVal int") \
                .groupBy("id")

        q = (
            df.groupBy("id")
            .transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
                initialState=initial_state
            )
            .writeStream.queryName("this_query")
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, "this_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_init_state_in_pandas(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                # for key 0, initial state was processed and it was only processed once;
                # for key 1, it did not appear in the initial state df;
                # for key 3, it did not appear in the first batch of input keys
                # so it won't be emitted
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", value=str(789 + 123 + 46)),
                    Row(id="1", value=str(146 + 346)),
                }
            else:
                # for key 0, verify initial state was only processed once in the first batch;
                # for key 3, verify init state was now processed
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", value=str(789 + 123 + 46 + 67)),
                    Row(id="3", value=str(987 + 12)),
                }

        self._test_transform_with_state_init_state_in_pandas(SimpleStatefulProcessorWithInitialState(), check_results)


class SimpleStatefulProcessorWithInitialState(StatefulProcessor):

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.value_state = handle.getValueState("value_state", state_schema)

    def handleInputRows(self, key, rows) -> Iterator[pd.DataFrame]:
        print(f"enter handle input rows: {key}\n")
        exists = self.value_state.exists()
        if exists:
            value_row = self.value_state.get()
            existing_value = value_row[0]
        else:
            existing_value = 0

        accumulated_value = existing_value

        for pdf in rows:
            value = pdf['temperature'].astype(int).sum()
            accumulated_value += value

        self.value_state.update((accumulated_value,))
        print(f"done with handle input for key: {key}\n")

        yield pd.DataFrame({"id": key, "value": str(accumulated_value)})

    def handleInitialState(self, key, initialState) -> None:
        initVal = initialState.at[0, "initVal"]
        self.value_state.update((initVal,))
        print(f"done with handle initial state for key: {key}\n")

    def close(self) -> None:
        pass


class SimpleStatefulProcessor(StatefulProcessor):
    dict = {0: {"0": 1, "1": 2}, 1: {"0": 4, "1": 3}}
    batch_id = 0

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.num_violations_state = handle.getValueState("numViolations", state_schema)

    def handleInputRows(self, key, rows) -> Iterator[pd.DataFrame]:
        new_violations = 0
        count = 0
        key_str = key[0]
        exists = self.num_violations_state.exists()
        if exists:
            existing_violations_row = self.num_violations_state.get()
            existing_violations = existing_violations_row[0]
            assert existing_violations == self.dict[0][key_str]
            self.batch_id = 1
        else:
            existing_violations = 0
        for pdf in rows:
            pdf_count = pdf.count()
            count += pdf_count.get("temperature")
            violations_pdf = pdf.loc[pdf["temperature"] > 100]
            new_violations += violations_pdf.count().get("temperature")
        updated_violations = new_violations + existing_violations
        assert updated_violations == self.dict[self.batch_id][key_str]
        self.num_violations_state.update((updated_violations,))
        yield pd.DataFrame({"id": key, "countAsString": str(count)})

    def close(self) -> None:
        pass


# A stateful processor that inherit all behavior of SimpleStatefulProcessor except that it use
# ttl state with a large timeout.
class SimpleTTLStatefulProcessor(SimpleStatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.num_violations_state = handle.getValueState("numViolations", state_schema, 30000)


class TTLStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.ttl_count_state = handle.getValueState("ttl-state", state_schema, 10000)
        self.count_state = handle.getValueState("state", state_schema)
        self.ttl_list_state = handle.getListState("ttl-list-state", state_schema, 10000)

    def handleInputRows(self, key, rows) -> Iterator[pd.DataFrame]:
        count = 0
        ttl_count = 0
        ttl_list_state_count = 0
        id = key[0]
        if self.count_state.exists():
            count = self.count_state.get()[0]
        if self.ttl_count_state.exists():
            ttl_count = self.ttl_count_state.get()[0]
        if self.ttl_list_state.exists():
            iter = self.ttl_list_state.get()
            for s in iter:
                ttl_list_state_count += s[0]
        for pdf in rows:
            pdf_count = pdf.count().get("temperature")
            count += pdf_count
            ttl_count += pdf_count
            ttl_list_state_count += pdf_count

        self.count_state.update((count,))
        # skip updating state for the 2nd batch so that ttl state expire
        if not (ttl_count == 2 and id == "0"):
            self.ttl_count_state.update((ttl_count,))
            self.ttl_list_state.put([(ttl_list_state_count,), (ttl_list_state_count,)])
        yield pd.DataFrame(
            {
                "id": [f"ttl-count-{id}", f"count-{id}", f"ttl-list-state-count-{id}"],
                "count": [ttl_count, count, ttl_list_state_count],
            }
        )

    def close(self) -> None:
        pass


class InvalidSimpleStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.num_violations_state = handle.getValueState("numViolations", state_schema)

    def handleInputRows(self, key, rows) -> Iterator[pd.DataFrame]:
        count = 0
        exists = self.num_violations_state.exists()
        assert not exists
        # try to get a state variable with no value
        assert self.num_violations_state.get() is None
        self.num_violations_state.clear()
        yield pd.DataFrame({"id": key, "countAsString": str(count)})

    def close(self) -> None:
        pass


class ListStateProcessor(StatefulProcessor):
    # Dict to store the expected results. The key represents the grouping key string, and the value
    # is a dictionary of pandas dataframe index -> expected temperature value. Since we set
    # maxRecordsPerBatch to 2, we expect the pandas dataframe dictionary to have 2 entries.
    dict = {0: 120, 1: 20}

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("temperature", IntegerType(), True)])
        self.list_state1 = handle.getListState("listState1", state_schema)
        self.list_state2 = handle.getListState("listState2", state_schema)

    def handleInputRows(self, key, rows) -> Iterator[pd.DataFrame]:
        count = 0
        for pdf in rows:
            list_state_rows = [(120,), (20,)]
            self.list_state1.put(list_state_rows)
            self.list_state2.put(list_state_rows)
            self.list_state1.append_value((111,))
            self.list_state2.append_value((222,))
            self.list_state1.append_list(list_state_rows)
            self.list_state2.append_list(list_state_rows)
            pdf_count = pdf.count()
            count += pdf_count.get("temperature")
        iter1 = self.list_state1.get()
        iter2 = self.list_state2.get()
        # Mixing the iterator to test it we can resume from the correct point
        assert next(iter1)[0] == self.dict[0]
        assert next(iter2)[0] == self.dict[0]
        assert next(iter1)[0] == self.dict[1]
        assert next(iter2)[0] == self.dict[1]
        # Get another iterator for list_state1 to test if the 2 iterators (iter1 and iter3) don't
        # interfere with each other.
        iter3 = self.list_state1.get()
        assert next(iter3)[0] == self.dict[0]
        assert next(iter3)[0] == self.dict[1]
        # the second arrow batch should contain the appended value 111 for list_state1 and
        # 222 for list_state2
        assert next(iter1)[0] == 111
        assert next(iter2)[0] == 222
        assert next(iter3)[0] == 111
        # since we put another 2 rows after 111/222, check them here
        assert next(iter1)[0] == self.dict[0]
        assert next(iter2)[0] == self.dict[0]
        assert next(iter3)[0] == self.dict[0]
        assert next(iter1)[0] == self.dict[1]
        assert next(iter2)[0] == self.dict[1]
        assert next(iter3)[0] == self.dict[1]
        yield pd.DataFrame({"id": key, "countAsString": str(count)})

    def close(self) -> None:
        pass


# A stateful processor that inherit all behavior of ListStateProcessor except that it use
# ttl state with a large timeout.
class ListStateLargeTTLProcessor(ListStateProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("temperature", IntegerType(), True)])
        self.list_state1 = handle.getListState("listState1", state_schema, 30000)
        self.list_state2 = handle.getListState("listState2", state_schema, 30000)


class TransformWithStateInPandasTests(TransformWithStateInPandasTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_transform_with_state import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
