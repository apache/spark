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
        self._prepare_input_data(input_path + "/text-test3.txt", [0, 1], [123, 6])

    def _build_test_df(self, input_path):
        df = self.spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(input_path)
        df_split = df.withColumn("split_values", split(df["value"], ","))
        df_final = df_split.select(
            df_split.split_values.getItem(0).alias("id").cast("string"),
            df_split.split_values.getItem(1).alias("temperature").cast("int"),
        )
        return df_final

    def _prepare_input_data_with_3_cols(self, input_path, col1, col2, col3):
        with open(input_path, "w") as fw:
            for e1, e2, e3 in zip(col1, col2, col3):
                fw.write(f"{e1},{e2},{e3}\n")

    def build_test_df_with_3_cols(self, input_path):
        df = self.spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(input_path)
        df_split = df.withColumn("split_values", split(df["value"], ","))
        df_final = df_split.select(
            df_split.split_values.getItem(0).alias("id1").cast("string"),
            df_split.split_values.getItem(1).alias("temperature").cast("int"),
            df_split.split_values.getItem(2).alias("id2").cast("string"),
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

    def test_transform_with_state_in_pandas_map_state(self):
        def check_results(batch_df, _):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="2"),
                Row(id="1", countAsString="2"),
            }

        self._test_transform_with_state_in_pandas_basic(MapStateProcessor(), check_results, True)

    # test map state with ttl has the same behavior as map state when state doesn't expire.
    def test_transform_with_state_in_pandas_map_state_large_ttl(self):
        def check_results(batch_df, _):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="2"),
                Row(id="1", countAsString="2"),
            }

        self._test_transform_with_state_in_pandas_basic(
            MapStateLargeTTLProcessor(), check_results, True, "processingTime"
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
                        Row(id="ttl-map-state-count-0", count=1),
                        Row(id="ttl-count-1", count=1),
                        Row(id="count-1", count=1),
                        Row(id="ttl-list-state-count-1", count=1),
                        Row(id="ttl-map-state-count-1", count=1),
                    ],
                )
            elif batch_id == 1:
                assertDataFrameEqual(
                    batch_df,
                    [
                        Row(id="ttl-count-0", count=2),
                        Row(id="count-0", count=2),
                        Row(id="ttl-list-state-count-0", count=3),
                        Row(id="ttl-map-state-count-0", count=2),
                        Row(id="ttl-count-1", count=2),
                        Row(id="count-1", count=2),
                        Row(id="ttl-list-state-count-1", count=3),
                        Row(id="ttl-map-state-count-1", count=2),
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
                        Row(id="ttl-map-state-count-0", count=1),
                        Row(id="ttl-count-1", count=3),
                        Row(id="count-1", count=3),
                        Row(id="ttl-list-state-count-1", count=7),
                        Row(id="ttl-map-state-count-1", count=3),
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

    def _test_transform_with_state_in_pandas_proc_timer(self, stateful_processor, check_results):
        input_path = tempfile.mkdtemp()
        self._prepare_test_resource3(input_path)
        self._prepare_test_resource1(input_path)
        self._prepare_test_resource2(input_path)

        df = self._build_test_df(input_path)

        for q in self.spark.streams.active:
            q.stop()
        self.assertTrue(df.isStreaming)

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("countAsString", StringType(), True),
                StructField("timeValues", StringType(), True),
            ]
        )

        query_name = "processing_time_test_query"
        q = (
            df.groupBy("id")
            .transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="processingtime",
            )
            .writeStream.queryName(query_name)
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, query_name)
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_in_pandas_proc_timer(self):
        # helper function to check expired timestamp is smaller than current processing time
        def check_timestamp(batch_df):
            expired_df = (
                batch_df.filter(batch_df["countAsString"] == "-1")
                .select("id", "timeValues")
                .withColumnRenamed("timeValues", "expiredTimestamp")
            )
            count_df = (
                batch_df.filter(batch_df["countAsString"] != "-1")
                .select("id", "timeValues")
                .withColumnRenamed("timeValues", "countStateTimestamp")
            )
            joined_df = expired_df.join(count_df, on="id")
            for row in joined_df.collect():
                assert row["expiredTimestamp"] < row["countStateTimestamp"]

        def check_results(batch_df, batch_id):
            if batch_id == 0:
                assert set(batch_df.sort("id").select("id", "countAsString").collect()) == {
                    Row(id="0", countAsString="1"),
                    Row(id="1", countAsString="1"),
                }
            elif batch_id == 1:
                # for key 0, the accumulated count is emitted before the count state is cleared
                # during the timer process
                assert set(batch_df.sort("id").select("id", "countAsString").collect()) == {
                    Row(id="0", countAsString="3"),
                    Row(id="0", countAsString="-1"),
                    Row(id="1", countAsString="3"),
                }
                self.first_expired_timestamp = batch_df.filter(
                    batch_df["countAsString"] == -1
                ).first()["timeValues"]
                check_timestamp(batch_df)

            else:
                assert set(batch_df.sort("id").select("id", "countAsString").collect()) == {
                    Row(id="0", countAsString="3"),
                    Row(id="0", countAsString="-1"),
                    Row(id="1", countAsString="5"),
                }
                # The expired timestamp in current batch is larger than expiry timestamp in batch 1
                # because this is a new timer registered in batch1 and
                # different from the one registered in batch 0
                current_batch_expired_timestamp = batch_df.filter(
                    batch_df["countAsString"] == -1
                ).first()["timeValues"]
                assert current_batch_expired_timestamp > self.first_expired_timestamp

        self._test_transform_with_state_in_pandas_proc_timer(
            ProcTimeStatefulProcessor(), check_results
        )

    def _test_transform_with_state_in_pandas_event_time(self, stateful_processor, check_results):
        import pyspark.sql.functions as f

        input_path = tempfile.mkdtemp()

        def prepare_batch1(input_path):
            with open(input_path + "/text-test3.txt", "w") as fw:
                fw.write("a, 20\n")

        def prepare_batch2(input_path):
            with open(input_path + "/text-test1.txt", "w") as fw:
                fw.write("a, 4\n")

        def prepare_batch3(input_path):
            with open(input_path + "/text-test2.txt", "w") as fw:
                fw.write("a, 11\n")
                fw.write("a, 13\n")
                fw.write("a, 15\n")

        prepare_batch1(input_path)
        prepare_batch2(input_path)
        prepare_batch3(input_path)

        df = self._build_test_df(input_path)
        df = df.select(
            "id", f.from_unixtime(f.col("temperature")).alias("eventTime").cast("timestamp")
        ).withWatermark("eventTime", "10 seconds")

        for q in self.spark.streams.active:
            q.stop()
        self.assertTrue(df.isStreaming)

        output_schema = StructType(
            [StructField("id", StringType(), True), StructField("timestamp", StringType(), True)]
        )

        query_name = "event_time_test_query"
        q = (
            df.groupBy("id")
            .transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="eventtime",
            )
            .writeStream.queryName(query_name)
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, query_name)
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_in_pandas_event_time(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                assert set(batch_df.sort("id").collect()) == {Row(id="a", timestamp="20")}
            elif batch_id == 1:
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="a", timestamp="20"),
                    Row(id="a-expired", timestamp="0"),
                }
            else:
                # watermark has not progressed, so timer registered in batch 1(watermark = 10)
                # has not yet expired
                assert set(batch_df.sort("id").collect()) == {Row(id="a", timestamp="15")}

        self._test_transform_with_state_in_pandas_event_time(
            EventTimeStatefulProcessor(), check_results
        )

    def _test_transform_with_state_init_state_in_pandas(self, stateful_processor, check_results):
        input_path = tempfile.mkdtemp()
        self._prepare_test_resource1(input_path)
        self._prepare_input_data(input_path + "/text-test2.txt", [0, 3], [67, 12])

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

        data = [("0", 789), ("3", 987)]
        initial_state = self.spark.createDataFrame(data, "id string, initVal int").groupBy("id")

        q = (
            df.groupBy("id")
            .transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
                initialState=initial_state,
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
                # for key 3, verify init state was processed and reflected in the accumulated value
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", value=str(789 + 123 + 46 + 67)),
                    Row(id="3", value=str(987 + 12)),
                }

        self._test_transform_with_state_init_state_in_pandas(
            SimpleStatefulProcessorWithInitialState(), check_results
        )

    def _test_transform_with_state_non_contiguous_grouping_cols(
        self, stateful_processor, check_results, initial_state=None
    ):
        input_path = tempfile.mkdtemp()
        self._prepare_input_data_with_3_cols(
            input_path + "/text-test1.txt", [0, 0, 1, 1], [123, 46, 146, 346], [1, 1, 2, 2]
        )

        df = self.build_test_df_with_3_cols(input_path)

        for q in self.spark.streams.active:
            q.stop()
        self.assertTrue(df.isStreaming)

        output_schema = StructType(
            [
                StructField("id1", StringType(), True),
                StructField("id2", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )

        q = (
            df.groupBy("id1", "id2")
            .transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
                initialState=initial_state,
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

    def test_transform_with_state_non_contiguous_grouping_cols(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                assert set(batch_df.collect()) == {
                    Row(id1="0", id2="1", value=str(123 + 46)),
                    Row(id1="1", id2="2", value=str(146 + 346)),
                }

        self._test_transform_with_state_non_contiguous_grouping_cols(
            SimpleStatefulProcessorWithInitialState(), check_results
        )

    def test_transform_with_state_non_contiguous_grouping_cols_with_init_state(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                # initial state for key (0, 1) is processed
                assert set(batch_df.collect()) == {
                    Row(id1="0", id2="1", value=str(789 + 123 + 46)),
                    Row(id1="1", id2="2", value=str(146 + 346)),
                }

        # grouping key of initial state is also not starting from the beginning of attributes
        data = [(789, "0", "1"), (987, "3", "2")]
        initial_state = self.spark.createDataFrame(
            data, "initVal int, id1 string, id2 string"
        ).groupBy("id1", "id2")

        self._test_transform_with_state_non_contiguous_grouping_cols(
            SimpleStatefulProcessorWithInitialState(), check_results, initial_state
        )


class SimpleStatefulProcessorWithInitialState(StatefulProcessor):
    # this dict is the same as input initial state dataframe
    dict = {("0",): 789, ("3",): 987}

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.value_state = handle.getValueState("value_state", state_schema)

    def handleInputRows(
        self, key, rows, timer_values, expired_timer_info
    ) -> Iterator[pd.DataFrame]:
        exists = self.value_state.exists()
        if exists:
            value_row = self.value_state.get()
            existing_value = value_row[0]
        else:
            existing_value = 0

        accumulated_value = existing_value

        for pdf in rows:
            value = pdf["temperature"].astype(int).sum()
            accumulated_value += value

        self.value_state.update((accumulated_value,))

        if len(key) > 1:
            yield pd.DataFrame(
                {"id1": (key[0],), "id2": (key[1],), "value": str(accumulated_value)}
            )
        else:
            yield pd.DataFrame({"id": key, "value": str(accumulated_value)})

    def handleInitialState(self, key, initialState) -> None:
        init_val = initialState.at[0, "initVal"]
        self.value_state.update((init_val,))
        if len(key) == 1:
            assert self.dict[key] == init_val

    def close(self) -> None:
        pass


# A stateful processor that output the max event time it has seen. Register timer for
# current watermark. Clear max state if timer expires.
class EventTimeStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", StringType(), True)])
        self.handle = handle
        self.max_state = handle.getValueState("max_state", state_schema)

    def handleInputRows(
        self, key, rows, timer_values, expired_timer_info
    ) -> Iterator[pd.DataFrame]:
        if expired_timer_info.is_valid():
            self.max_state.clear()
            self.handle.deleteTimer(expired_timer_info.get_expiry_time_in_ms())
            str_key = f"{str(key[0])}-expired"
            yield pd.DataFrame(
                {"id": (str_key,), "timestamp": str(expired_timer_info.get_expiry_time_in_ms())}
            )

        else:
            timestamp_list = []
            for pdf in rows:
                # int64 will represent timestamp in nanosecond, restore to second
                timestamp_list.extend((pdf["eventTime"].astype("int64") // 10**9).tolist())

            if self.max_state.exists():
                cur_max = int(self.max_state.get()[0])
            else:
                cur_max = 0
            max_event_time = str(max(cur_max, max(timestamp_list)))

            self.max_state.update((max_event_time,))
            self.handle.registerTimer(timer_values.get_current_watermark_in_ms())

            yield pd.DataFrame({"id": key, "timestamp": max_event_time})

    def close(self) -> None:
        pass


# A stateful processor that output the accumulation of count of input rows; register
# processing timer and clear the counter if timer expires.
class ProcTimeStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", StringType(), True)])
        self.handle = handle
        self.count_state = handle.getValueState("count_state", state_schema)

    def handleInputRows(
        self, key, rows, timer_values, expired_timer_info
    ) -> Iterator[pd.DataFrame]:
        if expired_timer_info.is_valid():
            # reset count state each time the timer is expired
            timer_list_1 = [e for e in self.handle.listTimers()]
            timer_list_2 = []
            idx = 0
            for e in self.handle.listTimers():
                timer_list_2.append(e)
                # check multiple iterator on the same grouping key works
                assert timer_list_2[idx] == timer_list_1[idx]
                idx += 1

            if len(timer_list_1) > 0:
                # before deleting the expiring timers, there are 2 timers -
                # one timer we just registered, and one that is going to be deleted
                assert len(timer_list_1) == 2
            self.count_state.clear()
            self.handle.deleteTimer(expired_timer_info.get_expiry_time_in_ms())
            yield pd.DataFrame(
                {
                    "id": key,
                    "countAsString": str("-1"),
                    "timeValues": str(expired_timer_info.get_expiry_time_in_ms()),
                }
            )

        else:
            if not self.count_state.exists():
                count = 0
            else:
                count = int(self.count_state.get()[0])

            if key == ("0",):
                self.handle.registerTimer(timer_values.get_current_processing_time_in_ms())

            rows_count = 0
            for pdf in rows:
                pdf_count = len(pdf)
                rows_count += pdf_count

            count = count + rows_count

            self.count_state.update((str(count),))
            timestamp = str(timer_values.get_current_processing_time_in_ms())

            yield pd.DataFrame({"id": key, "countAsString": str(count), "timeValues": timestamp})

    def close(self) -> None:
        pass


class SimpleStatefulProcessor(StatefulProcessor):
    dict = {0: {"0": 1, "1": 2}, 1: {"0": 4, "1": 3}}
    batch_id = 0

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.num_violations_state = handle.getValueState("numViolations", state_schema)

    def handleInputRows(
        self, key, rows, timer_values, expired_timer_info
    ) -> Iterator[pd.DataFrame]:
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
        user_key_schema = StructType([StructField("id", StringType(), True)])
        self.ttl_count_state = handle.getValueState("ttl-state", state_schema, 10000)
        self.count_state = handle.getValueState("state", state_schema)
        self.ttl_list_state = handle.getListState("ttl-list-state", state_schema, 10000)
        self.ttl_map_state = handle.getMapState(
            "ttl-map-state", user_key_schema, state_schema, 10000
        )

    def handleInputRows(
        self, key, rows, timer_values, expired_timer_info
    ) -> Iterator[pd.DataFrame]:
        count = 0
        ttl_count = 0
        ttl_list_state_count = 0
        ttl_map_state_count = 0
        id = key[0]
        if self.count_state.exists():
            count = self.count_state.get()[0]
        if self.ttl_count_state.exists():
            ttl_count = self.ttl_count_state.get()[0]
        if self.ttl_list_state.exists():
            iter = self.ttl_list_state.get()
            for s in iter:
                ttl_list_state_count += s[0]
        if self.ttl_map_state.exists():
            ttl_map_state_count = self.ttl_map_state.get_value(key)[0]
        for pdf in rows:
            pdf_count = pdf.count().get("temperature")
            count += pdf_count
            ttl_count += pdf_count
            ttl_list_state_count += pdf_count
            ttl_map_state_count += pdf_count

        self.count_state.update((count,))
        # skip updating state for the 2nd batch so that ttl state expire
        if not (ttl_count == 2 and id == "0"):
            self.ttl_count_state.update((ttl_count,))
            self.ttl_list_state.put([(ttl_list_state_count,), (ttl_list_state_count,)])
            self.ttl_map_state.update_value(key, (ttl_map_state_count,))
        yield pd.DataFrame(
            {
                "id": [
                    f"ttl-count-{id}",
                    f"count-{id}",
                    f"ttl-list-state-count-{id}",
                    f"ttl-map-state-count-{id}",
                ],
                "count": [ttl_count, count, ttl_list_state_count, ttl_map_state_count],
            }
        )

    def close(self) -> None:
        pass


class InvalidSimpleStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.num_violations_state = handle.getValueState("numViolations", state_schema)

    def handleInputRows(
        self, key, rows, timer_values, expired_timer_info
    ) -> Iterator[pd.DataFrame]:
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

    def handleInputRows(
        self, key, rows, timer_values, expired_timer_info
    ) -> Iterator[pd.DataFrame]:
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


class ListStateLargeTTLProcessor(ListStateProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("temperature", IntegerType(), True)])
        self.list_state1 = handle.getListState("listState1", state_schema, 30000)
        self.list_state2 = handle.getListState("listState2", state_schema, 30000)


class MapStateProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle):
        key_schema = StructType([StructField("name", StringType(), True)])
        value_schema = StructType([StructField("count", IntegerType(), True)])
        self.map_state = handle.getMapState("mapState", key_schema, value_schema)

    def handleInputRows(
        self, key, rows, timer_values, expired_timer_info
    ) -> Iterator[pd.DataFrame]:
        count = 0
        key1 = ("key1",)
        key2 = ("key2",)
        for pdf in rows:
            pdf_count = pdf.count()
            count += pdf_count.get("temperature")
        value1 = count
        value2 = count
        if self.map_state.exists():
            if self.map_state.contains_key(key1):
                value1 += self.map_state.get_value(key1)[0]
            if self.map_state.contains_key(key2):
                value2 += self.map_state.get_value(key2)[0]
        self.map_state.update_value(key1, (value1,))
        self.map_state.update_value(key2, (value2,))
        key_iter = self.map_state.keys()
        assert next(key_iter)[0] == "key1"
        assert next(key_iter)[0] == "key2"
        value_iter = self.map_state.values()
        assert next(value_iter)[0] == value1
        assert next(value_iter)[0] == value2
        map_iter = self.map_state.iterator()
        assert next(map_iter)[0] == key1
        assert next(map_iter)[1] == (value2,)
        self.map_state.remove_key(key1)
        assert not self.map_state.contains_key(key1)
        yield pd.DataFrame({"id": key, "countAsString": str(count)})

    def close(self) -> None:
        pass


# A stateful processor that inherit all behavior of MapStateProcessor except that it use
# ttl state with a large timeout.
class MapStateLargeTTLProcessor(MapStateProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        key_schema = StructType([StructField("name", StringType(), True)])
        value_schema = StructType([StructField("count", IntegerType(), True)])
        self.map_state = handle.getMapState("mapState", key_schema, value_schema, 30000)


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
