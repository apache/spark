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

from abc import abstractmethod

import json
import os
import time
import tempfile
from pyspark.sql.streaming import StatefulProcessor

import unittest
from typing import cast

from pyspark import SparkConf
from pyspark.sql.functions import array_sort, col, explode, split
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    Row,
    IntegerType,
    TimestampType,
)
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

from pyspark.sql.tests.pandas.helper.helper_pandas_transform_with_state import (
    SimpleStatefulProcessorWithInitialStateFactory,
    StatefulProcessorWithInitialStateTimersFactory,
    StatefulProcessorWithListStateInitialStateFactory,
    EventTimeStatefulProcessorFactory,
    ProcTimeStatefulProcessorFactory,
    SimpleStatefulProcessorFactory,
    StatefulProcessorChainingOpsFactory,
    SimpleTTLStatefulProcessorFactory,
    TTLStatefulProcessorFactory,
    InvalidSimpleStatefulProcessorFactory,
    ListStateProcessorFactory,
    ListStateLargeListProcessorFactory,
    ListStateLargeTTLProcessorFactory,
    MapStateProcessorFactory,
    MapStateLargeTTLProcessorFactory,
    BasicProcessorFactory,
    BasicProcessorNotNullableFactory,
    AddFieldsProcessorFactory,
    RemoveFieldsProcessorFactory,
    ReorderedFieldsProcessorFactory,
    UpcastProcessorFactory,
    MinEventTimeStatefulProcessorFactory,
)


class TransformWithStateTestsMixin:
    @classmethod
    @abstractmethod
    def use_pandas(cls) -> bool:
        ...

    @classmethod
    def get_processor(cls, stateful_processor_factory) -> StatefulProcessor:
        if cls.use_pandas():
            return stateful_processor_factory.pandas()
        else:
            return stateful_processor_factory.row()

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

    def _prepare_test_resource4(self, input_path):
        self._prepare_input_data(input_path + "/text-test4.txt", [0, 1], [123, 6])

    def _prepare_test_resource5(self, input_path):
        self._prepare_input_data(input_path + "/text-test5.txt", [0, 1], [123, 6])

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

    def _test_transform_with_state_basic(
        self,
        stateful_processor_factory,
        check_results,
        single_batch=False,
        timeMode="None",
        checkpoint_path=None,
        initial_state=None,
    ):
        input_path = tempfile.mkdtemp()
        if checkpoint_path is None:
            checkpoint_path = tempfile.mkdtemp()
        self._prepare_test_resource1(input_path)
        if not single_batch:
            time.sleep(2)
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

        stateful_processor = self.get_processor(stateful_processor_factory)
        if self.use_pandas():
            tws_df = df.groupBy("id").transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode=timeMode,
                initialState=initial_state,
            )
        else:
            tws_df = df.groupBy("id").transformWithState(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode=timeMode,
                initialState=initial_state,
            )

        q = (
            tws_df.writeStream.queryName("this_query")
            .option("checkpointLocation", checkpoint_path)
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, "this_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_basic(self):
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

        self._test_transform_with_state_basic(SimpleStatefulProcessorFactory(), check_results)

    def test_transform_with_state_non_exist_value_state(self):
        def check_results(batch_df, _):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="0"),
                Row(id="1", countAsString="0"),
            }

        self._test_transform_with_state_basic(
            InvalidSimpleStatefulProcessorFactory(), check_results, True
        )

    def test_transform_with_state_query_restarts(self):
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

        stateful_processor = self.get_processor(SimpleStatefulProcessorFactory())
        if self.use_pandas():
            tws_df = df.groupBy("id").transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
            )
        else:
            tws_df = df.groupBy("id").transformWithState(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
            )

        base_query = (
            tws_df.writeStream.queryName("this_query")
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

        # Verify custom metrics.
        self.assertTrue(q.lastProgress.stateOperators[0].customMetrics["numValueStateVars"] > 0)
        self.assertTrue(q.lastProgress.stateOperators[0].customMetrics["numDeletedStateVars"] > 0)

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

    def test_transform_with_state_list_state(self):
        def check_results(batch_df, _):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="2"),
                Row(id="1", countAsString="2"),
            }

        self._test_transform_with_state_basic(
            ListStateProcessorFactory(), check_results, True, "processingTime"
        )

    def test_transform_with_state_list_state_large_list(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                expected_prev_elements = ""
                expected_updated_elements = ",".join(map(lambda x: str(x), range(90)))
            else:
                # batch_id == 1:
                expected_prev_elements = ",".join(map(lambda x: str(x), range(90)))
                expected_updated_elements = ",".join(map(lambda x: str(x), range(180)))

            assert set(batch_df.sort("id").collect()) == {
                Row(
                    id="0",
                    prevElements=expected_prev_elements,
                    updatedElements=expected_updated_elements,
                ),
                Row(
                    id="1",
                    prevElements=expected_prev_elements,
                    updatedElements=expected_updated_elements,
                ),
            }

        input_path = tempfile.mkdtemp()
        checkpoint_path = tempfile.mkdtemp()

        self._prepare_test_resource1(input_path)
        time.sleep(2)
        self._prepare_test_resource2(input_path)

        df = self._build_test_df(input_path)

        for q in self.spark.streams.active:
            q.stop()
        self.assertTrue(df.isStreaming)

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("prevElements", StringType(), True),
                StructField("updatedElements", StringType(), True),
            ]
        )

        stateful_processor = self.get_processor(ListStateLargeListProcessorFactory())
        if self.use_pandas():
            tws_df = df.groupBy("id").transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="none",
            )
        else:
            tws_df = df.groupBy("id").transformWithState(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="none",
            )

        q = (
            tws_df.writeStream.queryName("this_query")
            .option("checkpointLocation", checkpoint_path)
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )
        self.assertEqual(q.name, "this_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    # test list state with ttl has the same behavior as list state when state doesn't expire.
    def test_transform_with_state_list_state_large_ttl(self):
        def check_results(batch_df, batch_id):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="2"),
                Row(id="1", countAsString="2"),
            }

        self._test_transform_with_state_basic(
            ListStateLargeTTLProcessorFactory(), check_results, True, "processingTime"
        )

    def test_transform_with_state_map_state(self):
        def check_results(batch_df, _):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="2"),
                Row(id="1", countAsString="2"),
            }

        self._test_transform_with_state_basic(MapStateProcessorFactory(), check_results, True)

    # test map state with ttl has the same behavior as map state when state doesn't expire.
    def test_transform_with_state_map_state_large_ttl(self):
        def check_results(batch_df, batch_id):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="2"),
                Row(id="1", countAsString="2"),
            }

        self._test_transform_with_state_basic(
            MapStateLargeTTLProcessorFactory(), check_results, True, "processingTime"
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

        self._test_transform_with_state_basic(
            SimpleTTLStatefulProcessorFactory(), check_results, False, "processingTime"
        )

    # TODO SPARK-50908 holistic fix for TTL suite
    @unittest.skip("test is flaky and it is only a timing issue, skipping until we can resolve")
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
            else:
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
                time.sleep(4)

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

            stateful_processor = self.get_processor(TTLStatefulProcessorFactory())
            if self.use_pandas():
                tws_df = df.groupBy("id").transformWithStateInPandas(
                    statefulProcessor=stateful_processor,
                    outputStructType=output_schema,
                    outputMode="Update",
                    timeMode="processingTime",
                )
            else:
                tws_df = df.groupBy("id").transformWithState(
                    statefulProcessor=stateful_processor,
                    outputStructType=output_schema,
                    outputMode="Update",
                    timeMode="processingTime",
                )

            q = tws_df.writeStream.foreachBatch(check_results).outputMode("update").start()
            self.assertTrue(q.isActive)
            q.processAllAvailable()
            q.stop()
            q.awaitTermination()
            self.assertTrue(q.exception() is None)
        finally:
            input_dir.cleanup()

    def _test_transform_with_state_proc_timer(self, stateful_processor_factory, check_results):
        input_path = tempfile.mkdtemp()
        self._prepare_test_resource3(input_path)
        time.sleep(2)
        self._prepare_test_resource1(input_path)
        time.sleep(2)
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
        stateful_processor = self.get_processor(stateful_processor_factory)
        if self.use_pandas():
            tws_df = df.groupBy("id").transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="processingtime",
            )
        else:
            tws_df = df.groupBy("id").transformWithState(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="processingtime",
            )

        q = (
            tws_df.writeStream.queryName(query_name)
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, query_name)
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_proc_timer(self):
        def check_results(batch_df, batch_id):
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
                check_timestamp(batch_df)

            else:
                assert set(batch_df.sort("id").select("id", "countAsString").collect()) == {
                    Row(id="0", countAsString="3"),
                    Row(id="0", countAsString="-1"),
                    Row(id="1", countAsString="5"),
                }

        self._test_transform_with_state_proc_timer(
            ProcTimeStatefulProcessorFactory(), check_results
        )

    def _test_transform_with_state_event_time(
        self, stateful_processor_factory, check_results, time_mode="eventtime"
    ):
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
                fw.write("a, 2\n")
                fw.write("a, 11\n")
                fw.write("a, 13\n")
                fw.write("a, 15\n")

        prepare_batch1(input_path)
        time.sleep(2)
        prepare_batch2(input_path)
        time.sleep(2)
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
        stateful_processor = self.get_processor(stateful_processor_factory)
        if self.use_pandas():
            tws_df = df.groupBy("id").transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode=time_mode,
            )
        else:
            tws_df = df.groupBy("id").transformWithState(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode=time_mode,
            )

        q = (
            tws_df.writeStream.queryName(query_name)
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, query_name)
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_event_time(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                # watermark for late event = 0
                # watermark for eviction = 0
                # timer is registered with expiration time = 0, hence expired at the same batch
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="a", timestamp="20"),
                    Row(id="a-expired", timestamp="0"),
                }
            elif batch_id == 1:
                # watermark for late event = 0
                # watermark for eviction = 10 (20 - 10)
                # timer is registered with expiration time = 10, hence expired at the same batch
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="a", timestamp="4"),
                    Row(id="a-expired", timestamp="10000"),
                }
            else:
                # watermark for late event = 10
                # watermark for eviction = 10 (unchanged as 4 < 10)
                # timer is registered with expiration time = 10, hence expired at the same batch
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="a", timestamp="15"),
                    Row(id="a-expired", timestamp="10000"),
                }

        self._test_transform_with_state_event_time(
            EventTimeStatefulProcessorFactory(), check_results
        )

    def test_transform_with_state_with_wmark_and_non_event_time(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                # watermark for late event = 0 and min event = 20
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="a", timestamp="20"),
                }
            elif batch_id == 1:
                # watermark for late event = 0 and min event = 4
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="a", timestamp="4"),
                }
            elif batch_id == 2:
                # watermark for late event = 10 and min event = 2 with no filtering
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="a", timestamp="2"),
                }
            else:
                for q in batch_df.sparkSession.streams.active:
                    q.stop()

        self._test_transform_with_state_event_time(
            MinEventTimeStatefulProcessorFactory(), check_results, "None"
        )

        self._test_transform_with_state_event_time(
            MinEventTimeStatefulProcessorFactory(), check_results, "ProcessingTime"
        )

    def _test_transform_with_state_init_state(
        self,
        stateful_processor_factory,
        check_results,
        time_mode="None",
        checkpoint_path=None,
        initial_state=None,
    ):
        input_path = tempfile.mkdtemp()
        if checkpoint_path is None:
            checkpoint_path = tempfile.mkdtemp()
        self._prepare_test_resource1(input_path)
        time.sleep(2)
        self._prepare_input_data(input_path + "/text-test2.txt", [0, 3], [67, 12])

        for q in self.spark.streams.active:
            q.stop()

        df = self._build_test_df(input_path)
        self.assertTrue(df.isStreaming)

        output_schema = "id string, value string"

        if initial_state is None:
            data = [("0", 789), ("3", 987)]
            initial_state = self.spark.createDataFrame(data, "id string, initVal int").groupBy("id")

        stateful_processor = self.get_processor(stateful_processor_factory)
        if self.use_pandas():
            tws_df = df.groupBy("id").transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode=time_mode,
                initialState=initial_state,
            )
        else:
            tws_df = df.groupBy("id").transformWithState(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode=time_mode,
                initialState=initial_state,
            )

        q = (
            tws_df.writeStream.queryName("this_query")
            .option("checkpointLocation", checkpoint_path)
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, "this_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_init_state(self):
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

        self._test_transform_with_state_init_state(
            SimpleStatefulProcessorWithInitialStateFactory(), check_results
        )

    def _test_transform_with_state_non_contiguous_grouping_cols(
        self, stateful_processor_factory, check_results, initial_state=None
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

        stateful_processor = self.get_processor(stateful_processor_factory)
        if self.use_pandas():
            tws_df = df.groupBy("id1", "id2").transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
                initialState=initial_state,
            )
        else:
            tws_df = df.groupBy("id1", "id2").transformWithState(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
                initialState=initial_state,
            )

        q = (
            tws_df.writeStream.queryName("this_query")
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
            assert set(batch_df.collect()) == {
                Row(id1="0", id2="1", value=str(123 + 46)),
                Row(id1="1", id2="2", value=str(146 + 346)),
            }

        self._test_transform_with_state_non_contiguous_grouping_cols(
            SimpleStatefulProcessorWithInitialStateFactory(), check_results
        )

    def test_transform_with_state_non_contiguous_grouping_cols_with_init_state(self):
        def check_results(batch_df, batch_id):
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
            SimpleStatefulProcessorWithInitialStateFactory(), check_results, initial_state
        )

    def _test_transform_with_state_chaining_ops(
        self,
        stateful_processor_factory,
        check_results,
        timeMode="None",
        grouping_cols=["outputTimestamp"],
    ):
        import pyspark.sql.functions as f

        input_path = tempfile.mkdtemp()
        self._prepare_input_data(input_path + "/text-test3.txt", ["a", "b"], [10, 15])
        time.sleep(2)
        self._prepare_input_data(input_path + "/text-test4.txt", ["a", "c"], [11, 25])
        time.sleep(2)
        self._prepare_input_data(input_path + "/text-test1.txt", ["a"], [5])

        df = self._build_test_df(input_path)
        df = df.select(
            "id", f.from_unixtime(f.col("temperature")).alias("eventTime").cast("timestamp")
        ).withWatermark("eventTime", "5 seconds")

        for q in self.spark.streams.active:
            q.stop()
        self.assertTrue(df.isStreaming)

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("outputTimestamp", TimestampType(), True),
            ]
        )

        stateful_processor = self.get_processor(stateful_processor_factory)
        if self.use_pandas():
            tws_df = df.groupBy("id").transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Append",
                timeMode=timeMode,
                eventTimeColumnName="outputTimestamp",
            )
        else:
            tws_df = df.groupBy("id").transformWithState(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Append",
                timeMode=timeMode,
                eventTimeColumnName="outputTimestamp",
            )

        q = (
            tws_df.groupBy(grouping_cols)
            .count()
            .writeStream.queryName("chaining_ops_query")
            .foreachBatch(check_results)
            .outputMode("append")
            .start()
        )

        self.assertEqual(q.name, "chaining_ops_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)

    def test_transform_with_state_chaining_ops(self):
        def check_results(batch_df, batch_id):
            import datetime

            if batch_id == 0:
                assert batch_df.isEmpty()
            elif batch_id == 1:
                # eviction watermark = 15 - 5 = 10 (max event time from batch 0),
                # late event watermark = 0 (eviction event time from batch 0)
                assert set(
                    batch_df.sort("outputTimestamp").select("outputTimestamp", "count").collect()
                ) == {
                    Row(outputTimestamp=datetime.datetime(1970, 1, 1, 0, 0, 10), count=1),
                }
            elif batch_id == 2:
                # eviction watermark = 25 - 5 = 20, late event watermark = 10;
                # row with watermark=5<10 is dropped so it does not show up in the results;
                # row with eventTime<=20 are finalized and emitted
                assert set(
                    batch_df.sort("outputTimestamp").select("outputTimestamp", "count").collect()
                ) == {
                    Row(outputTimestamp=datetime.datetime(1970, 1, 1, 0, 0, 11), count=1),
                    Row(outputTimestamp=datetime.datetime(1970, 1, 1, 0, 0, 15), count=1),
                }

        self._test_transform_with_state_chaining_ops(
            StatefulProcessorChainingOpsFactory(), check_results, "eventTime"
        )
        self._test_transform_with_state_chaining_ops(
            StatefulProcessorChainingOpsFactory(),
            check_results,
            "eventTime",
            ["outputTimestamp", "id"],
        )

    def test_transform_with_state_init_state_with_timers(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                # timers are registered and handled in the first batch for
                # rows in initial state; For key=0 and key=3 which contains
                # expired timers, both should be handled by handleExpiredTimers
                # regardless of whether key exists in the data rows or not
                expired_df = batch_df.filter(batch_df["id"].contains("expired"))
                data_df = batch_df.filter(~batch_df["id"].contains("expired"))
                assert set(expired_df.sort("id").select("id").collect()) == {
                    Row(id="0-expired"),
                    Row(id="3-expired"),
                }
                assert set(data_df.sort("id").collect()) == {
                    Row(id="0", value=str(789 + 123 + 46)),
                    Row(id="1", value=str(146 + 346)),
                }
            else:
                # handleInitialState is only processed in the first batch,
                # no more timer is registered so no more expired timers
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", value=str(789 + 123 + 46 + 67)),
                    Row(id="3", value=str(987 + 12)),
                }

        self._test_transform_with_state_init_state(
            StatefulProcessorWithInitialStateTimersFactory(), check_results, "processingTime"
        )

    def test_transform_with_state_batch_query(self):
        data = [("0", 123), ("0", 46), ("1", 146), ("1", 346)]
        df = self.spark.createDataFrame(data, "id string, temperature int")

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("countAsString", StringType(), True),
            ]
        )
        stateful_processor = self.get_processor(MapStateProcessorFactory())
        if self.use_pandas():
            batch_result = df.groupBy("id").transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
            )
        else:
            batch_result = df.groupBy("id").transformWithState(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
            )

        assert set(batch_result.sort("id").collect()) == {
            Row(id="0", countAsString="2"),
            Row(id="1", countAsString="2"),
        }

    def test_transform_with_state_batch_query_initial_state(self):
        data = [("0", 123), ("0", 46), ("1", 146), ("1", 346)]
        df = self.spark.createDataFrame(data, "id string, temperature int")

        init_data = [("0", 789), ("3", 987)]
        initial_state = self.spark.createDataFrame(init_data, "id string, initVal int").groupBy(
            "id"
        )

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )

        stateful_processor = self.get_processor(SimpleStatefulProcessorWithInitialStateFactory())
        if self.use_pandas():
            batch_result = df.groupBy("id").transformWithStateInPandas(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
                initialState=initial_state,
            )
        else:
            batch_result = df.groupBy("id").transformWithState(
                statefulProcessor=stateful_processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
                initialState=initial_state,
            )

        assert set(batch_result.sort("id").collect()) == {
            Row(id="0", value=str(789 + 123 + 46)),
            Row(id="1", value=str(146 + 346)),
        }

    # This test covers mapState with TTL, an empty state variable
    # and additional test against initial state python runner
    @unittest.skipIf(
        "COVERAGE_PROCESS_START" in os.environ, "Flaky with coverage enabled, skipping for now."
    )
    def test_transform_with_map_state_metadata(self):
        self._test_transform_with_map_state_metadata(None)

    def test_transform_with_map_state_metadata_with_init_state(self):
        # run the same test suite again but with no-op initial state
        # TWS with initial state is using a different python runner
        init_data = [("0", 789), ("3", 987)]
        initial_state = self.spark.createDataFrame(init_data, "id string, temperature int").groupBy(
            "id"
        )
        self._test_transform_with_map_state_metadata(initial_state)

    def _test_transform_with_map_state_metadata(self, initial_state):
        checkpoint_path = tempfile.mktemp()

        # This has to be outside of FEB to avoid serialization issues.
        if self.use_pandas():
            expected_operator_name = "transformWithStateInPandasExec"
        else:
            expected_operator_name = "transformWithStateInPySparkExec"

        def check_results(batch_df, batch_id):
            if batch_id == 0:
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", countAsString="2"),
                    Row(id="1", countAsString="2"),
                }
            else:
                # check for state metadata source
                metadata_df = batch_df.sparkSession.read.format("state-metadata").load(
                    checkpoint_path
                )

                assert set(
                    metadata_df.select(
                        "operatorId",
                        "operatorName",
                        "stateStoreName",
                        "numPartitions",
                        "minBatchId",
                        "maxBatchId",
                    ).collect()
                ) == {
                    Row(
                        operatorId=0,
                        operatorName=expected_operator_name,
                        stateStoreName="default",
                        numPartitions=5,
                        minBatchId=0,
                        maxBatchId=0,
                    )
                }
                operator_properties_json_obj = json.loads(
                    metadata_df.select("operatorProperties").collect()[0][0]
                )
                assert operator_properties_json_obj["timeMode"] == "ProcessingTime"
                assert operator_properties_json_obj["outputMode"] == "Update"

                state_var_list = operator_properties_json_obj["stateVariables"]
                assert len(state_var_list) == 3
                for state_var in state_var_list:
                    if state_var["stateName"] == "mapState":
                        assert state_var["stateVariableType"] == "MapState"
                        assert state_var["ttlEnabled"]
                    elif state_var["stateName"] == "listState":
                        assert state_var["stateVariableType"] == "ListState"
                        assert not state_var["ttlEnabled"]
                    else:
                        assert state_var["stateName"] == "$procTimers_keyToTimestamp"
                        assert state_var["stateVariableType"] == "TimerState"

                # check for state data source
                map_state_df = (
                    batch_df.sparkSession.read.format("statestore")
                    .option("path", checkpoint_path)
                    .option("stateVarName", "mapState")
                    .load()
                )
                assert map_state_df.selectExpr(
                    "key.id AS groupingKey",
                    "user_map_key.name AS mapKey",
                    "user_map_value.value.count AS mapValue",
                ).sort("groupingKey").collect() == [
                    Row(groupingKey="0", mapKey="key2", mapValue=2),
                    Row(groupingKey="1", mapKey="key2", mapValue=2),
                ]

                # check for map state with flatten option
                map_state_df_non_flatten = (
                    batch_df.sparkSession.read.format("statestore")
                    .option("path", checkpoint_path)
                    .option("stateVarName", "mapState")
                    .option("flattenCollectionTypes", False)
                    .load()
                )
                assert map_state_df_non_flatten.select(
                    "key.id", explode(col("map_value")).alias("map_key", "map_value")
                ).selectExpr(
                    "id AS groupingKey",
                    "map_key.name AS mapKey",
                    "map_value.value.count AS mapValue",
                ).sort(
                    "groupingKey"
                ).collect() == [
                    Row(groupingKey="0", mapKey="key2", mapValue=2),
                    Row(groupingKey="1", mapKey="key2", mapValue=2),
                ]

                ttl_df = map_state_df.selectExpr(
                    "user_map_value.ttlExpirationMs AS TTLVal"
                ).collect()
                # check if there are two rows containing TTL value in map state dataframe
                assert len(ttl_df) == 2
                # check if two rows are of the same TTL value
                assert len(set(ttl_df)) == 1

                list_state_df = (
                    batch_df.sparkSession.read.format("statestore")
                    .option("path", checkpoint_path)
                    .option("stateVarName", "listState")
                    .load()
                )
                assert list_state_df.isEmpty()

        self._test_transform_with_state_basic(
            MapStateLargeTTLProcessorFactory(),
            check_results,
            True,
            "processingTime",
            checkpoint_path=checkpoint_path,
            initial_state=initial_state,
        )

    # This test covers multiple list state variables and flatten option
    def test_transform_with_list_state_metadata(self):
        checkpoint_path = tempfile.mktemp()

        def check_results(batch_df, batch_id):
            if batch_id == 0:
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", countAsString="2"),
                    Row(id="1", countAsString="2"),
                }
            else:
                # check for state metadata source
                metadata_df = batch_df.sparkSession.read.format("state-metadata").load(
                    checkpoint_path
                )
                operator_properties_json_obj = json.loads(
                    metadata_df.select("operatorProperties").collect()[0][0]
                )
                state_var_list = operator_properties_json_obj["stateVariables"]
                assert len(state_var_list) == 4
                for state_var in state_var_list:
                    if state_var["stateName"] in ["listState1", "listState2", "listStateTimestamp"]:
                        assert state_var["stateVariableType"] == "ListState"
                    else:
                        assert state_var["stateName"] == "$procTimers_keyToTimestamp"
                        assert state_var["stateVariableType"] == "TimerState"

                # check for state data source and flatten option
                list_state_1_df = (
                    batch_df.sparkSession.read.format("statestore")
                    .option("path", checkpoint_path)
                    .option("stateVarName", "listState1")
                    .option("flattenCollectionTypes", True)
                    .load()
                )
                assert list_state_1_df.selectExpr(
                    "key.id AS groupingKey",
                    "list_element.temperature AS listElement",
                ).sort("groupingKey", "listElement").collect() == [
                    Row(groupingKey="0", listElement=20),
                    Row(groupingKey="0", listElement=20),
                    Row(groupingKey="0", listElement=111),
                    Row(groupingKey="0", listElement=120),
                    Row(groupingKey="0", listElement=120),
                    Row(groupingKey="1", listElement=20),
                    Row(groupingKey="1", listElement=20),
                    Row(groupingKey="1", listElement=111),
                    Row(groupingKey="1", listElement=120),
                    Row(groupingKey="1", listElement=120),
                ]

                list_state_2_df = (
                    batch_df.sparkSession.read.format("statestore")
                    .option("path", checkpoint_path)
                    .option("stateVarName", "listState2")
                    .option("flattenCollectionTypes", False)
                    .load()
                )
                assert list_state_2_df.selectExpr(
                    "key.id AS groupingKey", "list_value.temperature AS valueList"
                ).sort("groupingKey").withColumn(
                    "valueSortedList", array_sort(col("valueList"))
                ).select(
                    "groupingKey", "valueSortedList"
                ).collect() == [
                    Row(groupingKey="0", valueSortedList=[20, 20, 120, 120, 222]),
                    Row(groupingKey="1", valueSortedList=[20, 20, 120, 120, 222]),
                ]

        self._test_transform_with_state_basic(
            ListStateProcessorFactory(),
            check_results,
            False,
            "processingTime",
            checkpoint_path=checkpoint_path,
            initial_state=None,
        )

    # This test covers value state variable and read change feed,
    # snapshotStartBatchId related options
    def test_transform_with_value_state_metadata(self):
        checkpoint_path = tempfile.mktemp()

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

                # check for state metadata source
                metadata_df = batch_df.sparkSession.read.format("state-metadata").load(
                    checkpoint_path
                )
                operator_properties_json_obj = json.loads(
                    metadata_df.select("operatorProperties").collect()[0][0]
                )
                state_var_list = operator_properties_json_obj["stateVariables"]

                assert len(state_var_list) == 3
                for state_var in state_var_list:
                    if state_var["stateName"] in ["numViolations", "tempState"]:
                        state_var["stateVariableType"] == "ValueState"
                    else:
                        assert state_var["stateName"] == "$procTimers_keyToTimestamp"
                        assert state_var["stateVariableType"] == "TimerState"

                # check for state data source and readChangeFeed
                value_state_df = (
                    batch_df.sparkSession.read.format("statestore")
                    .option("path", checkpoint_path)
                    .option("stateVarName", "numViolations")
                    .option("readChangeFeed", True)
                    .option("changeStartBatchId", 0)
                    .load()
                ).selectExpr(
                    "change_type", "key.id AS groupingKey", "value.value AS value", "partition_id"
                )

                assert value_state_df.select("change_type", "groupingKey", "value").sort(
                    "groupingKey"
                ).collect() == [
                    Row(change_type="update", groupingKey="0", value=1),
                    Row(change_type="update", groupingKey="1", value=2),
                ]

                partition_id_list = [
                    row["partition_id"] for row in value_state_df.select("partition_id").collect()
                ]

                for partition_id in partition_id_list:
                    # check for state data source and snapshotStartBatchId options
                    state_snapshot_df = (
                        batch_df.sparkSession.read.format("statestore")
                        .option("path", checkpoint_path)
                        .option("stateVarName", "numViolations")
                        .option("snapshotPartitionId", partition_id)
                        .option("snapshotStartBatchId", 0)
                        .load()
                    )

                    assert (
                        value_state_df.select("partition_id", "groupingKey", "value")
                        .filter(value_state_df["partition_id"] == partition_id)
                        .sort("groupingKey")
                        .collect()
                        == state_snapshot_df.selectExpr(
                            "partition_id", "key.id AS groupingKey", "value.value AS value"
                        )
                        .sort("groupingKey")
                        .collect()
                    )

        with self.sql_conf(
            {"spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled": "true"}
        ):
            self._test_transform_with_state_basic(
                SimpleStatefulProcessorFactory(),
                check_results,
                False,
                "processingTime",
                checkpoint_path=checkpoint_path,
            )

    def test_transform_with_state_restart_with_multiple_rows_init_state(self):
        def check_results(batch_df, _):
            assert set(batch_df.sort("id").collect()) == {
                Row(id="0", countAsString="2"),
                Row(id="1", countAsString="2"),
            }

        def check_results_for_new_query(batch_df, batch_id):
            if batch_id == 0:
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", value=str(123 + 46)),
                    Row(id="1", value=str(146 + 346)),
                }
            else:
                assert set(batch_df.sort("id").collect()) == {
                    Row(id="0", value=str(123 + 46 + 67)),
                    Row(id="3", value=str(12)),
                }
                # verify values in initial state is appended into list state for all keys
                df = (
                    batch_df.sparkSession.read.format("statestore")
                    .option("path", new_checkpoint_path)
                    .option("stateVarName", "list_state")
                    .load()
                ).selectExpr("key.id AS id", "list_element.value AS value")

                def dataframe_to_value_list(output_df):
                    return [
                        row["value"] for row in output_df.sort("value").select("value").collect()
                    ]

                assert dataframe_to_value_list(df.filter(df.id == "0")) == [20, 20, 111, 120, 120]
                assert dataframe_to_value_list(df.filter(df.id == "1")) == [20, 20, 111, 120, 120]

        # run a tws query and read state data source dataframe from its checkpoint
        checkpoint_path = tempfile.mkdtemp()
        self._test_transform_with_state_basic(
            ListStateProcessorFactory(), check_results, True, checkpoint_path=checkpoint_path
        )
        list_state_df = (
            self.spark.read.format("statestore")
            .option("path", checkpoint_path)
            .option("stateVarName", "listState1")
            .load()
        ).selectExpr("key.id AS id", "list_element.temperature AS initVal")
        init_df = list_state_df.groupBy("id")

        # run a new tws query and pass state data source dataframe as initial state
        # multiple rows exist in the initial state with the same grouping key
        new_checkpoint_path = tempfile.mkdtemp()
        self._test_transform_with_state_init_state(
            StatefulProcessorWithListStateInitialStateFactory(),
            check_results_for_new_query,
            checkpoint_path=new_checkpoint_path,
            initial_state=init_df,
        )

    # run the same test suites again but with single shuffle partition
    def test_transform_with_state_with_timers_single_partition(self):
        with self.sql_conf({"spark.sql.shuffle.partitions": "1"}):
            self.test_transform_with_state_init_state_with_timers()
            self.test_transform_with_state_event_time()
            self.test_transform_with_state_proc_timer()
            self.test_transform_with_state_restart_with_multiple_rows_init_state()

    def _run_evolution_test(self, processor_factory, checkpoint_dir, check_results, df):
        processor = self.get_processor(processor_factory)
        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("value", processor.state_schema, True),
            ]
        )

        # Stop any active streams first
        for q in self.spark.streams.active:
            q.stop()

        if self.use_pandas():
            tws_df = df.groupBy("id").transformWithStateInPandas(
                statefulProcessor=processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
            )
        else:
            tws_df = df.groupBy("id").transformWithState(
                statefulProcessor=processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
            )

        q = (
            tws_df.writeStream.queryName("evolution_test")
            .option("checkpointLocation", checkpoint_dir)
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, "evolution_test")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.awaitTermination(10)

    def test_schema_evolution_scenarios(self):
        """Test various schema evolution scenarios"""
        with self.sql_conf({"spark.sql.streaming.stateStore.encodingFormat": "avro"}):
            with tempfile.TemporaryDirectory() as checkpoint_dir:
                # Test 1: Basic state

                input_path = tempfile.mkdtemp()
                self._prepare_test_resource1(input_path)

                df = self._build_test_df(input_path)

                def check_basic_state(batch_df, batch_id):
                    result = batch_df.collect()[0]
                    assert result.value["id"] == 0  # First ID from test data
                    assert result.value["name"] == "name-0"

                self._run_evolution_test(
                    BasicProcessorFactory(), checkpoint_dir, check_basic_state, df
                )

                self._prepare_test_resource2(input_path)

                # Test 2: Add fields
                def check_add_fields(batch_df, batch_id):
                    result = batch_df.collect()[0]
                    # Check default values for existing key
                    assert result.value["id"] == 0
                    assert result.value["count"] is None
                    assert result.value["active"] is None
                    assert result.value["score"] is None

                self._run_evolution_test(
                    AddFieldsProcessorFactory(), checkpoint_dir, check_add_fields, df
                )
                self._prepare_test_resource3(input_path)

                # Test 3: Remove fields
                def check_remove_fields(batch_df, batch_id):
                    result = batch_df.collect()[0]
                    assert result.value["id"] == 0  # First ID from test data
                    assert result.value["name"] == "name-00"

                self._run_evolution_test(
                    RemoveFieldsProcessorFactory(), checkpoint_dir, check_remove_fields, df
                )
                self._prepare_test_resource4(input_path)

                # Test 4: Reorder fields
                def check_reorder_fields(batch_df, batch_id):
                    result = batch_df.collect()[0]
                    assert result.value["name"] == "name-00"
                    assert result.value["id"] == 0

                self._run_evolution_test(
                    ReorderedFieldsProcessorFactory(), checkpoint_dir, check_reorder_fields, df
                )
                self._prepare_test_resource5(input_path)

                # Test 5: Upcast type
                def check_upcast(batch_df, batch_id):
                    result = batch_df.collect()[0]
                    assert result.value["id"] == 1
                    assert result.value["name"] == "name-0"

                self._run_evolution_test(UpcastProcessorFactory(), checkpoint_dir, check_upcast, df)

    # This test case verifies that an exception is thrown when downcasting, which violates
    # Avro's schema evolution rules
    def test_schema_evolution_fails(self):
        with self.sql_conf({"spark.sql.streaming.stateStore.encodingFormat": "avro"}):
            with tempfile.TemporaryDirectory() as checkpoint_dir:
                input_path = tempfile.mkdtemp()
                self._prepare_test_resource1(input_path)

                df = self._build_test_df(input_path)

                def check_add_fields(batch_df, batch_id):
                    results = batch_df.collect()
                    assert results[0].value["count"] == 100
                    assert results[0].value["active"]

                self._run_evolution_test(
                    AddFieldsProcessorFactory(), checkpoint_dir, check_add_fields, df
                )
                self._prepare_test_resource2(input_path)

                def check_upcast(batch_df, batch_id):
                    result = batch_df.collect()[0]
                    assert result.value["name"] == "name-0"

                # Long
                self._run_evolution_test(UpcastProcessorFactory(), checkpoint_dir, check_upcast, df)
                self._prepare_test_resource3(input_path)

                def check_basic_state(batch_df, batch_id):
                    result = batch_df.collect()[0]
                    assert result.value["id"] == 0  # First ID from test data
                    assert result.value["name"] == "name-0"

                # Int
                try:
                    self._run_evolution_test(
                        BasicProcessorFactory(),
                        checkpoint_dir,
                        check_basic_state,
                        df,
                    )
                except Exception as e:
                    # we are expecting an exception, verify it's the right one
                    from pyspark.errors.exceptions.captured import StreamingQueryException

                    if not isinstance(e, StreamingQueryException):
                        return False
                    error_msg = str(e)
                    assert (
                        "[STREAM_FAILED]" in error_msg
                        and "[STATE_STORE_INVALID_VALUE_SCHEMA_EVOLUTION]" in error_msg
                        and "Schema evolution is not possible" in error_msg
                    )

    def test_not_nullable_fails(self):
        with self.sql_conf({"spark.sql.streaming.stateStore.encodingFormat": "avro"}):
            with tempfile.TemporaryDirectory() as checkpoint_dir:
                input_path = tempfile.mkdtemp()
                self._prepare_test_resource1(input_path)

                df = self._build_test_df(input_path)

                def check_basic_state(batch_df, batch_id):
                    result = batch_df.collect()[0]
                    assert result.value["id"] == 0  # First ID from test data
                    assert result.value["name"] == "name-0"

                try:
                    self._run_evolution_test(
                        BasicProcessorNotNullableFactory(),
                        checkpoint_dir,
                        check_basic_state,
                        df,
                    )
                except Exception as e:
                    # we are expecting an exception, verify it's the right one
                    from pyspark.errors.exceptions.captured import StreamingQueryException

                    if not isinstance(e, StreamingQueryException):
                        return False
                    error_msg = str(e)
                    assert (
                        "[TRANSFORM_WITH_STATE_SCHEMA_MUST_BE_NULLABLE]" in error_msg
                        and "column family state must be nullable" in error_msg
                    )


@unittest.skipIf(
    not have_pyarrow or os.environ.get("PYTHON_GIL", "?") == "0",
    cast(str, pyarrow_requirement_message or "Not supported in no-GIL mode"),
)
class TransformWithStateInPySparkTestsMixin(TransformWithStateTestsMixin):
    @classmethod
    def use_pandas(cls) -> bool:
        return False

    @classmethod
    def conf(cls):
        cfg = SparkConf()
        cfg.set("spark.sql.shuffle.partitions", "5")
        cfg.set(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        cfg.set("spark.sql.execution.arrow.transformWithStateInPandas.maxRecordsPerBatch", "2")
        cfg.set("spark.sql.session.timeZone", "UTC")
        # TODO SPARK-49046 this config is to stop query from FEB sink gracefully
        cfg.set("spark.sql.streaming.noDataMicroBatches.enabled", "false")
        return cfg


@unittest.skipIf(
    not have_pandas or not have_pyarrow or os.environ.get("PYTHON_GIL", "?") == "0",
    cast(
        str,
        pandas_requirement_message or pyarrow_requirement_message or "Not supported in no-GIL mode",
    ),
)
class TransformWithStateInPandasTestsMixin(TransformWithStateTestsMixin):
    @classmethod
    def use_pandas(cls) -> bool:
        return True

    @classmethod
    def conf(cls):
        cfg = SparkConf()
        cfg.set("spark.sql.shuffle.partitions", "5")
        cfg.set(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        cfg.set("spark.sql.execution.arrow.transformWithStateInPandas.maxRecordsPerBatch", "2")
        cfg.set("spark.sql.session.timeZone", "UTC")
        # TODO SPARK-49046 this config is to stop query from FEB sink gracefully
        cfg.set("spark.sql.streaming.noDataMicroBatches.enabled", "false")
        return cfg


class TransformWithStateInPandasTests(TransformWithStateInPandasTestsMixin, ReusedSQLTestCase):
    pass


class TransformWithStateInPySparkTests(TransformWithStateInPySparkTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_transform_with_state import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
