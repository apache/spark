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
from pyspark.sql.functions import split
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    Row,
    IntegerType,
    TimestampType,
    DecimalType,
    ArrayType,
    MapType,
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
    SimpleStatefulProcessorWithInitialStateFactory,
    EventTimeStatefulProcessorFactory,
    ProcTimeStatefulProcessorFactory,
    SimpleStatefulProcessorFactory,
    StatefulProcessorChainingOpsFactory,
    MapStateProcessorFactory,
    LargeValueStatefulProcessorFactory,
    BasicProcessorFactory,
    BasicProcessorNotNullableFactory,
    AddFieldsProcessorFactory,
    RemoveFieldsProcessorFactory,
    ReorderedFieldsProcessorFactory,
    UpcastProcessorFactory,
    MinEventTimeStatefulProcessorFactory,
    StatefulProcessorCompositeTypeFactory,
    ChunkCountProcessorFactory,
    ChunkCountProcessorWithInitialStateFactory,
    CompositeOutputProcessorFactory,
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
        output_schema=StructType(
            [
                StructField("id", StringType(), True),
                StructField("countAsString", StringType(), True),
            ]
        ),
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
        q.stop()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_basic(self):
        def check_results(batch_df, batch_id):
            batch_df.collect()
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
        q.stop()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

        # Verify custom metrics.
        self.assertTrue(q.lastProgress.stateOperators[0].customMetrics["numValueStateVars"] > 0)
        self.assertTrue(q.lastProgress.stateOperators[0].customMetrics["numDeletedStateVars"] > 0)

        self._prepare_test_resource2(input_path)

        q = base_query.start()
        self.assertEqual(q.name, "this_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        q.stop()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)
        result_df = self.spark.read.parquet(output_path)
        assert set(result_df.sort("id").collect()) == {
            Row(id="0", countAsString="2"),
            Row(id="0", countAsString="3"),
            Row(id="1", countAsString="2"),
            Row(id="1", countAsString="2"),
        }

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
        q.stop()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_proc_timer(self):
        def check_results(batch_df, batch_id):
            batch_df.collect()

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
        q.stop()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_event_time(self):
        def check_results(batch_df, batch_id):
            batch_df.collect()
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
            batch_df.collect()
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
        q.stop()
        q.awaitTermination(10)
        self.assertTrue(q.exception() is None)

    def test_transform_with_state_non_contiguous_grouping_cols(self):
        def check_results(batch_df, batch_id):
            batch_df.collect()
            assert set(batch_df.collect()) == {
                Row(id1="0", id2="1", value=str(123 + 46)),
                Row(id1="1", id2="2", value=str(146 + 346)),
            }

        self._test_transform_with_state_non_contiguous_grouping_cols(
            SimpleStatefulProcessorWithInitialStateFactory(), check_results
        )

    def test_transform_with_state_non_contiguous_grouping_cols_with_init_state(self):
        def check_results(batch_df, batch_id):
            batch_df.collect()
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
        q.stop()
        q.awaitTermination(10)

    def test_transform_with_state_chaining_ops(self):
        def check_results(batch_df, batch_id):
            batch_df.collect()
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

    def test_transform_with_state_in_pandas_composite_type(self):
        def check_results(batch_df, batch_id):
            if batch_id == 0:
                map_val = {"key1": [1], "key2": [10]}
                nested_map_val = {"e1": {"e2": 5, "e3": 10}}
                assert set(batch_df.sort("id").collect()) == {
                    Row(
                        id="0",
                        value_arr="0",
                        list_state_arr="0",
                        map_state_arr=json.dumps(map_val, sort_keys=True),
                        nested_map_state_arr=json.dumps(nested_map_val, sort_keys=True),
                    ),
                    Row(
                        id="1",
                        value_arr="0",
                        list_state_arr="0",
                        map_state_arr=json.dumps(map_val, sort_keys=True),
                        nested_map_state_arr=json.dumps(nested_map_val, sort_keys=True),
                    ),
                }, f"batch id: {batch_id}, real df is: {batch_df.collect()}"
            else:
                map_val_0 = {"key1": [1], "key2": [10], "0": [669]}
                map_val_1 = {"key1": [1], "key2": [10], "1": [252]}
                nested_map_val_0 = {"e1": {"e2": 5, "e3": 10, "0": 669}}
                nested_map_val_1 = {"e1": {"e2": 5, "e3": 10, "1": 252}}
                assert set(batch_df.sort("id").collect()) == {
                    Row(
                        id="0",
                        countAsString="669",
                        list_state_arr="0,669",
                        map_state_arr=json.dumps(map_val_0, sort_keys=True),
                        nested_map_state_arr=json.dumps(nested_map_val_0, sort_keys=True),
                    ),
                    Row(
                        id="1",
                        countAsString="252",
                        list_state_arr="0,252",
                        map_state_arr=json.dumps(map_val_1, sort_keys=True),
                        nested_map_state_arr=json.dumps(nested_map_val_1, sort_keys=True),
                    ),
                }, f"batch id: {batch_id}, real df is: {batch_df.collect()}"

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("value_arr", StringType(), True),
                StructField("list_state_arr", StringType(), True),
                StructField("map_state_arr", StringType(), True),
                StructField("nested_map_state_arr", StringType(), True),
            ]
        )

        self._test_transform_with_state_basic(
            StatefulProcessorCompositeTypeFactory(), check_results, output_schema=output_schema
        )

    # run a test with composite types where the output of TWS (not just the states) are complex.
    def test_composite_output_schema(self):
        def check_results(batch_df, batch_id):
            batch_df.collect()
            # Cannot use set() wrapper because Row objects contain unhashable types (lists)
            if batch_id == 0:
                assert batch_df.sort("primitiveValue").collect() == [
                    Row(
                        primitiveValue="key_0_count_2",
                        listOfPrimitive=["item_0", "item_1"],
                        mapOfPrimitive={"key0": "value0", "key1": "value1"},
                        listOfComposite=[
                            Row(
                                intValue=0,
                                doubleValue=0.0,
                                arrayValue=["elem_0_0"],
                                mapValue={"map_0_0": "val_0_0"},
                            ),
                            Row(
                                intValue=1,
                                doubleValue=1.5,
                                arrayValue=["elem_1_0", "elem_1_1"],
                                mapValue={"map_1_0": "val_1_0", "map_1_1": "val_1_1"},
                            ),
                        ],
                        mapOfComposite={
                            "nested_key0": Row(
                                intValue=0,
                                doubleValue=0.0,
                                arrayValue=["elem_0_0"],
                                mapValue={"map_0_0": "val_0_0"},
                            ),
                            "nested_key1": Row(
                                intValue=10,
                                doubleValue=2.5,
                                arrayValue=["elem_1_0", "elem_1_1"],
                                mapValue={"map_1_0": "val_1_0", "map_1_1": "val_1_1"},
                            ),
                        },
                    ),
                    Row(
                        primitiveValue="key_1_count_2",
                        listOfPrimitive=["item_0", "item_1"],
                        mapOfPrimitive={"key0": "value0", "key1": "value1"},
                        listOfComposite=[
                            Row(
                                intValue=0,
                                doubleValue=0.0,
                                arrayValue=["elem_0_0"],
                                mapValue={"map_0_0": "val_0_0"},
                            ),
                            Row(
                                intValue=1,
                                doubleValue=1.5,
                                arrayValue=["elem_1_0", "elem_1_1"],
                                mapValue={"map_1_0": "val_1_0", "map_1_1": "val_1_1"},
                            ),
                        ],
                        mapOfComposite={
                            "nested_key0": Row(
                                intValue=0,
                                doubleValue=0.0,
                                arrayValue=["elem_0_0"],
                                mapValue={"map_0_0": "val_0_0"},
                            ),
                            "nested_key1": Row(
                                intValue=10,
                                doubleValue=2.5,
                                arrayValue=["elem_1_0", "elem_1_1"],
                                mapValue={"map_1_0": "val_1_0", "map_1_1": "val_1_1"},
                            ),
                        },
                    ),
                ]
            else:
                assert batch_df.sort("primitiveValue").collect() == [
                    Row(
                        primitiveValue="key_0_count_5",
                        listOfPrimitive=["item_0", "item_1", "item_2", "item_3", "item_4"],
                        mapOfPrimitive={
                            "key0": "value0",
                            "key1": "value1",
                            "key2": "value2",
                            "key3": "value3",
                            "key4": "value4",
                        },
                        listOfComposite=[
                            Row(
                                intValue=0,
                                doubleValue=0.0,
                                arrayValue=["elem_0_0"],
                                mapValue={"map_0_0": "val_0_0"},
                            ),
                            Row(
                                intValue=1,
                                doubleValue=1.5,
                                arrayValue=["elem_1_0", "elem_1_1"],
                                mapValue={"map_1_0": "val_1_0", "map_1_1": "val_1_1"},
                            ),
                            Row(
                                intValue=2,
                                doubleValue=3.0,
                                arrayValue=["elem_2_0", "elem_2_1", "elem_2_2"],
                                mapValue={
                                    "map_2_0": "val_2_0",
                                    "map_2_1": "val_2_1",
                                    "map_2_2": "val_2_2",
                                },
                            ),
                            Row(
                                intValue=3,
                                doubleValue=4.5,
                                arrayValue=["elem_3_0", "elem_3_1", "elem_3_2", "elem_3_3"],
                                mapValue={
                                    "map_3_0": "val_3_0",
                                    "map_3_1": "val_3_1",
                                    "map_3_2": "val_3_2",
                                    "map_3_3": "val_3_3",
                                },
                            ),
                            Row(
                                intValue=4,
                                doubleValue=6.0,
                                arrayValue=[
                                    "elem_4_0",
                                    "elem_4_1",
                                    "elem_4_2",
                                    "elem_4_3",
                                    "elem_4_4",
                                ],
                                mapValue={
                                    "map_4_0": "val_4_0",
                                    "map_4_1": "val_4_1",
                                    "map_4_2": "val_4_2",
                                    "map_4_3": "val_4_3",
                                    "map_4_4": "val_4_4",
                                },
                            ),
                        ],
                        mapOfComposite={
                            "nested_key0": Row(
                                intValue=0,
                                doubleValue=0.0,
                                arrayValue=["elem_0_0"],
                                mapValue={"map_0_0": "val_0_0"},
                            ),
                            "nested_key1": Row(
                                intValue=10,
                                doubleValue=2.5,
                                arrayValue=["elem_1_0", "elem_1_1"],
                                mapValue={"map_1_0": "val_1_0", "map_1_1": "val_1_1"},
                            ),
                            "nested_key2": Row(
                                intValue=20,
                                doubleValue=5.0,
                                arrayValue=["elem_2_0", "elem_2_1", "elem_2_2"],
                                mapValue={
                                    "map_2_0": "val_2_0",
                                    "map_2_1": "val_2_1",
                                    "map_2_2": "val_2_2",
                                },
                            ),
                            "nested_key3": Row(
                                intValue=30,
                                doubleValue=7.5,
                                arrayValue=["elem_3_0", "elem_3_1", "elem_3_2", "elem_3_3"],
                                mapValue={
                                    "map_3_0": "val_3_0",
                                    "map_3_1": "val_3_1",
                                    "map_3_2": "val_3_2",
                                    "map_3_3": "val_3_3",
                                },
                            ),
                            "nested_key4": Row(
                                intValue=40,
                                doubleValue=10.0,
                                arrayValue=[
                                    "elem_4_0",
                                    "elem_4_1",
                                    "elem_4_2",
                                    "elem_4_3",
                                    "elem_4_4",
                                ],
                                mapValue={
                                    "map_4_0": "val_4_0",
                                    "map_4_1": "val_4_1",
                                    "map_4_2": "val_4_2",
                                    "map_4_3": "val_4_3",
                                    "map_4_4": "val_4_4",
                                },
                            ),
                        },
                    ),
                    Row(
                        primitiveValue="key_1_count_4",
                        listOfPrimitive=["item_0", "item_1", "item_2", "item_3"],
                        mapOfPrimitive={
                            "key0": "value0",
                            "key1": "value1",
                            "key2": "value2",
                            "key3": "value3",
                        },
                        listOfComposite=[
                            Row(
                                intValue=0,
                                doubleValue=0.0,
                                arrayValue=["elem_0_0"],
                                mapValue={"map_0_0": "val_0_0"},
                            ),
                            Row(
                                intValue=1,
                                doubleValue=1.5,
                                arrayValue=["elem_1_0", "elem_1_1"],
                                mapValue={"map_1_0": "val_1_0", "map_1_1": "val_1_1"},
                            ),
                            Row(
                                intValue=2,
                                doubleValue=3.0,
                                arrayValue=["elem_2_0", "elem_2_1", "elem_2_2"],
                                mapValue={
                                    "map_2_0": "val_2_0",
                                    "map_2_1": "val_2_1",
                                    "map_2_2": "val_2_2",
                                },
                            ),
                            Row(
                                intValue=3,
                                doubleValue=4.5,
                                arrayValue=["elem_3_0", "elem_3_1", "elem_3_2", "elem_3_3"],
                                mapValue={
                                    "map_3_0": "val_3_0",
                                    "map_3_1": "val_3_1",
                                    "map_3_2": "val_3_2",
                                    "map_3_3": "val_3_3",
                                },
                            ),
                        ],
                        mapOfComposite={
                            "nested_key0": Row(
                                intValue=0,
                                doubleValue=0.0,
                                arrayValue=["elem_0_0"],
                                mapValue={"map_0_0": "val_0_0"},
                            ),
                            "nested_key1": Row(
                                intValue=10,
                                doubleValue=2.5,
                                arrayValue=["elem_1_0", "elem_1_1"],
                                mapValue={"map_1_0": "val_1_0", "map_1_1": "val_1_1"},
                            ),
                            "nested_key2": Row(
                                intValue=20,
                                doubleValue=5.0,
                                arrayValue=["elem_2_0", "elem_2_1", "elem_2_2"],
                                mapValue={
                                    "map_2_0": "val_2_0",
                                    "map_2_1": "val_2_1",
                                    "map_2_2": "val_2_2",
                                },
                            ),
                            "nested_key3": Row(
                                intValue=30,
                                doubleValue=7.5,
                                arrayValue=["elem_3_0", "elem_3_1", "elem_3_2", "elem_3_3"],
                                mapValue={
                                    "map_3_0": "val_3_0",
                                    "map_3_1": "val_3_1",
                                    "map_3_2": "val_3_2",
                                    "map_3_3": "val_3_3",
                                },
                            ),
                        },
                    ),
                ]

        # Define the output schema with inner nested class schema
        inner_nested_class_schema = StructType(
            [
                StructField("intValue", IntegerType(), True),
                StructField("doubleValue", DoubleType(), True),
                StructField("arrayValue", ArrayType(StringType()), True),
                StructField("mapValue", MapType(StringType(), StringType()), True),
            ]
        )

        output_schema = StructType(
            [
                StructField("primitiveValue", StringType(), True),
                StructField("listOfPrimitive", ArrayType(StringType()), True),
                StructField("mapOfPrimitive", MapType(StringType(), StringType()), True),
                StructField("listOfComposite", ArrayType(inner_nested_class_schema), True),
                StructField(
                    "mapOfComposite", MapType(StringType(), inner_nested_class_schema), True
                ),
            ]
        )

        self._test_transform_with_state_basic(
            CompositeOutputProcessorFactory(), check_results, output_schema=output_schema
        )

    # run the same test suites again but with single shuffle partition
    def test_transform_with_state_with_timers_single_partition(self):
        with self.sql_conf({"spark.sql.shuffle.partitions": "1"}):
            self.test_transform_with_state_event_time()
            self.test_transform_with_state_proc_timer()

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
        q.stop()
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

    def test_transform_with_state_int_to_decimal_coercion(self):
        if not self.use_pandas():
            return

        class IntToDecimalProcessor(StatefulProcessor):
            def init(self, handle):
                count_schema = StructType([StructField("value", IntegerType(), True)])
                self.count_state = handle.getValueState("count", count_schema)

            def handleInputRows(self, key, rows, timerValues):
                if self.count_state.exists():
                    count = self.count_state.get()[0]
                else:
                    count = 0
                count += len(list(rows))
                self.count_state.update((count,))

                import pandas as pd

                yield pd.DataFrame(
                    {"id": [key[0]], "decimal_result": [12345]}  # Integer to be coerced to decimal
                )

            def close(self):
                pass

        data = [("1", "a"), ("1", "b"), ("2", "c")]
        df = self.spark.createDataFrame(data, ["id", "value"])

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("decimal_result", DecimalType(10, 2), True),
            ]
        )

        with self.sql_conf(
            {"spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled": True}
        ):
            result = (
                df.groupBy("id")
                .transformWithStateInPandas(
                    statefulProcessor=IntToDecimalProcessor(),
                    outputStructType=output_schema,
                    outputMode="Update",
                    timeMode="None",
                )
                .collect()
            )
            self.assertTrue(len(result) > 0)

        with self.sql_conf(
            {"spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled": False}
        ):
            with self.assertRaisesRegex(
                Exception, "Exception thrown when converting pandas.Series"
            ):
                (
                    df.groupBy("id")
                    .transformWithStateInPandas(
                        statefulProcessor=IntToDecimalProcessor(),
                        outputStructType=output_schema,
                        outputMode="Update",
                        timeMode="None",
                    )
                    .collect()
                )

    def test_transform_with_state_with_bytes_limit(self):
        if not self.use_pandas():
            return

        def make_check_results(expected_per_batch):
            def check_results(batch_df, batch_id):
                batch_df.collect()
                if batch_id == 0:
                    assert set(batch_df.sort("id").collect()) == expected_per_batch[0]
                else:
                    assert set(batch_df.sort("id").collect()) == expected_per_batch[1]

            return check_results

        result_with_small_limit = [
            {
                Row(id="0", chunkCount=2),
                Row(id="1", chunkCount=2),
            },
            {
                Row(id="0", chunkCount=3),
                Row(id="1", chunkCount=2),
            },
        ]

        result_with_large_limit = [
            {
                Row(id="0", chunkCount=1),
                Row(id="1", chunkCount=1),
            },
            {
                Row(id="0", chunkCount=1),
                Row(id="1", chunkCount=1),
            },
        ]

        data = [("0", 789), ("3", 987)]
        initial_state = self.spark.createDataFrame(data, "id string, initVal int").groupBy("id")

        with self.sql_conf(
            # Set it to a very small number so that every row would be a separate pandas df
            {"spark.sql.execution.arrow.maxBytesPerBatch": "2"}
        ):
            self._test_transform_with_state_basic(
                ChunkCountProcessorFactory(),
                make_check_results(result_with_small_limit),
                output_schema=StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("chunkCount", IntegerType(), True),
                    ]
                ),
            )

            self._test_transform_with_state_basic(
                ChunkCountProcessorWithInitialStateFactory(),
                make_check_results(result_with_small_limit),
                initial_state=initial_state,
                output_schema=StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("chunkCount", IntegerType(), True),
                    ]
                ),
            )

        with self.sql_conf(
            # Set it to a very large number so that every row would be in the same pandas df
            {"spark.sql.execution.arrow.maxBytesPerBatch": "100000"}
        ):
            self._test_transform_with_state_basic(
                ChunkCountProcessorFactory(),
                make_check_results(result_with_large_limit),
                output_schema=StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("chunkCount", IntegerType(), True),
                    ]
                ),
            )

            self._test_transform_with_state_basic(
                ChunkCountProcessorWithInitialStateFactory(),
                make_check_results(result_with_large_limit),
                initial_state=initial_state,
                output_schema=StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("chunkCount", IntegerType(), True),
                    ]
                ),
            )

    def test_transform_with_state_with_records_limit(self):
        if not self.use_pandas():
            return

        def make_check_results(expected_per_batch):
            def check_results(batch_df, batch_id):
                batch_df.collect()
                if batch_id == 0:
                    assert set(batch_df.sort("id").collect()) == expected_per_batch[0]
                else:
                    assert set(batch_df.sort("id").collect()) == expected_per_batch[1]

            return check_results

        result_with_small_limit = [
            {
                Row(id="0", chunkCount=2),
                Row(id="1", chunkCount=2),
            },
            {
                Row(id="0", chunkCount=3),
                Row(id="1", chunkCount=2),
            },
        ]

        result_with_large_limit = [
            {
                Row(id="0", chunkCount=1),
                Row(id="1", chunkCount=1),
            },
            {
                Row(id="0", chunkCount=1),
                Row(id="1", chunkCount=1),
            },
        ]

        data = [("0", 789), ("3", 987)]
        initial_state = self.spark.createDataFrame(data, "id string, initVal int").groupBy("id")

        with self.sql_conf(
            # Set it to a very small number so that every row would be a separate pandas df
            {"spark.sql.execution.arrow.maxRecordsPerBatch": "1"}
        ):
            self._test_transform_with_state_basic(
                ChunkCountProcessorFactory(),
                make_check_results(result_with_small_limit),
                output_schema=StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("chunkCount", IntegerType(), True),
                    ]
                ),
            )

            self._test_transform_with_state_basic(
                ChunkCountProcessorWithInitialStateFactory(),
                make_check_results(result_with_small_limit),
                initial_state=initial_state,
                output_schema=StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("chunkCount", IntegerType(), True),
                    ]
                ),
            )

        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": "-1"}):
            self._test_transform_with_state_basic(
                ChunkCountProcessorFactory(),
                make_check_results(result_with_large_limit),
                output_schema=StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("chunkCount", IntegerType(), True),
                    ]
                ),
            )

            self._test_transform_with_state_basic(
                ChunkCountProcessorWithInitialStateFactory(),
                make_check_results(result_with_large_limit),
                initial_state=initial_state,
                output_schema=StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("chunkCount", IntegerType(), True),
                    ]
                ),
            )

    # test all state types (value, list, map) with large values (512 KB)
    def test_transform_with_state_large_values(self):
        def check_results(batch_df, batch_id):
            batch_df.collect()
            # Create expected large string (512 KB)
            target_size_bytes = 512 * 1024
            large_string = "a" * target_size_bytes
            expected_list_elements = ",".join(
                [large_string, large_string + "b", large_string + "c"]
            )
            expected_map_result = f"large_string_key:{large_string}"

            assert set(batch_df.sort("id").collect()) == {
                Row(
                    id="0",
                    valueStateResult=large_string,
                    listStateResult=expected_list_elements,
                    mapStateResult=expected_map_result,
                ),
                Row(
                    id="1",
                    valueStateResult=large_string,
                    listStateResult=expected_list_elements,
                    mapStateResult=expected_map_result,
                ),
            }

        output_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("valueStateResult", StringType(), True),
                StructField("listStateResult", StringType(), True),
                StructField("mapStateResult", StringType(), True),
            ]
        )

        self._test_transform_with_state_basic(
            LargeValueStatefulProcessorFactory(),
            check_results,
            True,
            "None",
            output_schema=output_schema,
        )


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


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.streaming.test_pandas_transform_with_state import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
