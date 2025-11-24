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
import unittest
import tempfile
import sys
import os
from typing import Iterator, List, Tuple, Any

import pandas as pd

from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql.functions import split
from pyspark.sql.streaming import StatefulProcessor
from pyspark.sql.streaming.tws_tester import TwsTester
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

def LOG(s):
    with open("/tmp/log.txt", "a") as f:
        f.write(s + "\n")



# @unittest.skip("a")
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
        # Disable snapshot upload reporting to avoid coordinator dependency in batch mode
        #cfg.set("spark.sql.streaming.stateStore.reportSnapshotUploadLag", "false")
        #cfg.set("spark.sql.execution.arrow.transformWithStateInPandas.maxRecordsPerBatch", "2")
        cfg.set("spark.sql.session.timeZone", "UTC")
        cfg.set("spark.sql.streaming.noDataMicroBatches.enabled", "false")
        return cfg

    """
    def _check_tws_tester(
        self, processor: StatefulProcessor, batches: List[List[Tuple[Any, Any]]]
    ):
        tester = TwsTester(processor)
        expected_results: List["pd.DataFrame"] = [
            tester.test(batch) for batch in batches
        ]
        
        # Run actual Spark streaming query
        input_path = tempfile.mkdtemp()
        checkpoint_path = tempfile.mkdtemp()
        
        # Prepare batch files: write each batch to a separate file
        for batch_id, batch in enumerate(batches):
            batch_file = f"{input_path}/batch-{batch_id:03d}.txt"
            with open(batch_file, "w") as fw:
                for key, input_df in batch:
                    # For each row in the input DataFrame, write: key,col1,col2,...
                    for _, row in input_df.iterrows():
                        row_values = [str(key)] + [str(v) for v in row.values]
                        fw.write(",".join(row_values) + "\n")
            if batch_id < len(batches) - 1:
                time.sleep(2)  # Delay between batches for streaming
        
        # Read streaming data
        df = self.spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(input_path)
        
        # Parse text into structured format: _key, value
        from pyspark.sql.functions import split as spark_split
        df_split = df.withColumn("split_values", spark_split(df["value"], ","))
        df_parsed = df_split.select(
            df_split.split_values.getItem(0).alias("_key"),
            df_split.split_values.getItem(1).alias("value"),
        )
        
        # Collect results from each batch
        actual_results = []
        
        def collect_batch(batch_df, batch_id):
            print("AAAA batch_df.collect()=", batch_df.collect())
            result_pdf = batch_df.toPandas()
            print("AAAA pdf:\n", result_pdf.to_string(index=False))
            actual_results.append(result_pdf)
        
        # Run streaming query with transformWithStateInPandas
        q = (
            df_parsed.groupBy("_key")
            .transformWithStateInPandas(
                statefulProcessor=processor,
                outputStructType=output_schema,
                outputMode="Update",
                timeMode="None",
            )
            #.writeStream.queryName("tws_tester_query")
            #.option("checkpointLocation", checkpoint_path)
            .foreachBatch(collect_batch)
            .outputMode("update")
            .start()
        )
        
        q.processAllAvailable()
        q.awaitTermination(10)
        self.assertIsNone(q.exception())
        q.stop()
        
        # Compare results: same number of batches and same content (up to reordering)
        self.assertEqual(len(actual_results), len(expected_results))
        for actual_pdf, expected_pdf in zip(actual_results, expected_results):
            actual_records = actual_pdf.to_dict("records")
            expected_records = expected_pdf.to_dict("records")
            actual_set = {frozenset(d.items()) for d in actual_records}
            expected_set = {frozenset(d.items()) for d in expected_records}
            self.assertEqual(actual_set, expected_set)
    
    @unittest.skip("tmep")
    def test_fuzz_running_count_processor(self):
        import random

        random.seed(0)

        input_data: List[Tuple[Any, Any]] = []
        for _ in range(1):  # TODO: replace with 1000
            key = f"key{random.randint(0, 9)}"
            value = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5))
            input_data.append((key, pd.DataFrame({"value": [value]})))

        batches = [input_data]

        self._check_tws_tester(RunningCountProcessor(), batches)
    """


    # Warning: first columns here is always the key, which is not necessary the case in TwsTester.
    def _check_tws_tester(
        self, 
        processor: StatefulProcessor, 
        batches: list[list[tuple]],
        input_columns: list[str],
        output_schema: StructType,
        use_pandas: bool = False,
    ):
        
       

        num_batches = len(batches)
        key_column: str = input_columns[0]
        num_input_columns = len(input_columns)
        tester = TwsTester(processor, key_column_name=key_column)
        
        def _run_tester(batch: list[tuple]) -> list[Row]:
            if use_pandas:
                input_to_tester = pd.DataFrame(batch, columns=input_columns)
                return self.spark.createDataFrame(tester.testInPandas(input_to_tester)).collect()
            else:
                input_to_tester: list[Row] = [Row(**dict(zip(input_columns, r))) for r in batch]
                return tester.test(input_to_tester)
        
        expected_results: list[list[Row]] = [_run_tester(batch) for batch in batches]



        # Create streaming source from file
        input_path = tempfile.mkdtemp()
        #checkpoint_path = tempfile.mkdtemp()
        
        def _write_batch(batch_id: int, batch: list[tuple]):
            with open(input_path + f"/test{batch_id}.txt", "w") as f:
                for row in batch:
                    assert len(row) == num_input_columns
                    f.write(",".join(row) + "\n")

        
        # Create streaming DataFrame

        df = self.spark.readStream.format("text").load(input_path)
        df_split = df.withColumn("split_values", split(df["value"], ","))
        df_final = df_split.select(*[
            df_split.split_values.getItem(i).alias(col_name).cast("string")
            for i, col_name in enumerate(input_columns)
        ])
        
        self.assertTrue(df_final.isStreaming)
         
        df_grouped =  df_final.groupBy(key_column) 

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

        
        actual_results: list[list[Row]] = []
        # Use foreachBatch to check results
        def _collect_results(batch_df: DataFrame, batch_id):
            rows: list[Row] = batch_df.collect()
            actual_results.append(rows)
            LOG(f"AAAAA batch_id={batch_id}")

        
        # Start streaming query
        q = (
            tws_df.writeStream
            .queryName("test_tmp_query")
            #.option("checkpointLocation", checkpoint_path)
            .foreachBatch(_collect_results)
            .outputMode("update")
            .start()
        )
        
        try:
            for batch_id, batch in enumerate(batches):
                _write_batch(batch_id, batch)
                q.processAllAvailable()
            q.awaitTermination(1)
            self.assertTrue(q.exception() is None)
        finally:
            q.stop()

        LOG(f"result lengths: {[len(x) for x in actual_results]}")
        self.assertEqual(len(actual_results), num_batches)
        self.assertEqual(len(expected_results), num_batches)
        for batch_id in range(num_batches):
            self.assertEqual(actual_results[batch_id], expected_results[batch_id])

    def test_running_count_fuzz(self):
        LOG("***** TEST START ******")



    

        output_schema = StructType(
            [
                StructField("key", StringType(), True),
                StructField("count", IntegerType(), True),
            ]
        )
        state_schema = StructType([StructField("value", IntegerType(), True)])

        class RunningCountProcessor(StatefulProcessor):
            def __init__(self, use_pandas=True):
                self.use_pandas = use_pandas

            def init(self, handle) -> None:
                self.handle = handle
                self.count = handle.getValueState("count", state_schema)

            def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
                count = self.count.get()[0] if self.count.exists() else 0

                if self.use_pandas:
                    for row_df in rows:
                        for _, row in row_df.iterrows():
                            count += 1
                else:
                    for row in rows:
                        count += 1

                self.count.update((count,))

                if self.use_pandas:
                    yield pd.DataFrame(
                        {
                            "key": [key[0]],
                            "count": [count],
                        }
                    )
                else:
                    yield Row(key=key[0], count=count)


        ########
        
        input_data: list[tuple[str, str]] = []
        for _ in range(1000):
            key = f"key{random.randint(0, 9)}"
            value = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5))
            input_data.append((key, value))
        input_columns: list[str] = ["key", "value"]
        batches = [input_data, input_data]

        for use_pandas in [False, True]: 
            LOG(f"**** use_pandas={use_pandas}")
            processor = RunningCountProcessor(use_pandas=use_pandas)
            self._check_tws_tester(processor, batches, input_columns, output_schema, use_pandas=use_pandas )

        


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.streaming.test_tws_tester import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
