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
import shutil
import string
import sys
import tempfile
import pandas as pd
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from typing import Iterator

import unittest
from typing import cast

from pyspark import SparkConf
from pyspark.sql.streaming.state import GroupStateTimeout, GroupState
from pyspark.sql.types import (
    LongType,
    StringType,
    StructType,
    StructField,
    Row,
)
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import eventually


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class TransformWithStateInPandasTestsMixin:
    @classmethod
    def conf(cls):
        cfg = SparkConf()
        cfg.set("spark.sql.shuffle.partitions", "5")
        cfg.set("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        return cfg

    def _test_apply_in_pandas_with_state_basic(self, func, check_results):
        input_path = tempfile.mkdtemp()

        def prepare_test_resource():
            with open(input_path + "/text-test.txt", "w") as fw:
                fw.write("hello\n")
                fw.write("this\n")

        prepare_test_resource()

        df = self.spark.readStream.format("text").load(input_path)

        for q in self.spark.streams.active:
            q.stop()
        self.assertTrue(df.isStreaming)

        output_type = StructType(
            [StructField("key", StringType()), StructField("countAsString", StringType())]
        )
        state_type = StructType([StructField("c", LongType())])

        q = (
            df.groupBy(df["value"])
            .transformWithStateInPandas(stateful_processor = SimpleStatefulProcessor(),
                                        outputStructType=output_type,
                                        outputMode="Update",
                                        timeMode="None")
            .writeStream.queryName("this_query")
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, "this_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()
        self.assertTrue(q.exception() is None)

    def test_apply_in_pandas_with_state_basic(self):
        def func(key, pdf_iter, state):
            assert isinstance(state, GroupState)

            total_len = 0
            for pdf in pdf_iter:
                total_len += len(pdf)

            state.update((total_len,))
            assert state.get[0] == 1
            yield pd.DataFrame({"key": [key[0]], "countAsString": [str(total_len)]})

        def check_results(batch_df, _):
            assert set(batch_df.sort("key").collect()) == {
                Row(key="hello", countAsString="1"),
                Row(key="this", countAsString="1"),
            }

        self._test_apply_in_pandas_with_state_basic(func, check_results)


class SimpleStatefulProcessor(StatefulProcessor):
  def init(self, handle: StatefulProcessorHandle) -> None:
    state_schema = StructType([
      StructField("value", StringType(), True)
    ])
    self.value_state = handle.getValueState("testValueState", state_schema)
  def handleInputRows(self, key, rows) -> Iterator[pd.DataFrame]:
    self.value_state.update("test_value")
    exists = self.value_state.exists()
    value = self.value_state.get()
    self.value_state.clear()
    return rows
  def close(self) -> None:
    pass


class TransformWithStateInPandasTests(
    TransformWithStateInPandasTestsMixin, ReusedSQLTestCase
):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_grouped_map_with_state import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)