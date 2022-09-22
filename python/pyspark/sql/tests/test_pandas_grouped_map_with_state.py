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

import unittest
from typing import cast

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

if have_pandas:
    import pandas as pd

if have_pyarrow:
    import pyarrow as pa  # noqa: F401


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class GroupedMapInPandasWithStateTests(ReusedSQLTestCase):
    def test_apply_in_pandas_with_state_basic(self):
        df = self.spark.readStream.format("text").load("python/test_support/sql/streaming")

        for q in self.spark.streams.active:
            q.stop()
        self.assertTrue(df.isStreaming)

        output_type = StructType(
            [StructField("key", StringType()), StructField("countAsString", StringType())]
        )
        state_type = StructType([StructField("c", LongType())])

        def func(key, pdf_iter, state):
            assert isinstance(state, GroupState)

            total_len = 0
            for pdf in pdf_iter:
                total_len += len(pdf)

            state.update((total_len,))
            assert state.get[0] == 1
            yield pd.DataFrame({"key": [key[0]], "countAsString": [str(total_len)]})

        def check_results(batch_df, _):
            self.assertEqual(
                set(batch_df.collect()),
                {Row(key="hello", countAsString="1"), Row(key="this", countAsString="1")},
            )

        q = (
            df.groupBy(df["value"])
            .applyInPandasWithState(
                func, output_type, state_type, "Update", GroupStateTimeout.NoTimeout
            )
            .writeStream.queryName("this_query")
            .foreachBatch(check_results)
            .outputMode("update")
            .start()
        )

        self.assertEqual(q.name, "this_query")
        self.assertTrue(q.isActive)
        q.processAllAvailable()


if __name__ == "__main__":
    from pyspark.sql.tests.test_pandas_grouped_map_with_state import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
