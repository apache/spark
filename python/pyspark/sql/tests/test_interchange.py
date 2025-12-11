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
import pyarrow as pa
import pandas as pd
import pyspark.pandas as ps

try:
    import duckdb

    DUCKDB_TESTS = True
except ImportError:
    DUCKDB_TESTS = False


@unittest.skipIf(not DUCKDB_TESTS, " ... ")
class TestSparkArrowCStreamer(unittest.TestCase):
    def test_spark_arrow_c_streamer(self):
        if not DUCKDB_TESTS:
            self.skipTest("duckdb is not installed")

        pdf = pd.DataFrame({"A": [1, "a"], "B": [2, "b"], "C": [3, "c"], "D": [4, "d"]})
        psdf = ps.from_pandas(pdf)
        # Use Spark Arrow C Streamer to convert PyArrow Table to DuckDB relation
        stream = pa.RecordBatchReader.from_stream(psdf)
        assert isinstance(stream, pa.RecordBatchReader)

        # Verify the contents of the DuckDB relation
        result = duckdb.execute("SELECT * from stream").fetchall()
        expected = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
        self.assertEqual(result, expected)
