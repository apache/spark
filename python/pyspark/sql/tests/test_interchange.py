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
from pyspark.testing.utils import have_duckdb

if have_duckdb:
    import duckdb


@unittest.skipIf(not have_duckdb, "duckdb is not installed")
class TestSparkArrowCStreamer(unittest.TestCase):
    def test_spark_arrow_c_streamer(self):
        pdf = pd.DataFrame([[1, "a"], [2, "b"], [3, "c"], [4, "d"]], columns=["id", "value"])
        psdf = ps.from_pandas(pdf)
        # Use Spark Arrow C Streamer to convert PyArrow Table to DuckDB relation
        stream = pa.RecordBatchReader.from_stream(psdf)
        assert isinstance(stream, pa.RecordBatchReader)

        # Verify the contents of the DuckDB relation
        result = duckdb.execute("SELECT id, value from stream").fetchall()
        expected = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
        self.assertEqual(result, expected)


if __name__ == "__main__":
    from pyspark.sql.tests.test_interchange import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        test_runner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        test_runner = None
    unittest.main(testRunner=test_runner, verbosity=2)
