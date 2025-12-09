import unittest
import pyarrow as pa
import pandas as pd
import pyspark.pandas as ps

try:
    import duckdb

    DUCKDB_TESTS = True
except ImportError:
    DUCKDB_TESTS = False


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
