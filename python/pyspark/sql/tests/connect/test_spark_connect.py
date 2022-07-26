import uuid
import unittest
import tempfile
import os
import shutil

from pyspark.sql import SparkSession, Row
from pyspark.sql.connect.client import RemoteSparkSession
from pyspark.sql.connect.function_builder import udf, UserDefinedFunction
from pyspark.testing.utils import ReusedPySparkTestCase

import py4j

class SparkConnectSQLTestCase(ReusedPySparkTestCase):
    """Parent test fixture class for all Spark Connect related
    test cases."""

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        cls.hive_available = True
        # Create the new Spark Session
        cls.spark = SparkSession(cls.sc)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.df = cls.sc.parallelize(cls.testData).toDF()

        # Load test data
        cls.spark_connect_test_data()

    @classmethod
    def spark_connect_test_data(cls):
        # Setup Remote Spark Session
        cls.tbl_name = f"tbl{uuid.uuid4()}".replace("-", "")
        cls.connect = RemoteSparkSession()
        df = cls.spark.createDataFrame([(x, f"{x}") for x in range(100)], ["id", "name"])
        # Since we might create multiple Spark sessions, we need to creata global temporary view
        # that is specifically maintained in the "global_temp" schema.
        df.createGlobalTempView(cls.tbl_name)
        cls.tbl_name = "global_temp." + cls.tbl_name



class SparkConnectTests(SparkConnectSQLTestCase):
    def test_simple_read(self):
        """Tests that we can access the Spark Connect GRPC service locally."""
        df = self.connect.readTable(self.tbl_name)
        data = df.limit(10).collect()
        # Check that the limit is applied
        assert len(data.index) == 10

    def notest_simple_udf(self):
        def conv_udf(x):
            return "Martin"

        u = udf(conv_udf)
        df = self.connect.readTable(self.tbl_name)
        result = df.select(u(df.id)).collect()

if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_spark_connect import *  # noqa: F401
    try:
        import xmlrunner  # type: ignore
        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
