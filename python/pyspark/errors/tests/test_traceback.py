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
import importlib.util
import linecache
import sys
import tempfile
import traceback
import unittest

from pyspark.errors import PythonException
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.errors.exceptions.traceback import Traceback
from pyspark.sql.functions import udf
from pyspark.sql.session import SparkSession
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


class TracebackTests(unittest.TestCase):
    def make_traceback(self):
        try:
            raise ValueError("bar")
        except ValueError:
            return traceback.format_exc()

    def make_traceback_with_temp_file(self, code: str):
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=True) as f:
            f.write(code)
            f.flush()
            spec = importlib.util.spec_from_file_location("foo", f.name)
            foo = importlib.util.module_from_spec(spec)
            try:
                spec.loader.exec_module(foo)
                foo.f()
            except Exception:
                return traceback.format_exc()
            else:
                assert False, "ZeroDivisionError not raised"

    def assert_traceback(self, tb: Traceback, error: str):
        tb.populate_linecache()
        expected = "".join(error.splitlines(keepends=True)[1:-1])  # keep only the traceback
        actual = "".join(traceback.format_tb(tb.as_traceback()))
        self.assertEqual(actual, expected)

    def test_simple(self):
        s = self.make_traceback()
        self.assert_traceback(Traceback.from_string(s), s)

    def test_missing_source(self):
        s = self.make_traceback_with_temp_file("""def f(): 1 / 0""")
        linecache.clearcache()  # remove temp file from cache
        self.assert_traceback(Traceback.from_string(s), s)

    def test_syntax_error(self):
        self.maxDiff = None
        s = self.make_traceback_with_temp_file("""bad syntax""")
        tb = Traceback.from_string(s)
        tb.populate_linecache()
        actual = "".join(traceback.format_tb(tb.as_traceback()))
        self.assertIn("bad syntax", actual)
        self.assertNotIn("^", actual)


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class BaseTracebackSqlTestsMixin:
    spark: SparkSession

    @staticmethod
    def raise_exception():
        raise ValueError("bar")

    def test_udf(self):
        @udf()
        def foo():
            raise ValueError("bar")

        df = self.spark.range(1).select(foo())
        try:
            df.show()
        except PythonException as e:
            _, _, tb = sys.exc_info()
        else:
            self.fail("PythonException not raised")

        s = "\n".join(traceback.format_tb(tb))
        self.assertIn("""df.show()""", s)
        self.assertIn("""raise ValueError("bar")""", s)

    def test_datasource(self):
        from pyspark.sql.datasource import DataSource

        class MyDataSource(DataSource):
            def schema(self):
                raise ValueError("bar")

        self.spark.dataSource.register(MyDataSource)
        try:
            self.spark.read.format("MyDataSource").load().show()
        except AnalysisException as e:
            traceback.print_exc()
            _, _, tb = sys.exc_info()
        else:
            self.fail("AnalysisException not raised")

        s = "\n".join(traceback.format_tb(tb))
        self.assertIn("""self.spark.read.format("MyDataSource").load().show()""", s)
        self.assertIn("""raise ValueError("bar")""", s)


class TracebackSqlClassicTests(BaseTracebackSqlTestsMixin, ReusedSQLTestCase):
    ...


class TracebackSqlConnectTests(BaseTracebackSqlTestsMixin, ReusedConnectTestCase):
    ...


if __name__ == "__main__":
    from pyspark.errors.tests.test_traceback import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
