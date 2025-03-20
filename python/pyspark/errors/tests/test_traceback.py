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
import os
import sys
import tempfile
import traceback
import unittest

import pyspark.sql.functions as F
from pyspark.errors import PythonException
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.errors.exceptions.tblib import Traceback
from pyspark.sql.datasource import DataSource, DataSourceReader
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
    """Tests for the Traceback class."""

    def make_traceback(self):
        try:
            raise ValueError("bar")
        except ValueError:
            return traceback.format_exc()

    def make_traceback_with_temp_file(self, code: str, filename=""):
        with tempfile.NamedTemporaryFile("w", suffix=f"{filename}.py", delete=True) as f:
            f.write(code)
            f.flush()
            spec = importlib.util.spec_from_file_location("foo", f.name)
            foo = importlib.util.module_from_spec(spec)
            try:
                spec.loader.exec_module(foo)
                foo.foo()
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
        s = self.make_traceback_with_temp_file("""def foo(): 1 / 0""")
        linecache.clearcache()  # remove temp file from cache
        self.assert_traceback(Traceback.from_string(s), s)

    @unittest.skipIf(
        os.name != "posix",
        "These file names may be invalid on non-posix systems",
    )
    def test_filename(self):
        for filename in [
            "",
            " ",
            "\\",
            '"',
            "'",
            '", line 1, in hello',
        ]:
            with self.subTest(filename=filename):
                s = self.make_traceback_with_temp_file("""def foo(): 1 / 0""", filename=filename)
                linecache.clearcache()
                self.assert_traceback(Traceback.from_string(s), s)

    @unittest.skipIf(
        os.name != "posix",
        "These file names may be invalid on non-posix systems",
    )
    def test_filename_failure_newline(self):
        # tblib can't handle newline in the filename
        s = self.make_traceback_with_temp_file("""def foo(): 1 / 0""", filename="\n")
        linecache.clearcache()
        tb = Traceback.from_string(s)
        tb.populate_linecache()
        expected = "".join(s.splitlines(keepends=True)[1:-1])
        actual = "".join(traceback.format_tb(tb.as_traceback()))
        self.assertNotEqual(actual, expected)

    def test_syntax_error(self):
        bad_syntax = "bad syntax"
        s = self.make_traceback_with_temp_file(bad_syntax)
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
    """Tests for recovering the original traceback from JVM exceptions."""

    spark: SparkSession

    @staticmethod
    def raise_exception():
        raise ValueError("bar")

    def test_udf(self):
        @F.udf()
        def foo():
            raise ValueError("bar")

        df = self.spark.range(1).select(foo())
        try:
            df.show()
        except PythonException:
            _, _, tb = sys.exc_info()
        else:
            self.fail("PythonException not raised")

        s = "".join(traceback.format_tb(tb))
        self.assertIn("""df.show()""", s)
        self.assertIn("""raise ValueError("bar")""", s)

    def test_datasource_analysis(self):
        class MyDataSource(DataSource):
            def schema(self):
                raise ValueError("bar")

        self.spark.dataSource.register(MyDataSource)
        try:
            self.spark.read.format("MyDataSource").load().show()
        except AnalysisException:
            _, _, tb = sys.exc_info()
        else:
            self.fail("AnalysisException not raised")

        s = "".join(traceback.format_tb(tb))
        self.assertIn("""self.spark.read.format("MyDataSource").load().show()""", s)
        self.assertIn("""raise ValueError("bar")""", s)

    def test_datasource_execution(self):
        class MyDataSource(DataSource):
            def schema(self):
                return "x int"

            def reader(self, schema):
                return MyDataSourceReader()

        class MyDataSourceReader(DataSourceReader):
            def read(self, partitions):
                raise ValueError("bar")

        self.spark.dataSource.register(MyDataSource)
        try:
            self.spark.read.format("MyDataSource").load().show()
        except PythonException:
            _, _, tb = sys.exc_info()
        else:
            self.fail("PythonException not raised")

        s = "".join(traceback.format_tb(tb))
        self.assertIn("""self.spark.read.format("MyDataSource").load().show()""", s)
        self.assertIn("""raise ValueError("bar")""", s)

    def test_udtf_analysis(self):
        @F.udtf()
        class MyUdtf:
            @staticmethod
            def analyze():
                raise ValueError("bar")

            def eval(self):
                ...

        try:
            MyUdtf().show()
        except AnalysisException:
            _, _, tb = sys.exc_info()
        else:
            self.fail("AnalysisException not raised")

        s = "".join(traceback.format_tb(tb))
        self.assertIn("""MyUdtf().show()""", s)
        self.assertIn("""raise ValueError("bar")""", s)

    def test_udtf_execution(self):
        @F.udtf(returnType="x int")
        class MyUdtf:
            def eval(self):
                raise ValueError("bar")

        try:
            MyUdtf().show()
        except PythonException:
            _, _, tb = sys.exc_info()
        else:
            self.fail("PythonException not raised")

        s = "".join(traceback.format_tb(tb))
        self.assertIn("""MyUdtf().show()""", s)
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
