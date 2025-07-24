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
import re
import sys
import tempfile
import traceback
import unittest

import pyspark.sql.functions as sf
from pyspark.errors import PythonException
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.errors.exceptions.tblib import Traceback
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.session import SparkSession
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
            _, _, tb = sys.exc_info()
            return traceback.format_exc(), "".join(traceback.format_tb(tb))

    def make_traceback_with_temp_file(self, code="def foo(): 1 / 0", filename=""):
        with tempfile.NamedTemporaryFile("w", suffix=f"{filename}.py", delete=True) as f:
            f.write(code)
            f.flush()
            spec = importlib.util.spec_from_file_location("foo", f.name)
            foo = importlib.util.module_from_spec(spec)
            try:
                spec.loader.exec_module(foo)
                foo.foo()
            except Exception:
                _, _, tb = sys.exc_info()
                return traceback.format_exc(), "".join(traceback.format_tb(tb))
            else:
                self.fail("Error not raised")

    def assert_traceback(self, tb: Traceback, expected: str):
        def remove_positions(s):
            # For example, remove the 2nd line in this traceback:
            """
            result = (x / y / z) * (a / b / c)
                      ~~~~~~^~~
            """
            pattern = r"\s*[\^\~]+\s*\n"
            return re.sub(pattern, "\n", s)

        tb.populate_linecache()
        actual = remove_positions("".join(traceback.format_tb(tb.as_traceback())))
        expected = remove_positions(expected)
        self.assertEqual(actual, expected)

    def test_simple(self):
        s, expected = self.make_traceback()
        self.assert_traceback(Traceback.from_string(s), expected)

    def test_missing_source(self):
        s, expected = self.make_traceback_with_temp_file()
        linecache.clearcache()  # remove temp file from cache
        self.assert_traceback(Traceback.from_string(s), expected)

    def test_recursion(self):
        """
        Don't parse [Previous line repeated n times] because it's expensive for large n.
        Since the input string is not necessarily a Python traceback, Traceback should keep runtime
        linear to the input string length to be safe from malicious inputs.
        """

        def foo(depth):
            if depth > 0:
                return foo(depth - 1)
            raise 1 / 0

        try:
            foo(100)
        except ZeroDivisionError:
            s = traceback.format_exc()
        actual = "".join(traceback.format_tb(Traceback.from_string(s).as_traceback()))
        self.assertIn("[Previous line repeated", s)
        self.assertNotIn("[Previous line repeated", actual)

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
                s, expected = self.make_traceback_with_temp_file(filename=filename)
                linecache.clearcache()
                self.assert_traceback(Traceback.from_string(s), expected)

    @unittest.skipIf(
        os.name != "posix",
        "These file names may be invalid on non-posix systems",
    )
    def test_filename_failure_newline(self):
        # tblib can't handle newline in the filename
        s, expected = self.make_traceback_with_temp_file(filename="\n")
        linecache.clearcache()
        tb = Traceback.from_string(s)
        tb.populate_linecache()
        actual = "".join(traceback.format_tb(tb.as_traceback()))
        self.assertNotEqual(actual, expected)

    def test_syntax_error(self):
        bad_syntax = "bad syntax"
        s, _ = self.make_traceback_with_temp_file(bad_syntax)
        tb = Traceback.from_string(s)
        tb.populate_linecache()
        actual = "".join(traceback.format_tb(tb.as_traceback()))
        self.assertIn("bad syntax", actual)


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

    def assertInOnce(self, needle: str, haystack: str):
        """Assert that a string appears only once in another string."""
        count = haystack.count(needle)
        self.assertEqual(count, 1, f"{needle} appears more than once in {haystack}")

    def test_udf(self):
        for jvm_stack_trace in [False, True]:
            with self.subTest(jvm_stack_trace=jvm_stack_trace), self.sql_conf(
                {"spark.sql.pyspark.jvmStacktrace.enabled": jvm_stack_trace}
            ):

                @sf.udf()
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
                self.assertInOnce("""df.show()""", s)
                self.assertInOnce("""raise ValueError("bar")""", s)

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
        self.assertInOnce("""self.spark.read.format("MyDataSource").load().show()""", s)
        self.assertInOnce("""raise ValueError("bar")""", s)

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
        self.assertInOnce("""self.spark.read.format("MyDataSource").load().show()""", s)
        self.assertInOnce("""raise ValueError("bar")""", s)

    def test_udtf_analysis(self):
        @sf.udtf()
        class MyUdtf:
            @staticmethod
            def analyze():
                raise ValueError("bar")

            def eval(self):
                pass

        try:
            MyUdtf().show()
        except AnalysisException:
            _, _, tb = sys.exc_info()
        else:
            self.fail("AnalysisException not raised")

        s = "".join(traceback.format_tb(tb))
        self.assertInOnce("""MyUdtf().show()""", s)
        self.assertInOnce("""raise ValueError("bar")""", s)

    def test_udtf_execution(self):
        @sf.udtf(returnType="x int")
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
        self.assertInOnce("""MyUdtf().show()""", s)
        self.assertInOnce("""raise ValueError("bar")""", s)


class TracebackSqlClassicTests(BaseTracebackSqlTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.errors.tests.test_traceback import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
