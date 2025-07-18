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
import os
import time
import unittest
from unittest.mock import patch

from py4j.protocol import Py4JJavaError

from pyspark import keyword_only
from pyspark.util import _parse_memory
from pyspark.loose_version import LooseVersion
from pyspark.testing.utils import PySparkTestCase, eventually, timeout
from pyspark.find_spark_home import _find_spark_home


class KeywordOnlyTests(unittest.TestCase):
    class Wrapped:
        @keyword_only
        def set(self, x=None, y=None):
            if "x" in self._input_kwargs:
                self._x = self._input_kwargs["x"]
            if "y" in self._input_kwargs:
                self._y = self._input_kwargs["y"]
            return x, y

    def test_keywords(self):
        w = self.Wrapped()
        x, y = w.set(y=1)
        self.assertEqual(y, 1)
        self.assertEqual(y, w._y)
        self.assertIsNone(x)
        self.assertFalse(hasattr(w, "_x"))

    def test_non_keywords(self):
        w = self.Wrapped()
        self.assertRaises(TypeError, lambda: w.set(0, y=1))

    def test_kwarg_ownership(self):
        # test _input_kwargs is owned by each class instance and not a shared static variable
        class Setter:
            @keyword_only
            def set(self, x=None, other=None, other_x=None):
                if "other" in self._input_kwargs:
                    self._input_kwargs["other"].set(x=self._input_kwargs["other_x"])
                self._x = self._input_kwargs["x"]

        a = Setter()
        b = Setter()
        a.set(x=1, other=b, other_x=2)
        self.assertEqual(a._x, 1)
        self.assertEqual(b._x, 2)


class UtilTests(PySparkTestCase):
    def test_py4j_str(self):
        with self.assertRaises(Py4JJavaError) as context:
            # This attempts java.lang.String(null) which throws an NPE.
            self.sc._jvm.java.lang.String(None)

        self.assertTrue("NullPointerException" in str(context.exception))

    def test_parsing_version_string(self):
        from pyspark.util import VersionUtils

        self.assertRaises(ValueError, lambda: VersionUtils.majorMinorVersion("abced"))

    def test_find_spark_home(self):
        # SPARK-38827: Test find_spark_home without `SPARK_HOME` environment variable set.
        origin = os.environ["SPARK_HOME"]
        try:
            del os.environ["SPARK_HOME"]
            self.assertEqual(origin, _find_spark_home())
        finally:
            os.environ["SPARK_HOME"] = origin

    def test_timeout_decorator(self):
        @timeout(1)
        def timeout_func():
            time.sleep(10)

        with self.assertRaises(TimeoutError) as e:
            timeout_func()
        self.assertTrue("Function timeout_func timed out after 1 seconds" in str(e.exception))

    def test_timeout_function(self):
        def timeout_func():
            time.sleep(10)

        with self.assertRaises(TimeoutError) as e:
            timeout(1)(timeout_func)()
        self.assertTrue("Function timeout_func timed out after 1 seconds" in str(e.exception))

    def test_timeout_lambda(self):
        with self.assertRaises(TimeoutError) as e:
            timeout(1)(lambda: time.sleep(10))()
        self.assertTrue("Function <lambda> timed out after 1 seconds" in str(e.exception))

    @eventually(timeout=180, catch_assertions=True)
    def test_eventually_decorator(self):
        import random

        self.assertTrue(random.random() < 0.1)

    def test_eventually_function(self):
        import random

        def condition():
            self.assertTrue(random.random() < 0.1)

        eventually(timeout=180, catch_assertions=True)(condition)()

    def test_eventually_lambda(self):
        import random

        eventually(timeout=180, catch_assertions=True)(
            lambda: self.assertTrue(random.random() < 0.1)
        )()

    def test_loose_version(self):
        v1 = LooseVersion("1.2.3")
        self.assertEqual(str(v1), "1.2.3")
        self.assertEqual(repr(v1), "LooseVersion ('1.2.3')")
        v2 = "1.2.3"
        self.assertTrue(v1 == v2)
        v3 = 1.1
        with self.assertRaises(TypeError):
            v1 > v3
        v4 = LooseVersion("1.2.4")
        self.assertTrue(v1 <= v4)

    def test_parse_memory(self):
        self.assertEqual(_parse_memory("1g"), 1024)
        with self.assertRaisesRegex(ValueError, "invalid format"):
            _parse_memory("2gs")


class HandleWorkerExceptionTests(unittest.TestCase):
    exception_bytes = b"ValueError: test_message"
    traceback_bytes = b"Traceback (most recent call last):"

    def run_handle_worker_exception(self, hide_traceback=None):
        import io
        from pyspark.util import handle_worker_exception

        try:
            raise ValueError("test_message")
        except Exception as e:
            with io.BytesIO() as stream:
                handle_worker_exception(e, stream, hide_traceback)
                return stream.getvalue()

    @patch.dict(os.environ, {"SPARK_SIMPLIFIED_TRACEBACK": "", "SPARK_HIDE_TRACEBACK": ""})
    def test_env_full(self):
        result = self.run_handle_worker_exception()
        self.assertIn(self.exception_bytes, result)
        self.assertIn(self.traceback_bytes, result)

    @patch.dict(os.environ, {"SPARK_HIDE_TRACEBACK": "1"})
    def test_env_hide_traceback(self):
        result = self.run_handle_worker_exception()
        self.assertIn(self.exception_bytes, result)
        self.assertNotIn(self.traceback_bytes, result)

    @patch.dict(os.environ, {"SPARK_HIDE_TRACEBACK": "1"})
    def test_full(self):
        # Should ignore the environment variable because hide_traceback is explicitly set.
        result = self.run_handle_worker_exception(False)
        self.assertIn(self.exception_bytes, result)
        self.assertIn(self.traceback_bytes, result)

    def test_hide_traceback(self):
        result = self.run_handle_worker_exception(True)
        self.assertIn(self.exception_bytes, result)
        self.assertNotIn(self.traceback_bytes, result)


if __name__ == "__main__":
    from pyspark.tests.test_util import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
