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

from py4j.protocol import Py4JJavaError

from pyspark import keyword_only
from pyspark.testing.utils import PySparkTestCase


class KeywordOnlyTests(unittest.TestCase):
    class Wrapped(object):
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
        class Setter(object):
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
    def test_py4j_exception_message(self):
        from pyspark.util import _exception_message

        with self.assertRaises(Py4JJavaError) as context:
            # This attempts java.lang.String(null) which throws an NPE.
            self.sc._jvm.java.lang.String(None)

        self.assertTrue('NullPointerException' in _exception_message(context.exception))

    def test_parsing_version_string(self):
        from pyspark.util import VersionUtils
        self.assertRaises(ValueError, lambda: VersionUtils.majorMinorVersion("abced"))


if __name__ == "__main__":
    from pyspark.tests.test_util import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
