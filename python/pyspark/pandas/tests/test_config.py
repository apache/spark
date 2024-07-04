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

from pyspark import pandas as ps
from pyspark.pandas import config
from pyspark.pandas.config import Option, DictWrapper
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class ConfigTestsMixin:
    def setUp(self):
        config._options_dict["test.config"] = Option(key="test.config", doc="", default="default")

        config._options_dict["test.config.list"] = Option(
            key="test.config.list", doc="", default=[], types=list
        )
        config._options_dict["test.config.float"] = Option(
            key="test.config.float", doc="", default=1.2, types=float
        )

        config._options_dict["test.config.int"] = Option(
            key="test.config.int",
            doc="",
            default=1,
            types=int,
            check_func=(lambda v: v > 0, "bigger then 0"),
        )
        config._options_dict["test.config.int.none"] = Option(
            key="test.config.int", doc="", default=None, types=(int, type(None))
        )

    def tearDown(self):
        ps.reset_option("test.config")
        del config._options_dict["test.config"]
        del config._options_dict["test.config.list"]
        del config._options_dict["test.config.float"]
        del config._options_dict["test.config.int"]
        del config._options_dict["test.config.int.none"]

    def test_get_set_reset_option(self):
        self.assertEqual(ps.get_option("test.config"), "default")

        ps.set_option("test.config", "value")
        self.assertEqual(ps.get_option("test.config"), "value")

        ps.reset_option("test.config")
        self.assertEqual(ps.get_option("test.config"), "default")

    def test_get_set_reset_option_different_types(self):
        ps.set_option("test.config.list", [1, 2, 3, 4])
        self.assertEqual(ps.get_option("test.config.list"), [1, 2, 3, 4])

        ps.set_option("test.config.float", 5.0)
        self.assertEqual(ps.get_option("test.config.float"), 5.0)

        ps.set_option("test.config.int", 123)
        self.assertEqual(ps.get_option("test.config.int"), 123)

        self.assertEqual(ps.get_option("test.config.int.none"), None)  # default None
        ps.set_option("test.config.int.none", 123)
        self.assertEqual(ps.get_option("test.config.int.none"), 123)
        ps.set_option("test.config.int.none", None)
        self.assertEqual(ps.get_option("test.config.int.none"), None)

    def test_different_types(self):
        with self.assertRaisesRegex(TypeError, "was <class 'int'>"):
            ps.set_option("test.config.list", 1)

        with self.assertRaisesRegex(TypeError, "however, expected types are"):
            ps.set_option("test.config.float", "abc")

        with self.assertRaisesRegex(TypeError, "[<class 'int'>]"):
            ps.set_option("test.config.int", "abc")

        with self.assertRaisesRegex(TypeError, "(<class 'int'>, <class 'NoneType'>)"):
            ps.set_option("test.config.int.none", "abc")

    def test_check_func(self):
        with self.assertRaisesRegex(ValueError, "bigger then 0"):
            ps.set_option("test.config.int", -1)

    def test_unknown_option(self):
        with self.assertRaisesRegex(config.OptionError, "No such option"):
            ps.get_option("unknown")

        with self.assertRaisesRegex(config.OptionError, "Available options"):
            ps.set_option("unknown", "value")

        with self.assertRaisesRegex(config.OptionError, "test.config"):
            ps.reset_option("unknown")

    def test_namespace_access(self):
        try:
            self.assertEqual(ps.options.compute.max_rows, ps.get_option("compute.max_rows"))
            ps.options.compute.max_rows = 0
            self.assertEqual(ps.options.compute.max_rows, 0)
            self.assertTrue(isinstance(ps.options.compute, DictWrapper))

            wrapper = ps.options.compute
            self.assertEqual(wrapper.max_rows, ps.get_option("compute.max_rows"))
            wrapper.max_rows = 1000
            self.assertEqual(ps.options.compute.max_rows, 1000)

            self.assertRaisesRegex(config.OptionError, "No such option", lambda: ps.options.compu)
            self.assertRaisesRegex(
                config.OptionError, "No such option", lambda: ps.options.compute.max
            )
            self.assertRaisesRegex(
                config.OptionError, "No such option", lambda: ps.options.max_rows1
            )

            with self.assertRaisesRegex(config.OptionError, "No such option"):
                ps.options.compute.max = 0
            with self.assertRaisesRegex(config.OptionError, "No such option"):
                ps.options.compute = 0
            with self.assertRaisesRegex(config.OptionError, "No such option"):
                ps.options.com = 0
        finally:
            ps.reset_option("compute.max_rows")

    def test_dir_options(self):
        self.assertTrue("compute.default_index_type" in dir(ps.options))
        self.assertTrue("plotting.sample_ratio" in dir(ps.options))

        self.assertTrue("default_index_type" in dir(ps.options.compute))
        self.assertTrue("sample_ratio" not in dir(ps.options.compute))

        self.assertTrue("default_index_type" not in dir(ps.options.plotting))
        self.assertTrue("sample_ratio" in dir(ps.options.plotting))


class ConfigTests(ConfigTestsMixin, PandasOnSparkTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_config import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
