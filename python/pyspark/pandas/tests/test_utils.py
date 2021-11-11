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

import pandas as pd

from pyspark.pandas.utils import (
    lazy_property,
    validate_arguments_and_invoke_function,
    validate_bool_kwarg,
    validate_mode,
)
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils

some_global_variable = 0


class UtilsTest(PandasOnSparkTestCase, SQLTestUtils):

    # a dummy to_html version with an extra parameter that pandas does not support
    # used in test_validate_arguments_and_invoke_function
    def to_html(self, max_rows=None, unsupported_param=None):
        args = locals()

        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[0, 1, 3])
        validate_arguments_and_invoke_function(pdf, self.to_html, pd.DataFrame.to_html, args)

    def to_clipboard(self, sep=",", **kwargs):
        args = locals()

        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[0, 1, 3])
        validate_arguments_and_invoke_function(
            pdf, self.to_clipboard, pd.DataFrame.to_clipboard, args
        )

        # Support for **kwargs
        self.to_clipboard(sep=",", index=False)

    def test_validate_arguments_and_invoke_function(self):
        # This should pass and run fine
        self.to_html()
        self.to_html(unsupported_param=None)
        self.to_html(max_rows=5)

        # This should fail because we are explicitly setting an unsupported param
        # to a non-default value
        with self.assertRaises(TypeError):
            self.to_html(unsupported_param=1)

    def test_lazy_property(self):
        obj = TestClassForLazyProp()
        # If lazy prop is not working, the second test would fail (because it'd be 2)
        self.assert_eq(obj.lazy_prop, 1)
        self.assert_eq(obj.lazy_prop, 1)

    def test_validate_bool_kwarg(self):
        # This should pass and run fine
        pandas_on_spark = True
        self.assert_eq(validate_bool_kwarg(pandas_on_spark, "pandas_on_spark"), True)
        pandas_on_spark = False
        self.assert_eq(validate_bool_kwarg(pandas_on_spark, "pandas_on_spark"), False)
        pandas_on_spark = None
        self.assert_eq(validate_bool_kwarg(pandas_on_spark, "pandas_on_spark"), None)

        # This should fail because we are explicitly setting a non-boolean value
        pandas_on_spark = "true"
        with self.assertRaisesRegex(
            TypeError, 'For argument "pandas_on_spark" expected type bool, received type str.'
        ):
            validate_bool_kwarg(pandas_on_spark, "pandas_on_spark")

    def test_validate_mode(self):
        self.assert_eq(validate_mode("a"), "append")
        self.assert_eq(validate_mode("w"), "overwrite")
        self.assert_eq(validate_mode("a+"), "append")
        self.assert_eq(validate_mode("w+"), "overwrite")

        with self.assertRaises(ValueError):
            validate_mode("r")


class TestClassForLazyProp:
    def __init__(self):
        self.some_variable = 0

    @lazy_property
    def lazy_prop(self):
        self.some_variable += 1
        return self.some_variable


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_utils import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
