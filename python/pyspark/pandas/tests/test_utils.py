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

from pyspark.pandas.indexes.base import Index
from pyspark.pandas.utils import (
    lazy_property,
    validate_arguments_and_invoke_function,
    validate_bool_kwarg,
    validate_index_loc,
    validate_mode,
)
from pyspark.testing.pandasutils import (
    PandasOnSparkTestCase,
    _assert_pandas_equal,
    _assert_pandas_almost_equal,
)
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.errors import PySparkAssertionError

some_global_variable = 0


class UtilsTestsMixin:
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

    def test_validate_index_loc(self):
        psidx = Index([1, 2, 3])
        validate_index_loc(psidx, -1)
        validate_index_loc(psidx, -3)
        err_msg = "index 4 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            validate_index_loc(psidx, 4)
        err_msg = "index -4 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            validate_index_loc(psidx, -4)

    def test_dataframe_error_assert_pandas_equal(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[0, 1, 3])
        pdf2 = pd.DataFrame({"a": [1, 3, 3], "b": [4, 5, 6]}, index=[0, 1, 3])

        with self.assertRaises(PySparkAssertionError) as pe:
            _assert_pandas_equal(pdf1, pdf2, True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_PANDAS_DATAFRAME",
            messageParameters={
                "left": pdf1.to_string(),
                "left_dtype": str(pdf1.dtypes),
                "right": pdf2.to_string(),
                "right_dtype": str(pdf2.dtypes),
            },
        )

    def test_series_error_assert_pandas_equal(self):
        series1 = pd.Series([1, 2, 3])
        series2 = pd.Series([4, 5, 6])

        with self.assertRaises(PySparkAssertionError) as pe:
            _assert_pandas_equal(series1, series2, True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_PANDAS_SERIES",
            messageParameters={
                "left": series1.to_string(),
                "left_dtype": str(series1.dtype),
                "right": series2.to_string(),
                "right_dtype": str(series2.dtype),
            },
        )

    def test_index_error_assert_pandas_equal(self):
        index1 = pd.Index([1, 2, 3])
        index2 = pd.Index([4, 5, 6])

        with self.assertRaises(PySparkAssertionError) as pe:
            _assert_pandas_equal(index1, index2, True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_PANDAS_INDEX",
            messageParameters={
                "left": index1,
                "left_dtype": str(index1.dtype),
                "right": index2,
                "right_dtype": str(index2.dtype),
            },
        )

    def test_multiindex_error_assert_pandas_almost_equal(self):
        pdf1 = pd.DataFrame({"a": [1, 2], "b": [4, 10]}, index=[0, 1])
        pdf2 = pd.DataFrame({"a": [1, 5, 3], "b": [1, 5, 6]}, index=[0, 1, 3])
        multiindex1 = pd.MultiIndex.from_frame(pdf1)
        multiindex2 = pd.MultiIndex.from_frame(pdf2)

        with self.assertRaises(PySparkAssertionError) as pe:
            _assert_pandas_almost_equal(multiindex1, multiindex2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_PANDAS_MULTIINDEX",
            messageParameters={
                "left": multiindex1,
                "left_dtype": str(multiindex1.dtype),
                "right": multiindex2,
                "right_dtype": str(multiindex1.dtype),
            },
        )


class TestClassForLazyProp:
    def __init__(self):
        self.some_variable = 0

    @lazy_property
    def lazy_prop(self):
        self.some_variable += 1
        return self.some_variable


class UtilsTests(UtilsTestsMixin, PandasOnSparkTestCase, SQLTestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_utils import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
