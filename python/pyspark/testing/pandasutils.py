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

import functools
import shutil
import tempfile
import warnings
import pandas as pd
from contextlib import contextmanager
from distutils.version import LooseVersion
import decimal
from typing import Union

from pyspark import pandas as ps
from pyspark.pandas.frame import DataFrame
from pyspark.pandas.indexes import Index
from pyspark.pandas.series import Series
from pyspark.pandas.utils import SPARK_CONF_ARROW_ENABLED
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.errors import PySparkAssertionError

tabulate_requirement_message = None
try:
    from tabulate import tabulate
except ImportError as e:
    # If tabulate requirement is not satisfied, skip related tests.
    tabulate_requirement_message = str(e)
have_tabulate = tabulate_requirement_message is None

matplotlib_requirement_message = None
try:
    import matplotlib
except ImportError as e:
    # If matplotlib requirement is not satisfied, skip related tests.
    matplotlib_requirement_message = str(e)
have_matplotlib = matplotlib_requirement_message is None

plotly_requirement_message = None
try:
    import plotly
except ImportError as e:
    # If plotly requirement is not satisfied, skip related tests.
    plotly_requirement_message = str(e)
have_plotly = plotly_requirement_message is None


def assertPandasDFEqual(left, right, check_exact):
    import pandas as pd
    from pandas.core.dtypes.common import is_numeric_dtype
    from pandas.testing import assert_frame_equal, assert_index_equal, assert_series_equal

    if isinstance(left, pd.DataFrame) and isinstance(right, pd.DataFrame):
        try:
            if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
                kwargs = dict(check_freq=False)
            else:
                kwargs = dict()

            if LooseVersion(pd.__version__) < LooseVersion("1.1.1"):
                # Due to https://github.com/pandas-dev/pandas/issues/35446
                check_exact = (
                    check_exact
                    and all([is_numeric_dtype(dtype) for dtype in left.dtypes])
                    and all([is_numeric_dtype(dtype) for dtype in right.dtypes])
                )

            assert_frame_equal(
                left,
                right,
                check_index_type=("equiv" if len(left.index) > 0 else False),
                check_column_type=("equiv" if len(left.columns) > 0 else False),
                check_exact=check_exact,
                **kwargs,
            )
        except AssertionError as e:
            msg = (
                str(e)
                + "\n\nLeft:\n%s\n%s" % (left, left.dtypes)
                + "\n\nRight:\n%s\n%s" % (right, right.dtypes)
            )
            raise AssertionError(msg) from e
    elif isinstance(left, pd.Series) and isinstance(right, pd.Series):
        try:
            if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
                kwargs = dict(check_freq=False)
            else:
                kwargs = dict()
            if LooseVersion(pd.__version__) < LooseVersion("1.1.1"):
                # Due to https://github.com/pandas-dev/pandas/issues/35446
                check_exact = (
                    check_exact and is_numeric_dtype(left.dtype) and is_numeric_dtype(right.dtype)
                )
            assert_series_equal(
                left,
                right,
                check_index_type=("equiv" if len(left.index) > 0 else False),
                check_exact=check_exact,
                **kwargs,
            )
        except AssertionError as e:
            msg = (
                str(e)
                + "\n\nLeft:\n%s\n%s" % (left, left.dtype)
                + "\n\nRight:\n%s\n%s" % (right, right.dtype)
            )
            raise AssertionError(msg) from e
    elif isinstance(left, pd.Index) and isinstance(right, pd.Index):
        try:
            if LooseVersion(pd.__version__) < LooseVersion("1.1.1"):
                # Due to https://github.com/pandas-dev/pandas/issues/35446
                check_exact = (
                    check_exact and is_numeric_dtype(left.dtype) and is_numeric_dtype(right.dtype)
                )
            assert_index_equal(left, right, check_exact=check_exact)
        except AssertionError as e:
            msg = (
                str(e)
                + "\n\nLeft:\n%s\n%s" % (left, left.dtype)
                + "\n\nRight:\n%s\n%s" % (right, right.dtype)
            )
            raise AssertionError(msg) from e
    else:
        raise ValueError("Unexpected values: (%s, %s)" % (left, right))


def assertPandasDFAlmostEqual(left, right):
    """
    This function checks if given pandas objects approximately same,
    which means the conditions below:
      - Both objects are nullable
      - Compare floats rounding to the number of decimal places, 7 after
        dropping missing values (NaN, NaT, None)
    """
    import pandas as pd

    rtol = 1e-5
    atol = 1e-8

    if isinstance(left, pd.DataFrame) and isinstance(right, pd.DataFrame):
        msg = (
            "DataFrames are not almost equal: "
            + "\n\nLeft:\n%s\n%s" % (left, left.dtypes)
            + "\n\nRight:\n%s\n%s" % (right, right.dtypes)
        )
        if left.shape != right.shape:
            raise PySparkAssertionError(
                error_class="DIFFERENT_PANDAS_DATAFRAME",
                message_parameters={
                    "msg": msg,
                },
            )
        for lcol, rcol in zip(left.columns, right.columns):
            if lcol != rcol:
                raise PySparkAssertionError(
                    error_class="DIFFERENT_PANDAS_DATAFRAME",
                    message_parameters={
                        "msg": msg,
                    },
                )
            for lnull, rnull in zip(left[lcol].isnull(), right[rcol].isnull()):
                if lnull != rnull:
                    raise PySparkAssertionError(
                        error_class="DIFFERENT_PANDAS_DATAFRAME",
                        message_parameters={
                            "msg": msg,
                        },
                    )
            for lval, rval in zip(left[lcol].dropna(), right[rcol].dropna()):
                if (isinstance(lval, float) or isinstance(lval, decimal.Decimal)) and (
                    isinstance(rval, float) or isinstance(rval, decimal.Decimal)
                ):
                    if abs(float(lval) - float(rval)) > (atol + rtol * abs(float(rval))):
                        raise PySparkAssertionError(
                            error_class="DIFFERENT_PANDAS_DATAFRAME",
                            message_parameters={
                                "msg": msg,
                            },
                        )
        if left.columns.names != right.columns.names:
            raise PySparkAssertionError(
                error_class="DIFFERENT_PANDAS_DATAFRAME",
                message_parameters={
                    "msg": msg,
                },
            )
    elif isinstance(left, pd.Series) and isinstance(right, pd.Series):
        msg = (
            "Series are not almost equal: "
            + "\n\nLeft:\n%s\n%s" % (left, left.dtype)
            + "\n\nRight:\n%s\n%s" % (right, right.dtype)
        )
        if left.name != right.name or len(left) != len(right):
            raise PySparkAssertionError(
                error_class="DIFFERENT_PANDAS_DATAFRAME",
                message_parameters={
                    "msg": msg,
                },
            )
        for lnull, rnull in zip(left.isnull(), right.isnull()):
            if lnull != rnull:
                raise PySparkAssertionError(
                    error_class="DIFFERENT_PANDAS_DATAFRAME",
                    message_parameters={
                        "msg": msg,
                    },
                )
        for lval, rval in zip(left.dropna(), right.dropna()):
            if (isinstance(lval, float) or isinstance(lval, decimal.Decimal)) and (
                isinstance(rval, float) or isinstance(rval, decimal.Decimal)
            ):
                if abs(float(lval) - float(rval)) > (atol + rtol * abs(float(rval))):
                    raise PySparkAssertionError(
                        error_class="DIFFERENT_PANDAS_DATAFRAME",
                        message_parameters={
                            "msg": msg,
                        },
                    )
    elif isinstance(left, pd.MultiIndex) and isinstance(right, pd.MultiIndex):
        msg = (
            "MultiIndices are not almost equal: "
            + "\n\nLeft:\n%s\n%s" % (left, left.dtype)
            + "\n\nRight:\n%s\n%s" % (right, right.dtype)
        )
        if len(left) != len(right):
            raise PySparkAssertionError(
                error_class="DIFFERENT_PANDAS_DATAFRAME",
                message_parameters={
                    "msg": msg,
                },
            )
        for lval, rval in zip(left, right):
            if (isinstance(lval, float) or isinstance(lval, decimal.Decimal)) and (
                isinstance(rval, float) or isinstance(rval, decimal.Decimal)
            ):
                if abs(float(lval) - float(rval)) > (atol + rtol * abs(float(rval))):
                    raise PySparkAssertionError(
                        error_class="DIFFERENT_PANDAS_DATAFRAME",
                        message_parameters={
                            "msg": msg,
                        },
                    )
    elif isinstance(left, pd.Index) and isinstance(right, pd.Index):
        msg = (
            "Indices are not almost equal: "
            + "\n\nLeft:\n%s\n%s" % (left, left.dtype)
            + "\n\nRight:\n%s\n%s" % (right, right.dtype)
        )
        if len(left) != len(right):
            raise PySparkAssertionError(
                error_class="DIFFERENT_PANDAS_DATAFRAME",
                message_parameters={
                    "msg": msg,
                },
            )
        for lnull, rnull in zip(left.isnull(), right.isnull()):
            if lnull != rnull:
                raise PySparkAssertionError(
                    error_class="DIFFERENT_PANDAS_DATAFRAME",
                    message_parameters={
                        "msg": msg,
                    },
                )
        for lval, rval in zip(left.dropna(), right.dropna()):
            if (isinstance(lval, float) or isinstance(lval, decimal.Decimal)) and (
                isinstance(rval, float) or isinstance(rval, decimal.Decimal)
            ):
                if abs(float(lval) - float(rval)) > (atol + rtol * abs(float(rval))):
                    raise PySparkAssertionError(
                        error_class="DIFFERENT_PANDAS_DATAFRAME",
                        message_parameters={
                            "msg": msg,
                        },
                    )
    else:
        raise ValueError("Unexpected values: (%s, %s)" % (left, right))


def assertPandasOnSparkEqual(
    actual: DataFrame,
    expected: Union[pd.DataFrame, DataFrame],
    check_exact=True,
    almost=False,
):
    r"""
    A util function to assert equality between actual (pandas-on-Spark DataFrame) and expected
    (pandas-on-Spark or pandas DataFrame).

    .. versionadded:: 3.5.0

    Parameters
    ----------
    actual: pandas-on-Spark DataFrame
        The DataFrame that is being compared or tested.
    expected: pandas-on-Spark or pandas DataFrame
        The expected DataFrame, for comparison with the actual result.
    check_exact: bool, optional
        A flag indicating whether to compare exact equality.
        If set to 'True' (default), the data is compared exactly.
        if set to 'False', the data is compared less precisely, following pandas assert_frame_equal
        (see documentation for more details).
    almost: bool, optional
        A flag indicating whether to use unittest `assertAlmostEqual` or `assertEqual`.
        If set to 'True', the comparison is delegated to `unittest`'s `assertAlmostEqual`
        (see documentation for more details).
        If set to 'False' (default), the data is compared exactly with `unittest`'s
        `assertEqual`.

    Examples
    --------
    >>> import pandas as pd
    >>> df1 = pd.DataFrame(data=[10, 20, 30], columns=["Numbers"])
    >>> df2 = pd.DataFrame(data=[10, 20, 30], columns=["Numbers"])
    >>> assertPandasOnSparkEqual(df1, df2)  # pass, DataFrames are equal
    >>> df1 = pd.Series([212.32, 100.0001])
    >>> df2 = pd.Series([212.32, 100.0])
    >>> assertPandasOnSparkEqual(df1, df2, check_exact=False)  # pass, DataFrames are approx equal
    """
    if isinstance(actual, (pd.DataFrame, pd.Series, pd.Index)):
        if almost:
            assertPandasDFAlmostEqual(actual, expected)
        else:
            assertPandasDFEqual(actual, expected, check_exact=check_exact)
    else:
        raise PySparkAssertionError(
            error_class="INVALID_TYPE_DF_EQUALITY_ARG",
            message_parameters={
                "expected_type": Union[pd.DataFrame, pd.Series, pd.Index],
                "arg_name": "left",
                "actual_type": type(actual),
            },
        )


class PandasOnSparkTestUtils:
    def convert_str_to_lambda(self, func):
        """
        This function coverts `func` str to lambda call
        """
        return lambda x: getattr(x, func)()

    def assertPandasEqual(self, left, right, check_exact=True):
        assertPandasDFEqual(left, right, check_exact)

    def assertPandasAlmostEqual(self, left, right):
        assertPandasDFAlmostEqual(left, right)

    def assert_eq(self, left, right, check_exact=True, almost=False):
        """
        Asserts if two arbitrary objects are equal or not. If given objects are Koalas DataFrame
        or Series, they are converted into pandas' and compared.

        :param left: object to compare
        :param right: object to compare
        :param check_exact: if this is False, the comparison is done less precisely.
        :param almost: if this is enabled, the comparison is delegated to `unittest`'s
                       `assertAlmostEqual`. See its documentation for more details.
        """
        import pandas as pd
        from pandas.api.types import is_list_like

        lobj = self._to_pandas(left)
        robj = self._to_pandas(right)
        if isinstance(lobj, (pd.DataFrame, pd.Series, pd.Index)):
            assertPandasOnSparkEqual(lobj, robj, check_exact, almost)
        elif is_list_like(lobj) and is_list_like(robj):
            self.assertTrue(len(left) == len(right))
            for litem, ritem in zip(left, right):
                self.assert_eq(litem, ritem, check_exact=check_exact, almost=almost)
        elif (lobj is not None and pd.isna(lobj)) and (robj is not None and pd.isna(robj)):
            pass
        else:
            if almost:
                self.assertAlmostEqual(lobj, robj)
            else:
                self.assertEqual(lobj, robj)

    @staticmethod
    def _to_pandas(obj):
        if isinstance(obj, (DataFrame, Series, Index)):
            return obj.to_pandas()
        else:
            return obj


class PandasOnSparkTestCase(ReusedSQLTestCase, PandasOnSparkTestUtils):
    @classmethod
    def setUpClass(cls):
        super(PandasOnSparkTestCase, cls).setUpClass()
        cls.spark.conf.set(SPARK_CONF_ARROW_ENABLED, True)


class TestUtils:
    @contextmanager
    def temp_dir(self):
        tmp = tempfile.mkdtemp()
        try:
            yield tmp
        finally:
            shutil.rmtree(tmp)

    @contextmanager
    def temp_file(self):
        with self.temp_dir() as tmp:
            yield tempfile.mkstemp(dir=tmp)[1]


class ComparisonTestBase(PandasOnSparkTestCase):
    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    @property
    def pdf(self):
        return self.psdf.to_pandas()


def compare_both(f=None, almost=True):

    if f is None:
        return functools.partial(compare_both, almost=almost)
    elif isinstance(f, bool):
        return functools.partial(compare_both, almost=f)

    @functools.wraps(f)
    def wrapped(self):
        if almost:
            compare = self.assertPandasAlmostEqual
        else:
            compare = self.assertPandasEqual

        for result_pandas, result_spark in zip(f(self, self.pdf), f(self, self.psdf)):
            compare(result_pandas, result_spark.to_pandas())

    return wrapped


@contextmanager
def assert_produces_warning(
    expected_warning=Warning,
    filter_level="always",
    check_stacklevel=True,
    raise_on_extra_warnings=True,
):
    """
    Context manager for running code expected to either raise a specific
    warning, or not raise any warnings. Verifies that the code raises the
    expected warning, and that it does not raise any other unexpected
    warnings. It is basically a wrapper around ``warnings.catch_warnings``.

    Notes
    -----
    Replicated from pandas/_testing/_warnings.py.

    Parameters
    ----------
    expected_warning : {Warning, False, None}, default Warning
        The type of Exception raised. ``exception.Warning`` is the base
        class for all warnings. To check that no warning is returned,
        specify ``False`` or ``None``.
    filter_level : str or None, default "always"
        Specifies whether warnings are ignored, displayed, or turned
        into errors.
        Valid values are:
        * "error" - turns matching warnings into exceptions
        * "ignore" - discard the warning
        * "always" - always emit a warning
        * "default" - print the warning the first time it is generated
          from each location
        * "module" - print the warning the first time it is generated
          from each module
        * "once" - print the warning the first time it is generated
    check_stacklevel : bool, default True
        If True, displays the line that called the function containing
        the warning to show were the function is called. Otherwise, the
        line that implements the function is displayed.
    raise_on_extra_warnings : bool, default True
        Whether extra warnings not of the type `expected_warning` should
        cause the test to fail.

    Examples
    --------
    >>> import warnings
    >>> with assert_produces_warning():
    ...     warnings.warn(UserWarning())
    ...
    >>> with assert_produces_warning(False): # doctest: +SKIP
    ...     warnings.warn(RuntimeWarning())
    ...
    Traceback (most recent call last):
        ...
    AssertionError: Caused unexpected warning(s): ['RuntimeWarning'].
    >>> with assert_produces_warning(UserWarning): # doctest: +SKIP
    ...     warnings.warn(RuntimeWarning())
    Traceback (most recent call last):
        ...
    AssertionError: Did not see expected warning of class 'UserWarning'
    ..warn:: This is *not* thread-safe.
    """
    __tracebackhide__ = True

    with warnings.catch_warnings(record=True) as w:

        saw_warning = False
        warnings.simplefilter(filter_level)
        yield w
        extra_warnings = []

        for actual_warning in w:
            if expected_warning and issubclass(actual_warning.category, expected_warning):
                saw_warning = True

                if check_stacklevel and issubclass(
                    actual_warning.category, (FutureWarning, DeprecationWarning)
                ):
                    from inspect import getframeinfo, stack

                    caller = getframeinfo(stack()[2][0])
                    msg = (
                        "Warning not set with correct stacklevel. ",
                        "File where warning is raised: {} != ".format(actual_warning.filename),
                        "{}. Warning message: {}".format(caller.filename, actual_warning.message),
                    )
                    assert actual_warning.filename == caller.filename, msg
            else:
                extra_warnings.append(
                    (
                        actual_warning.category.__name__,
                        actual_warning.message,
                        actual_warning.filename,
                        actual_warning.lineno,
                    )
                )
        if expected_warning:
            msg = "Did not see expected warning of class {}".format(repr(expected_warning.__name__))
            assert saw_warning, msg
        if raise_on_extra_warnings and extra_warnings:
            raise AssertionError("Caused unexpected warning(s): {}".format(repr(extra_warnings)))
