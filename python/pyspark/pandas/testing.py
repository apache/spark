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

"""
Public testing utility functions.
"""
from typing import Literal, Union
import pyspark.pandas as ps

try:
    from pyspark.sql.pandas.utils import require_minimum_pandas_version

    require_minimum_pandas_version()
    import pandas as pd
except ImportError:
    pass


def assert_frame_equal(
    left: Union[ps.DataFrame, pd.DataFrame],
    right: Union[ps.DataFrame, pd.DataFrame],
    check_dtype: bool = True,
    check_index_type: Union[bool, Literal["equiv"]] = "equiv",
    check_column_type: Union[bool, Literal["equiv"]] = "equiv",
    check_frame_type: bool = True,
    check_names: bool = True,
    by_blocks: bool = False,
    check_exact: bool = False,
    check_datetimelike_compat: bool = False,
    check_categorical: bool = True,
    check_like: bool = False,
    check_freq: bool = True,
    check_flags: bool = True,
    rtol: float = 1.0e-5,
    atol: float = 1.0e-8,
    obj: str = "DataFrame",
) -> None:
    """
    Check that left and right DataFrame are equal.

    This function is intended to compare two DataFrames and output any
    differences. It is mostly intended for use in unit tests.
    Additional parameters allow varying the strictness of the
    equality checks performed.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    left : DataFrame
        First DataFrame to compare.
    right : DataFrame
        Second DataFrame to compare.
    check_dtype : bool, default True
        Whether to check the DataFrame dtype is identical.
    check_index_type : bool or {'equiv'}, default 'equiv'
        Whether to check the Index class, dtype and inferred_type
        are identical.
    check_column_type : bool or {'equiv'}, default 'equiv'
        Whether to check the columns class, dtype and inferred_type
        are identical. Is passed as the ``exact`` argument of
        :func:`assert_index_equal`.
    check_frame_type : bool, default True
        Whether to check the DataFrame class is identical.
    check_names : bool, default True
        Whether to check that the `names` attribute for both the `index`
        and `column` attributes of the DataFrame is identical.
    by_blocks : bool, default False
        Specify how to compare internal data. If False, compare by columns.
        If True, compare by blocks.
    check_exact : bool, default False
        Whether to compare number exactly.
    check_datetimelike_compat : bool, default False
        Compare datetime-like which is comparable ignoring dtype.
    check_categorical : bool, default True
        Whether to compare internal Categorical exactly.
    check_like : bool, default False
        If True, ignore the order of index & columns.
        Note: index labels must match their respective rows
        (same as in columns) - same labels must be with the same data.
    check_freq : bool, default True
        Whether to check the `freq` attribute on a DatetimeIndex or TimedeltaIndex.
    check_flags : bool, default True
        Whether to check the `flags` attribute.
    rtol : float, default 1e-5
        Relative tolerance. Only used when check_exact is False.
    atol : float, default 1e-8
        Absolute tolerance. Only used when check_exact is False.
    obj : str, default 'DataFrame'
        Specify object name being compared, internally used to show appropriate
        assertion message.

    See Also
    --------
    assert_series_equal : Equivalent method for asserting Series equality.
    DataFrame.equals : Check DataFrame equality.

    Examples
    --------
    This example shows comparing two DataFrames that are equal
    but with columns of differing dtypes.

    >>> from pyspark.pandas.testing import assert_frame_equal
    >>> df1 = ps.DataFrame({'a': [1, 2], 'b': [3, 4]})
    >>> df2 = ps.DataFrame({'a': [1, 2], 'b': [3.0, 4.0]})

    df1 equals itself.

    >>> assert_frame_equal(df1, df1)

    df1 differs from df2 as column 'b' is of a different type.

    >>> assert_frame_equal(df1, df2)
    Traceback (most recent call last):
    ...
    AssertionError: Attributes of DataFrame.iloc[:, 1] (column name="b") are different
    <BLANKLINE>
    Attribute "dtype" are different
    [left]:  int64
    [right]: float64

    Ignore differing dtypes in columns with check_dtype.

    >>> assert_frame_equal(df1, df2, check_dtype=False)
    """
    if isinstance(left, ps.DataFrame):
        left = left.to_pandas()
    if isinstance(right, ps.DataFrame):
        right = right.to_pandas()

    pd.testing.assert_frame_equal(
        left,
        right,
        check_dtype=check_dtype,
        check_index_type=check_index_type,  # type: ignore[arg-type]
        check_column_type=check_column_type,  # type: ignore[arg-type]
        check_frame_type=check_frame_type,
        check_names=check_names,
        by_blocks=by_blocks,
        check_exact=check_exact,
        check_datetimelike_compat=check_datetimelike_compat,
        check_categorical=check_categorical,
        check_like=check_like,
        check_freq=check_freq,
        check_flags=check_flags,
        rtol=rtol,
        atol=atol,
        obj=obj,
    )


def assert_series_equal(
    left: Union[ps.Series, pd.Series],
    right: Union[ps.Series, pd.Series],
    check_dtype: bool = True,
    check_index_type: Union[bool, Literal["equiv"]] = "equiv",
    check_series_type: bool = True,
    check_names: bool = True,
    check_exact: bool = False,
    check_datetimelike_compat: bool = False,
    check_categorical: bool = True,
    check_category_order: bool = True,
    check_freq: bool = True,
    check_flags: bool = True,
    rtol: float = 1.0e-5,
    atol: float = 1.0e-8,
    obj: str = "Series",
    *,
    check_index: bool = True,
    check_like: bool = False,
) -> None:
    """
    Check that left and right Series are equal.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    left : Series
    right : Series
    check_dtype : bool, default True
        Whether to check the Series dtype is identical.
    check_index_type : bool or {'equiv'}, default 'equiv'
        Whether to check the Index class, dtype and inferred_type
        are identical.
    check_series_type : bool, default True
         Whether to check the Series class is identical.
    check_names : bool, default True
        Whether to check the Series and Index names attribute.
    check_exact : bool, default False
        Whether to compare number exactly.
    check_datetimelike_compat : bool, default False
        Compare datetime-like which is comparable ignoring dtype.
    check_categorical : bool, default True
        Whether to compare internal Categorical exactly.
    check_category_order : bool, default True
        Whether to compare category order of internal Categoricals.
    check_freq : bool, default True
        Whether to check the `freq` attribute on a DatetimeIndex or TimedeltaIndex.
    check_flags : bool, default True
        Whether to check the `flags` attribute.
    rtol : float, default 1e-5
        Relative tolerance. Only used when check_exact is False.
    atol : float, default 1e-8
        Absolute tolerance. Only used when check_exact is False.
    obj : str, default 'Series'
        Specify object name being compared, internally used to show appropriate
        assertion message.
    check_index : bool, default True
        Whether to check index equivalence. If False, then compare only values.
    check_like : bool, default False
        If True, ignore the order of the index. Must be False if check_index is False.
        Note: same labels must be with the same data.

    Examples
    --------
    >>> from pyspark.pandas import testing as tm
    >>> a = ps.Series([1, 2, 3, 4])
    >>> b = ps.Series([1, 2, 3, 4])
    >>> tm.assert_series_equal(a, b)
    """
    if isinstance(left, ps.Series):
        left = left.to_pandas()
    if isinstance(right, ps.Series):
        right = right.to_pandas()

    pd.testing.assert_series_equal(  # type: ignore[call-arg]
        left,
        right,
        check_dtype=check_dtype,
        check_index_type=check_index_type,  # type: ignore[arg-type]
        check_series_type=check_series_type,
        check_names=check_names,
        check_exact=check_exact,
        check_datetimelike_compat=check_datetimelike_compat,
        check_categorical=check_categorical,
        check_category_order=check_category_order,
        check_freq=check_freq,
        check_flags=check_flags,
        rtol=rtol,  # type: ignore[arg-type]
        atol=atol,  # type: ignore[arg-type]
        obj=obj,
        check_index=check_index,
        check_like=check_like,
    )


def assert_index_equal(
    left: Union[ps.Index, pd.Index],
    right: Union[ps.Index, pd.Index],
    exact: Union[bool, Literal["equiv"]] = "equiv",
    check_names: bool = True,
    check_exact: bool = True,
    check_categorical: bool = True,
    check_order: bool = True,
    rtol: float = 1.0e-5,
    atol: float = 1.0e-8,
    obj: str = "Index",
) -> None:
    """
    Check that left and right Index are equal.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    left : Index
    right : Index
    exact : bool or {'equiv'}, default 'equiv'
        Whether to check the Index class, dtype and inferred_type
        are identical. If 'equiv', then RangeIndex can be substituted for
        Index with an int64 dtype as well.
    check_names : bool, default True
        Whether to check the names attribute.
    check_exact : bool, default True
        Whether to compare number exactly.
    check_categorical : bool, default True
        Whether to compare internal Categorical exactly.
    check_order : bool, default True
        Whether to compare the order of index entries as well as their values.
        If True, both indexes must contain the same elements, in the same order.
        If False, both indexes must contain the same elements, but in any order.
    rtol : float, default 1e-5
        Relative tolerance. Only used when check_exact is False.
    atol : float, default 1e-8
        Absolute tolerance. Only used when check_exact is False.
    obj : str, default 'Index'
        Specify object name being compared, internally used to show appropriate
        assertion message.

    Examples
    --------
    >>> from pyspark.pandas import testing as tm
    >>> a = ps.Index([1, 2, 3])
    >>> b = ps.Index([1, 2, 3])
    >>> tm.assert_index_equal(a, b)
    """
    if isinstance(left, ps.Index):
        left = left.to_pandas()
    if isinstance(right, ps.Index):
        right = right.to_pandas()

    pd.testing.assert_index_equal(  # type: ignore[call-arg]
        left,
        right,
        exact=exact,
        check_names=check_names,
        check_exact=check_exact,
        check_categorical=check_categorical,
        check_order=check_order,
        rtol=rtol,
        atol=atol,
        obj=obj,
    )
