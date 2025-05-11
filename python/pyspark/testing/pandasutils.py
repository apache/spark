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
from contextlib import contextmanager
import decimal
from typing import Any, Dict, List, Optional, Set, Union


try:
    from pyspark.sql.pandas.utils import require_minimum_pandas_version

    require_minimum_pandas_version()
    import pandas as pd
except ImportError:
    pass

import pyspark.pandas as ps
from pyspark.pandas.frame import DataFrame
from pyspark.pandas.indexes import Index
from pyspark.pandas.series import Series
from pyspark.pandas.utils import SPARK_CONF_ARROW_ENABLED
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.errors import PySparkAssertionError


def _assert_pandas_equal(
    left: Union[pd.DataFrame, pd.Series, pd.Index],
    right: Union[pd.DataFrame, pd.Series, pd.Index],
    checkExact: bool,
):
    from pandas.core.dtypes.common import is_numeric_dtype
    from pandas.testing import assert_frame_equal, assert_index_equal, assert_series_equal

    if isinstance(left, pd.DataFrame) and isinstance(right, pd.DataFrame):
        try:
            kwargs = dict(check_freq=False)

            assert_frame_equal(
                left,
                right,
                check_index_type=("equiv" if len(left.index) > 0 else False),
                check_column_type=("equiv" if len(left.columns) > 0 else False),
                check_exact=checkExact,
                **kwargs,
            )
        except AssertionError:
            raise PySparkAssertionError(
                errorClass="DIFFERENT_PANDAS_DATAFRAME",
                messageParameters={
                    "left": left.to_string(),
                    "left_dtype": str(left.dtypes),
                    "right": right.to_string(),
                    "right_dtype": str(right.dtypes),
                },
            )
    elif isinstance(left, pd.Series) and isinstance(right, pd.Series):
        try:
            kwargs = dict(check_freq=False)
            assert_series_equal(
                left,
                right,
                check_index_type=("equiv" if len(left.index) > 0 else False),
                check_exact=checkExact,
                **kwargs,
            )
        except AssertionError:
            raise PySparkAssertionError(
                errorClass="DIFFERENT_PANDAS_SERIES",
                messageParameters={
                    "left": left.to_string(),
                    "left_dtype": str(left.dtype),
                    "right": right.to_string(),
                    "right_dtype": str(right.dtype),
                },
            )
    elif isinstance(left, pd.Index) and isinstance(right, pd.Index):
        try:
            assert_index_equal(left, right, check_exact=checkExact)
        except AssertionError:
            raise PySparkAssertionError(
                errorClass="DIFFERENT_PANDAS_INDEX",
                messageParameters={
                    "left": left,
                    "left_dtype": str(left.dtype),
                    "right": right,
                    "right_dtype": str(right.dtype),
                },
            )
    else:
        raise ValueError("Unexpected values: (%s, %s)" % (left, right))


def _assert_pandas_almost_equal(
    left: Union[pd.DataFrame, pd.Series, pd.Index],
    right: Union[pd.DataFrame, pd.Series, pd.Index],
    rtol: float = 1e-5,
    atol: float = 1e-8,
):
    """
    This function checks if given pandas objects approximately same,
    which means the conditions below:
      - Both objects are nullable
      - Compare decimals and floats, where two values a and b are approximately equal
        if they satisfy the following formula:
        absolute(a - b) <= (atol + rtol * absolute(b))
        where rtol=1e-5 and atol=1e-8 by default
    """

    def compare_vals_approx(val1, val2):
        # compare vals for approximate equality
        if isinstance(lval, (float, decimal.Decimal)) or isinstance(rval, (float, decimal.Decimal)):
            if abs(float(lval) - float(rval)) > (atol + rtol * abs(float(rval))):
                return False
        elif val1 != val2:
            return False
        return True

    if isinstance(left, pd.DataFrame) and isinstance(right, pd.DataFrame):
        if left.shape != right.shape:
            raise PySparkAssertionError(
                errorClass="DIFFERENT_PANDAS_DATAFRAME",
                messageParameters={
                    "left": left.to_string(),
                    "left_dtype": str(left.dtypes),
                    "right": right.to_string(),
                    "right_dtype": str(right.dtypes),
                },
            )
        for lcol, rcol in zip(left.columns, right.columns):
            if lcol != rcol:
                raise PySparkAssertionError(
                    errorClass="DIFFERENT_PANDAS_DATAFRAME",
                    messageParameters={
                        "left": left.to_string(),
                        "left_dtype": str(left.dtypes),
                        "right": right.to_string(),
                        "right_dtype": str(right.dtypes),
                    },
                )
            for lnull, rnull in zip(left[lcol].isnull(), right[rcol].isnull()):
                if lnull != rnull:
                    raise PySparkAssertionError(
                        errorClass="DIFFERENT_PANDAS_DATAFRAME",
                        messageParameters={
                            "left": left.to_string(),
                            "left_dtype": str(left.dtypes),
                            "right": right.to_string(),
                            "right_dtype": str(right.dtypes),
                        },
                    )
            for lval, rval in zip(left[lcol].dropna(), right[rcol].dropna()):
                if not compare_vals_approx(lval, rval):
                    raise PySparkAssertionError(
                        errorClass="DIFFERENT_PANDAS_DATAFRAME",
                        messageParameters={
                            "left": left.to_string(),
                            "left_dtype": str(left.dtypes),
                            "right": right.to_string(),
                            "right_dtype": str(right.dtypes),
                        },
                    )
        if left.columns.names != right.columns.names:
            raise PySparkAssertionError(
                errorClass="DIFFERENT_PANDAS_DATAFRAME",
                messageParameters={
                    "left": left.to_string(),
                    "left_dtype": str(left.dtypes),
                    "right": right.to_string(),
                    "right_dtype": str(right.dtypes),
                },
            )
    elif isinstance(left, pd.Series) and isinstance(right, pd.Series):
        if left.name != right.name or len(left) != len(right):
            raise PySparkAssertionError(
                errorClass="DIFFERENT_PANDAS_SERIES",
                messageParameters={
                    "left": left.to_string(),
                    "left_dtype": str(left.dtype),
                    "right": right.to_string(),
                    "right_dtype": str(right.dtype),
                },
            )
        for lnull, rnull in zip(left.isnull(), right.isnull()):
            if lnull != rnull:
                raise PySparkAssertionError(
                    errorClass="DIFFERENT_PANDAS_SERIES",
                    messageParameters={
                        "left": left.to_string(),
                        "left_dtype": str(left.dtype),
                        "right": right.to_string(),
                        "right_dtype": str(right.dtype),
                    },
                )
        for lval, rval in zip(left.dropna(), right.dropna()):
            if not compare_vals_approx(lval, rval):
                raise PySparkAssertionError(
                    errorClass="DIFFERENT_PANDAS_SERIES",
                    messageParameters={
                        "left": left.to_string(),
                        "left_dtype": str(left.dtype),
                        "right": right.to_string(),
                        "right_dtype": str(right.dtype),
                    },
                )
    elif isinstance(left, pd.MultiIndex) and isinstance(right, pd.MultiIndex):
        if len(left) != len(right):
            raise PySparkAssertionError(
                errorClass="DIFFERENT_PANDAS_MULTIINDEX",
                messageParameters={
                    "left": left,
                    "left_dtype": str(left.dtype),
                    "right": right,
                    "right_dtype": str(right.dtype),
                },
            )
        for lval, rval in zip(left, right):
            if not compare_vals_approx(lval, rval):
                raise PySparkAssertionError(
                    errorClass="DIFFERENT_PANDAS_MULTIINDEX",
                    messageParameters={
                        "left": left,
                        "left_dtype": str(left.dtype),
                        "right": right,
                        "right_dtype": str(right.dtype),
                    },
                )
    elif isinstance(left, pd.Index) and isinstance(right, pd.Index):
        if len(left) != len(right):
            raise PySparkAssertionError(
                errorClass="DIFFERENT_PANDAS_INDEX",
                messageParameters={
                    "left": left,
                    "left_dtype": str(left.dtype),
                    "right": right,
                    "right_dtype": str(right.dtype),
                },
            )
        for lnull, rnull in zip(left.isnull(), right.isnull()):
            if lnull != rnull:
                raise PySparkAssertionError(
                    errorClass="DIFFERENT_PANDAS_INDEX",
                    messageParameters={
                        "left": left,
                        "left_dtype": str(left.dtype),
                        "right": right,
                        "right_dtype": str(right.dtype),
                    },
                )
        for lval, rval in zip(left.dropna(), right.dropna()):
            if not compare_vals_approx(lval, rval):
                raise PySparkAssertionError(
                    errorClass="DIFFERENT_PANDAS_INDEX",
                    messageParameters={
                        "left": left,
                        "left_dtype": str(left.dtype),
                        "right": right,
                        "right_dtype": str(right.dtype),
                    },
                )
    else:
        if not isinstance(left, (pd.DataFrame, pd.Series, pd.Index)):
            raise PySparkAssertionError(
                errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
                messageParameters={
                    "expected_type": f"{pd.DataFrame.__name__}, "
                    f"{pd.Series.__name__}, "
                    f"{pd.Index.__name__}, ",
                    "arg_name": "left",
                    "actual_type": type(left),
                },
            )
        elif not isinstance(right, (pd.DataFrame, pd.Series, pd.Index)):
            raise PySparkAssertionError(
                errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
                messageParameters={
                    "expected_type": f"{pd.DataFrame.__name__}, "
                    f"{pd.Series.__name__}, "
                    f"{pd.Index.__name__}, ",
                    "arg_name": "right",
                    "actual_type": type(right),
                },
            )


class PandasOnSparkTestUtils:
    def convert_str_to_lambda(self, func: str):
        """
        This function converts `func` str to lambda call
        """
        return lambda x: getattr(x, func)()

    def sort_index_with_values(self, pobj: Any):
        assert isinstance(pobj, (pd.Series, pd.DataFrame, ps.Series, ps.DataFrame))

        if isinstance(pobj, (ps.Series, ps.DataFrame)):
            if isinstance(pobj, ps.Series):
                psdf = pobj._psdf[[pobj.name]]
            else:
                psdf = pobj
            scols = psdf._internal.index_spark_columns + psdf._internal.data_spark_columns
            sorted = psdf._sort(
                by=scols,
                ascending=True,
                na_position="last",
            )
            if isinstance(pobj, ps.Series):
                from pyspark.pandas.series import first_series

                return first_series(sorted)
            else:
                return sorted
        else:
            # quick-sort values and then stable-sort index
            if isinstance(pobj, pd.Series):
                return pobj.sort_values(
                    ascending=True,
                    na_position="last",
                ).sort_index(
                    ascending=True,
                    na_position="last",
                    kind="mergesort",
                )
            else:
                return pobj.sort_values(
                    by=list(pobj.columns),
                    ascending=True,
                    na_position="last",
                ).sort_index(
                    ascending=True,
                    na_position="last",
                    kind="mergesort",
                )

    def assertPandasEqual(self, left: Any, right: Any, check_exact: bool = True):
        _assert_pandas_equal(left, right, check_exact)

    def assertPandasAlmostEqual(
        self,
        left: Any,
        right: Any,
        rtol: float = 1e-5,
        atol: float = 1e-8,
    ):
        _assert_pandas_almost_equal(left, right, rtol=rtol, atol=atol)

    def assert_eq(
        self,
        left: Any,
        right: Any,
        check_exact: bool = True,
        almost: bool = False,
        rtol: float = 1e-5,
        atol: float = 1e-8,
        check_row_order: bool = True,
    ):
        """
        Asserts if two arbitrary objects are equal or not. If given objects are
        Pandas-on-Spark DataFrame or Series, they are converted into pandas' and compared.

        :param left: object to compare
        :param right: object to compare
        :param check_exact: if this is False, the comparison is done less precisely.
        :param almost: if this is enabled, the comparison asserts approximate equality
            for float and decimal values, where two values a and b are approximately equal
            if they satisfy the following formula:
            absolute(a - b) <= (atol + rtol * absolute(b))
        :param rtol: The relative tolerance, used in asserting approximate equality for
            float values. Set to 1e-5 by default.
        :param atol: The absolute tolerance, used in asserting approximate equality for
            float values in actual and expected. Set to 1e-8 by default.
        :param check_row_order: A flag indicating whether the order of rows should be considered
            in the comparison. If set to False, row order will be ignored.
        """
        import pandas as pd
        from pandas.api.types import is_list_like

        # for pandas-on-Spark DataFrames, allow choice to ignore row order
        if isinstance(left, (ps.DataFrame, ps.Series, ps.Index)):
            if left is None and right is None:
                return True
            elif left is None or right is None:
                return False

            if not isinstance(left, (DataFrame, Series, Index)):
                raise PySparkAssertionError(
                    errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
                    messageParameters={
                        "expected_type": f"{DataFrame.__name__}, {Series.__name__}, "
                        f"{Index.__name__}",
                        "arg_name": "actual",
                        "actual_type": type(left),
                    },
                )
            elif not isinstance(
                right, (DataFrame, pd.DataFrame, Series, pd.Series, Index, pd.Index)
            ):
                raise PySparkAssertionError(
                    errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
                    messageParameters={
                        "expected_type": f"{DataFrame.__name__}, "
                        f"{pd.DataFrame.__name__}, "
                        f"{Series.__name__}, "
                        f"{pd.Series.__name__}, "
                        f"{Index.__name__}"
                        f"{pd.Index.__name__}, ",
                        "arg_name": "expected",
                        "actual_type": type(right),
                    },
                )
            else:
                if not isinstance(left, (pd.DataFrame, pd.Index, pd.Series)):
                    left = left.to_pandas()
                if not isinstance(right, (pd.DataFrame, pd.Index, pd.Series)):
                    right = right.to_pandas()

                if not check_row_order:
                    if isinstance(left, pd.DataFrame) and len(left.columns) > 0:
                        left = left.sort_values(by=left.columns[0], ignore_index=True)
                    if isinstance(right, pd.DataFrame) and len(right.columns) > 0:
                        right = right.sort_values(by=right.columns[0], ignore_index=True)

                if almost:
                    _assert_pandas_almost_equal(left, right, rtol=rtol, atol=atol)
                else:
                    _assert_pandas_equal(left, right, checkExact=check_exact)

        lobj = self._to_pandas(left)
        robj = self._to_pandas(right)
        if isinstance(lobj, (pd.DataFrame, pd.Series, pd.Index)):
            if almost:
                _assert_pandas_almost_equal(lobj, robj, rtol=rtol, atol=atol)
            else:
                _assert_pandas_equal(lobj, robj, checkExact=check_exact)
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
    def _to_pandas(obj: Any):
        if isinstance(obj, (DataFrame, Series, Index)):
            return obj.to_pandas()
        else:
            return obj

    def assert_column_unique(
        self,
        df: Union[pd.DataFrame, ps.DataFrame],
        columns: Union[str, List[str]],
        message: Optional[str] = None,
    ) -> None:
        """
        Assert that the specified column(s) in a pandas or pandas-on-Spark DataFrame
        contain unique values.

        Parameters
        ----------
        df : pandas.DataFrame or pyspark.pandas.DataFrame
            The DataFrame to check for uniqueness.
        columns : str or list of str
            The column name(s) to check for uniqueness. Can be a single column name or a list
            of column names. If a list is provided, the combination of values across these
            columns is checked for uniqueness.
        message : str, optional
            Custom error message to include if the assertion fails.

        Raises
        ------
        AssertionError
            If the specified column(s) contain duplicate values.
        """
        # Convert to pandas if it's a pandas-on-Spark DataFrame
        if not isinstance(df, pd.DataFrame):
            df = self._to_pandas(df)

        # Validate columns parameter
        if isinstance(columns, str):
            columns = [columns]

        # Check if all columns exist in the DataFrame
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"The following columns do not exist in the DataFrame: {missing_columns}"
            )

        # Check for duplicates
        if len(columns) == 1:
            # For a single column, use duplicated() method
            duplicates = df[df[columns[0]].duplicated(keep=False)]
            if not duplicates.empty:
                # Get examples of duplicates
                duplicate_examples = duplicates.head(5)
                examples_str = "\n".join([str(row) for _, row in duplicate_examples.iterrows()])

                # Create error message
                error_msg = f"Column '{columns[0]}' contains duplicate values.\n"
                error_msg += f"Examples of duplicates:\n{examples_str}"

                if message:
                    error_msg += f"\n{message}"

                raise AssertionError(error_msg)
        else:
            # For multiple columns, use duplicated() with subset parameter
            duplicates = df[df.duplicated(subset=columns, keep=False)]
            if not duplicates.empty:
                # Get examples of duplicates
                duplicate_examples = duplicates.head(5)
                examples_str = "\n".join([str(row) for _, row in duplicate_examples.iterrows()])

                # Create error message
                error_msg = f"Columns {columns} contain duplicate values.\n"
                error_msg += f"Examples of duplicates:\n{examples_str}"

                if message:
                    error_msg += f"\n{message}"

                raise AssertionError(error_msg)

    def assert_column_non_null(
        self,
        df: Union[pd.DataFrame, ps.DataFrame],
        columns: Union[str, List[str]],
        message: Optional[str] = None,
    ) -> None:
        """
        Assert that the specified column(s) in a pandas or pandas-on-Spark DataFrame
        do not contain any null values.

        Parameters
        ----------
        df : pandas.DataFrame or pyspark.pandas.DataFrame
            The DataFrame to check for null values.
        columns : str or list of str
            The column name(s) to check for null values. Can be a single column name or a list
            of column names.
        message : str, optional
            Custom error message to include if the assertion fails.

        Raises
        ------
        AssertionError
            If any of the specified columns contain null values.
        """
        # Convert to pandas if it's a pandas-on-Spark DataFrame
        if not isinstance(df, pd.DataFrame):
            df = self._to_pandas(df)

        # Validate columns parameter
        if isinstance(columns, str):
            columns = [columns]

        # Check if all columns exist in the DataFrame
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"The following columns do not exist in the DataFrame: {missing_columns}"
            )

        # Check for null values
        null_counts = {}
        for column in columns:
            # Count null values in the column
            null_count = df[column].isna().sum()
            if null_count > 0:
                null_counts[column] = null_count

        if null_counts:
            # Create error message
            column_desc = f"Column '{columns[0]}'" if len(columns) == 1 else f"Columns {columns}"

            plural = "s" if len(columns) == 1 else ""
            error_msg = f"{column_desc} contain{plural} null values.\n"
            error_msg += "Null counts by column:\n"
            for col_name, count in null_counts.items():
                error_msg += f"- {col_name}: {count} null value{'s' if count != 1 else ''}\n"

            if message:
                error_msg += f"\n{message}"

            raise AssertionError(error_msg)

    def assert_column_values_in_set(
        self,
        df: Union[pd.DataFrame, ps.DataFrame],
        columns: Union[str, List[str]],
        accepted_values: Union[Set[Any], List[Any], Dict[str, Set[Any]]],
        message: Optional[str] = None,
    ) -> None:
        """
        Assert that all values in the specified column(s) of a pandas or pandas-on-Spark DataFrame
        are within a given set of accepted values.

        Parameters
        ----------
        df : pandas.DataFrame or pyspark.pandas.DataFrame
            The DataFrame to check.
        columns : str or list of str
            The column name(s) to check for values.
        accepted_values : set or list or tuple or dict
            The set of accepted values for the column(s). If columns is a list and accepted_values
            is a dict, the keys in the dict should correspond to the column names, and the values
            should be the sets of accepted values for each column. If columns is a list and
            accepted_values is not a dict, the same set of accepted values will be used for all
            columns.
        message : str, optional
            Custom error message to include if the assertion fails.

        Raises
        ------
        AssertionError
            If any of the specified columns contain values not in the accepted set.
        """
        # Convert to pandas if it's a pandas-on-Spark DataFrame
        if not isinstance(df, pd.DataFrame):
            df = self._to_pandas(df)

        # Validate columns parameter
        if isinstance(columns, str):
            columns = [columns]

        # Check if all columns exist in the DataFrame
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"The following columns do not exist in the DataFrame: {missing_columns}"
            )

        # Normalize accepted_values to a dictionary mapping column names to sets of accepted values
        if not isinstance(accepted_values, dict):
            # Convert to set if it's not already
            if not isinstance(accepted_values, set):
                accepted_values = set(accepted_values)
            # Use the same set of accepted values for all columns
            accepted_values = {col: accepted_values for col in columns}
        else:
            # Verify that all columns have corresponding accepted values
            missing_columns = [col for col in columns if col not in accepted_values]
            if missing_columns:
                raise ValueError(
                    f"""The following columns do not have accepted values specified: 
                    {missing_columns}"""
                )

            # Convert values to sets if they're not already
            for col, values in accepted_values.items():
                if not isinstance(values, set):
                    accepted_values[col] = set(values)

        # Check each column for invalid values
        invalid_columns = {}

        for column in columns:
            # Get the set of accepted values for this column
            column_accepted_values = accepted_values[column]

            # Find values that are not in the accepted set (excluding nulls)
            invalid_mask = ~df[column].isin(list(column_accepted_values)) & ~df[column].isna()
            invalid_values = df.loc[invalid_mask, column]

            if not invalid_values.empty:
                # Get examples of invalid values (limit to 10 for readability)
                invalid_examples = invalid_values.drop_duplicates().head(10).tolist()
                invalid_count = len(invalid_values)

                invalid_columns[column] = {
                    "count": invalid_count,
                    "examples": invalid_examples,
                    "accepted": column_accepted_values,
                }

        if invalid_columns:
            # Create error message
            column_desc = f"Column '{columns[0]}'" if len(columns) == 1 else f"Columns {columns}"
            plural = "s" if len(columns) == 1 else ""
            error_msg = f"{column_desc} contain{plural} values not in the accepted set.\n"

            for column, details in invalid_columns.items():
                error_msg += f"\nColumn '{column}':\n"
                error_msg += f"  Accepted values: {details['accepted']}\n"
                error_msg += f"  Invalid values found: {details['examples']}\n"
                error_msg += f"  Total invalid values: {details['count']}\n"

            if message:
                error_msg += f"\n{message}"

            raise AssertionError(error_msg)

    def assert_referential_integrity(
        self,
        source_df: Union[pd.DataFrame, ps.DataFrame],
        source_column: str,
        target_df: Union[pd.DataFrame, ps.DataFrame],
        target_column: str,
        message: Optional[str] = None,
    ) -> None:
        """
        Assert that all non-null values in a column of one DataFrame exist in a column of
        another DataFrame.

        This function checks referential integrity between two DataFrames, similar to a foreign
        key constraint in a relational database. It verifies that all non-null values in the
        source column exist in the target column.

        Parameters
        ----------
        source_df : pandas.DataFrame or pyspark.pandas.DataFrame
            The DataFrame containing the foreign key column to check.
        source_column : str
            The name of the column in source_df to check (foreign key).
        target_df : pandas.DataFrame or pyspark.pandas.DataFrame
            The DataFrame containing the primary key column to check against.
        target_column : str
            The name of the column in target_df to check against (primary key).
        message : str, optional
            Custom error message to include if the assertion fails.

        Raises
        ------
        AssertionError
            If any non-null values in the source column do not exist in the target column.
        ValueError
            If the column parameters are invalid or if specified columns don't exist in the
            DataFrames.
        """
        # Convert to pandas if they're pandas-on-Spark DataFrames
        if not isinstance(source_df, pd.DataFrame):
            source_df = self._to_pandas(source_df)

        if not isinstance(target_df, pd.DataFrame):
            target_df = self._to_pandas(target_df)

        # Validate source_column
        if not source_column or not isinstance(source_column, str):
            raise ValueError("The 'source_column' parameter must be a non-empty string.")

        # Validate target_column
        if not target_column or not isinstance(target_column, str):
            raise ValueError("The 'target_column' parameter must be a non-empty string.")

        # Check if source_column exists in source_df
        if source_column not in source_df.columns:
            raise ValueError(f"Column '{source_column}' does not exist in the source DataFrame.")

        # Check if target_column exists in target_df
        if target_column not in target_df.columns:
            raise ValueError(f"Column '{target_column}' does not exist in the target DataFrame.")

        # Get all non-null values from the source column
        source_values = source_df[source_column].dropna()

        # Get all values from the target column
        target_values = set(target_df[target_column].dropna())

        # Find values in source that are not in target
        missing_mask = ~source_values.isin(target_values)
        missing_values = source_values[missing_mask]

        if not missing_values.empty:
            # Get examples of missing values (limit to 10 for readability)
            missing_examples = missing_values.drop_duplicates().head(10).tolist()
            missing_count = len(missing_values)

            # Create error message
            error_msg = (
                f"Column '{source_column}' contains values not found in "
                f"target column '{target_column}'.\n"
            )
            error_msg += f"Missing values: {missing_examples}" + (
                " (showing first 10 only)" if len(missing_examples) >= 10 else ""
            )
            error_msg += f"\nTotal missing values: {missing_count}"

            if message:
                error_msg += f"\n{message}"

            raise AssertionError(error_msg)


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
