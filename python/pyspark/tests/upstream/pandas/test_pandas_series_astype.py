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
Golden tests for pandas.Series.astype, without a Spark session.

PySpark uses astype in _create_converter_to_pandas and convert_pandas_using_numpy_type
(sql/pandas/types.py), for categorical inputs in PandasToArrowConversion.convert
(sql/conversion.py), and to convert Arrow-backed integers to nullable pandas integers.
The target dtypes also cover casts exposed by pandas-on-Spark.

Separate goldens record values, dtypes, and exception classes for the default call and
copy=False. They measure conversion results, not buffer ownership. The input column records
the source values and dtype; string storage and categorical metadata are included explicitly.

Regenerate with:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/pandas/test_pandas_series_astype.py

Install tabulate to regenerate Markdown too. Measured version differences belong in
overrides rather than replacing the baseline golden.
"""

import datetime
import unittest
import warnings
from decimal import Decimal

from pyspark.loose_version import LooseVersion
from pyspark.testing.goldenutils import GoldenFileTestMixin
from pyspark.testing.utils import (
    have_numpy,
    have_pandas,
    have_pyarrow,
    numpy_requirement_message,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_numpy:
    import numpy as np
if have_pandas:
    import pandas as pd
if have_pyarrow:
    import pyarrow as pa


COL_INPUT = "input"


@unittest.skipIf(
    not have_numpy or not have_pandas or not have_pyarrow,
    numpy_requirement_message or pandas_requirement_message or pyarrow_requirement_message,
)
class PandasSeriesAstypeTests(GoldenFileTestMixin, unittest.TestCase):
    @staticmethod
    def _source_series():
        rows = {}

        # Native, nullable, and Arrow-backed integers, in Spark's four widths.
        for bits in (8, 16, 32, 64):
            rows[f"int{bits}:standard"] = pd.Series([0, 1, -1], dtype=f"int{bits}")
            rows[f"Int{bits}:null"] = pd.Series([0, 1, None], dtype=f"Int{bits}")
            rows[f"int{bits}[pyarrow]:null"] = pd.Series(
                [0, 1, None], dtype=pd.ArrowDtype(getattr(pa, f"int{bits}")())
            )
        rows["int64:bounds"] = pd.Series(
            [np.iinfo(np.int64).min, np.iinfo(np.int64).max], dtype="int64"
        )

        for bits in (32, 64):
            rows[f"float{bits}:fractional"] = pd.Series([-1.5, -0.0, 1.5], dtype=f"float{bits}")
            rows[f"Float{bits}:null"] = pd.Series([0.0, 1.5, None], dtype=f"Float{bits}")
        rows["float64:nonfinite"] = pd.Series([np.nan, np.inf, -np.inf], dtype="float64")
        rows["double[pyarrow]:null"] = pd.Series(
            [0.0, 1.5, None], dtype=pd.ArrowDtype(pa.float64())
        )
        rows["bool:standard"] = pd.Series([False, True], dtype=bool)
        rows["boolean:null"] = pd.Series([False, True, None], dtype="boolean")
        rows["bool[pyarrow]:null"] = pd.Series([False, True, None], dtype=pd.ArrowDtype(pa.bool_()))

        # Object values and extension strings take different conversion paths.
        rows["object:integers"] = pd.Series([0, 1, None], dtype=object)
        rows["object:booleans"] = pd.Series([False, True, None], dtype=object)
        rows["object:numeric_strings"] = pd.Series(["1", "-1", None], dtype=object)
        rows["object:strings"] = pd.Series(["", "x", None], dtype=object)
        rows["object:decimal"] = pd.Series([Decimal("1.5"), Decimal("-2"), None], dtype=object)
        rows["object:nulls"] = pd.Series([None, np.nan, pd.NA, pd.NaT], dtype=object)
        for storage in ("python", "pyarrow"):
            rows[f"string[{storage}]:null"] = pd.Series(
                ["1", "-1", None], dtype=pd.StringDtype(storage)
            )
        rows["inferred:strings"] = pd.Series(["a", None])

        # Unused categories and ordering remain visible in the input column.
        rows["category:integers"] = pd.Series(
            pd.Categorical([1, 2, None], categories=[2, 1, 3], ordered=True)
        )
        rows["category:strings"] = pd.Series(
            pd.Categorical(["b", "a", None], categories=["b", "a", "unused"])
        )

        # Both units are passed by Spark; include a sub-microsecond value to expose truncation.
        timestamp = pd.Timestamp("2020-01-02 03:04:05.000000123")
        duration = pd.Timedelta(123, unit="ns")
        for unit in ("ns", "us"):
            rows[f"datetime64[{unit}]:null"] = pd.Series(
                [timestamp, None], dtype=f"datetime64[{unit}]"
            )
            rows[f"timedelta64[{unit}]:null"] = pd.Series(
                [duration, None], dtype=f"timedelta64[{unit}]"
            )
        rows["datetime64[ns,UTC]:null"] = pd.Series(
            [timestamp.tz_localize("UTC"), None], dtype=pd.DatetimeTZDtype("ns", "UTC")
        )
        rows["timestamp[us][pyarrow]:null"] = pd.Series(
            [datetime.datetime(2020, 1, 2, 3, 4, 5, 123456), None],
            dtype=pd.ArrowDtype(pa.timestamp("us")),
        )
        rows["duration[us][pyarrow]:null"] = pd.Series(
            [datetime.timedelta(microseconds=123), None], dtype=pd.ArrowDtype(pa.duration("us"))
        )

        for name, dtype in {
            "object": object,
            "int64": np.int64,
            "Int64": pd.Int64Dtype(),
            "int64[pyarrow]": pd.ArrowDtype(pa.int64()),
        }.items():
            rows[f"{name}:empty"] = pd.Series([], dtype=dtype)

        return rows

    @staticmethod
    def _target_dtypes():
        return {
            "bool": bool,
            "boolean": pd.BooleanDtype(),
            "int8": np.int8,
            "Int8": pd.Int8Dtype(),
            "int16": np.int16,
            "Int16": pd.Int16Dtype(),
            "int32": np.int32,
            "Int32": pd.Int32Dtype(),
            "int64": np.int64,
            "Int64": pd.Int64Dtype(),
            "float32": np.float32,
            "Float32": pd.Float32Dtype(),
            "float64": np.float64,
            "Float64": pd.Float64Dtype(),
            "object": object,
            "str": str,
            "string[python]": pd.StringDtype("python"),
            "string[pyarrow]": pd.StringDtype("pyarrow"),
            "category": pd.CategoricalDtype(),
            "datetime64[ns]": np.dtype("datetime64[ns]"),
            "datetime64[us]": np.dtype("datetime64[us]"),
            "timedelta64[ns]": np.dtype("timedelta64[ns]"),
            "timedelta64[us]": np.dtype("timedelta64[us]"),
        }

    def _series_cell(self, series):
        result = self.repr_value(series, max_len=0)
        # Ignore NumPy 2's cosmetic repr change for the "nan" string scalar.
        result = result.replace("np.str_('nan')", "'nan'")
        if isinstance(series.dtype, pd.StringDtype):
            result += f"; storage={series.dtype.storage}"
        elif isinstance(series.dtype, pd.CategoricalDtype):
            categories = series.dtype.categories
            result += (
                f"; categories={categories.tolist()}@{categories.dtype}"
                f"; ordered={series.dtype.ordered}"
            )
            if isinstance(categories.dtype, pd.StringDtype):
                result += f"; categories_storage={categories.dtype.storage}"
        return result

    def _astype_cell(self, series, dtype, **kwargs):
        try:
            # Pin results and exceptions independently of the caller's warning filters.
            with np.errstate(all="ignore"), warnings.catch_warnings():
                warnings.simplefilter("ignore")
                result = series.astype(dtype, **kwargs)
        except Exception as e:
            return f"ERR@{type(e).__name__}"
        return self._series_cell(result)

    @staticmethod
    def _pandas_before_2_2_1_overrides() -> dict[tuple[str, str], str]:
        """
        Account for nullable conversions that changed in pandas 2.2.1.

        Pandas 2.2.0 rejects nullable numeric-to-timedelta and Arrow-backed-to-float
        conversions that later versions accept. It also renders Arrow integers through
        floats when casting to string and exposes the temporal null sentinel as a float.
        """
        overrides = {}

        # These casts raise instead of preserving the missing value as NaT.
        for row_name in (
            "Int8:null",
            "Int16:null",
            "Int32:null",
            "Int64:null",
            "Float32:null",
            "Float64:null",
            "boolean:null",
        ):
            for unit in ("ns", "us"):
                overrides[(row_name, f"timedelta64[{unit}]")] = "ERR@ValueError"

        # These casts raise instead of converting the missing value to NumPy NaN.
        for row_name in (
            "int8[pyarrow]:null",
            "int16[pyarrow]:null",
            "int32[pyarrow]:null",
            "int64[pyarrow]:null",
            "bool[pyarrow]:null",
        ):
            for col_name in ("float32", "float64"):
                overrides[(row_name, col_name)] = "ERR@TypeError"

        # Nullable Arrow integers stringify as "0.0" rather than "0".
        for bits in (8, 16, 32, 64):
            for storage in ("python", "pyarrow"):
                overrides[(f"int{bits}[pyarrow]:null", f"string[{storage}]")] = (
                    f"['0.0', '1.0', <NA>]@Series[string]; storage={storage}"
                )

        # Missing Arrow temporal values become the int64-min sentinel, not NumPy NaN.
        for row_name, col_name, value in (
            ("timestamp[us][pyarrow]:null", "float32", "1577934208892928.0"),
            ("timestamp[us][pyarrow]:null", "float64", "1577934245123456.0"),
            ("duration[us][pyarrow]:null", "float32", "123.0"),
            ("duration[us][pyarrow]:null", "float64", "123.0"),
        ):
            overrides[(row_name, col_name)] = (
                f"[{value}, -9.223372036854776e+18]@Series[{col_name}]"
            )
        return overrides

    @staticmethod
    def _pandas_before_2_3_0_overrides() -> dict[tuple[str, str], str]:
        """
        Account for Arrow temporal string formatting that changed in pandas 2.3.0.

        Pandas 2.2 uses Arrow-style spellings (an ISO ``T`` for timestamps and
        ``123 microseconds`` for durations); later versions use pandas' display format.
        """
        overrides = {}
        for row_name, value in (
            ("timestamp[us][pyarrow]:null", "2020-01-02T03:04:05.123456"),
            ("duration[us][pyarrow]:null", "123 microseconds"),
        ):
            for storage in ("python", "pyarrow"):
                overrides[(row_name, f"string[{storage}]")] = (
                    f"['{value}', <NA>]@Series[string]; storage={storage}"
                )
        return overrides

    @staticmethod
    def _pandas_3_overrides() -> dict[tuple[str, str], str]:
        """
        Account for pandas 3 string inference and conversion semantics.

        Pandas 3 infers its new ``str`` extension dtype instead of ``object`` and makes
        astype(str) return that dtype with NaN missing values. This also changes casts
        from inferred strings and the dtype/storage of string-backed categories.
        """
        overrides = {}

        # All astype(str) results now use Arrow-backed StringDtype rather than object.
        for bits in (8, 16, 32, 64):
            overrides[(f"int{bits}:standard", "str")] = (
                "['0', '1', '-1']@Series[str]; storage=pyarrow"
            )
            for row_name in (f"Int{bits}:null", f"int{bits}[pyarrow]:null"):
                overrides[(row_name, "str")] = "['0', '1', nan]@Series[str]; storage=pyarrow"

        # The loop above handles integer rows with shared expected values. List every
        # remaining source row explicitly because their converted values do not share a pattern.
        for row_name, values in {
            "int64:bounds": "['-9223372036854775808', '9223372036854775807']",
            "float32:fractional": "['-1.5', '-0.0', '1.5']",
            "Float32:null": "['0.0', '1.5', nan]",
            "float64:fractional": "['-1.5', '-0.0', '1.5']",
            "Float64:null": "['0.0', '1.5', nan]",
            "float64:nonfinite": "[nan, 'inf', '-inf']",
            "double[pyarrow]:null": "['0.0', '1.5', nan]",
            "bool:standard": "['False', 'True']",
            "boolean:null": "['False', 'True', nan]",
            "bool[pyarrow]:null": "['False', 'True', nan]",
            "object:integers": "['0', '1', nan]",
            "object:booleans": "['False', 'True', nan]",
            "object:numeric_strings": "['1', '-1', nan]",
            "object:strings": "['', 'x', nan]",
            "object:decimal": "['1.5', '-2', nan]",
            "object:nulls": "[nan, nan, nan, nan]",
            "string[python]:null": "['1', '-1', nan]",
            "string[pyarrow]:null": "['1', '-1', nan]",
            "inferred:strings": "['a', nan]",
            "category:integers": "['1.0', '2.0', nan]",
            "category:strings": "['b', 'a', nan]",
            "datetime64[ns]:null": "['2020-01-02 03:04:05.000000123', nan]",
            "timedelta64[ns]:null": "['0 days 00:00:00.000000123', nan]",
            "datetime64[us]:null": "['2020-01-02 03:04:05', nan]",
            "timedelta64[us]:null": "['0 days', nan]",
            "datetime64[ns,UTC]:null": "['2020-01-02 03:04:05.000000123+00:00', nan]",
            "timestamp[us][pyarrow]:null": "['2020-01-02 03:04:05.123456', nan]",
            "duration[us][pyarrow]:null": "['0 days 00:00:00.000123', nan]",
            "object:empty": "[]",
            "int64:empty": "[]",
            "Int64:empty": "[]",
            "int64[pyarrow]:empty": "[]",
        }.items():
            overrides[(row_name, "str")] = f"{values}@Series[str]; storage=pyarrow"

        # Arrow integer 1 now becomes one microsecond instead of being truncated to zero.
        overrides[("int64[pyarrow]:null", "timedelta64[us]")] = (
            "[Timedelta('0 days 00:00:00'), Timedelta('0 days 00:00:00.000001'), NaT]"
            "@Series[timedelta64[us]]"
        )

        # Inferred strings are now Arrow-backed, changing null and conversion behavior.
        overrides[("inferred:strings", COL_INPUT)] = "['a', nan]@Series[str]; storage=pyarrow"
        overrides[("inferred:strings", "bool")] = "[True, True]@Series[bool]"
        overrides[("inferred:strings", "object")] = "['a', nan]@Series[object]"
        for col_name in ("Int8", "Int16", "Int32", "Int64", "Float32", "Float64"):
            overrides[("inferred:strings", col_name)] = "ERR@ArrowInvalid"

        # String categories now retain the new str dtype and its Arrow storage.
        for row_name, values, categories in (
            ("object:numeric_strings", "['1', '-1', nan]", "['-1', '1']"),
            ("object:strings", "['', 'x', nan]", "['', 'x']"),
            ("inferred:strings", "['a', nan]", "['a']"),
            ("category:strings", "['b', 'a', nan]", "['b', 'a', 'unused']"),
        ):
            overrides[(row_name, "category")] = (
                f"{values}@Series[category]; categories={categories}@str; ordered=False"
                "; categories_storage=pyarrow"
            )
        overrides[("category:strings", COL_INPUT)] = (
            "['b', 'a', nan]@Series[category]; categories=['b', 'a', 'unused']@str"
            "; ordered=False"
            "; categories_storage=pyarrow"
        )
        return overrides

    def _version_overrides(self) -> dict[tuple[str, str], str]:
        """
        Return expected cells that differ from the pandas 2.3.3 / NumPy 2 baseline.

        Conditions are independent because one dependency profile can require several
        groups at once. No PyArrow-specific overrides were needed across versions 18-25.
        """
        overrides = {}
        pandas_version = LooseVersion(pd.__version__)
        if pandas_version < "2.2.1":
            overrides.update(self._pandas_before_2_2_1_overrides())
        if pandas_version < "2.3.0":
            overrides.update(self._pandas_before_2_3_0_overrides())
        if pandas_version >= "3.0.0":
            overrides.update(self._pandas_3_overrides())
        return overrides

    def _test_astype(self, golden_file_prefix, **kwargs):
        rows = self._source_series()
        targets = self._target_dtypes()

        def compute_cell(row_name, col_name):
            series = rows[row_name]
            if col_name == COL_INPUT:
                return self._series_cell(series)
            elif col_name in targets:
                return self._astype_cell(series, targets[col_name], **kwargs)
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=list(rows),
            col_names=[COL_INPUT, *targets],
            compute_cell=compute_cell,
            golden_file_prefix=golden_file_prefix,
            overrides=self._version_overrides(),
        )

    def test_astype_default(self):
        self._test_astype("golden_pandas_series_astype_default")

    def test_astype_copy_false(self):
        self._test_astype("golden_pandas_series_astype_copy_false", copy=False)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
