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
Tests for pandas' type-introspection predicates (pandas.api.types.is_*) using golden files.

PySpark BRANCHES on the boolean these return, so a drift misroutes control flow rather than
corrupting one value. Three ways the answer is load-bearing:

- picks a Spark type: `_create_from_pandas_with_arrow` in python/pyspark/sql/pandas/conversion.py
  and `createDataFrame` in python/pyspark/sql/connect/session.py map a column to TimestampType /
  DayTimeIntervalType only when is_datetime64_dtype / is_timedelta64_dtype say so.
- rewrites data: `convert_int_to_decimal` in python/pyspark/sql/pandas/types.py converts ints to
  Decimal only when is_integer_dtype holds.
- selects an implementation or raises: `astype` in python/pyspark/pandas/data_type_ops/num_ops.py
  dispatches on is_integer_dtype, and `Index.__new__` in python/pyspark/pandas/indexes/base.py
  turns is_hashable into a user-visible TypeError.

Two classes, one per input axis, because their rows are disjoint: the dtype predicates
(pandas.core.dtypes.common) take a dtype or an array, the object predicates
(pandas.core.dtypes.inference, pandas._libs.lib) take an arbitrary object.

is_integer and is_sequence come from pandas.core.dtypes.inference because that is where PySpark
imports them from, and is_sequence has no pandas.api.types equivalent at all. Those private names
are the same function objects as the public ones, so there is no second behavior to measure --
what the private import buys is an ImportError here if pandas ever moves or drops them.

Cell format: "True" / "False", or "ERR@ExceptionClassName" if the call raises. The leading column
records the input -- "<shape>(<dtype>)" for the dtype tests, type(obj).__name__ for the object
tests -- read off the value, so a row cannot keep a label that no longer matches it.

Regenerating golden files:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/pandas/test_pandas_api_types.py

If package tabulate (https://pypi.org/project/tabulate/) is installed, it also regenerates the
Markdown files. Version differences belong in a test's ``overrides`` dict, not in a regenerated
golden; the three "inferred(...)" rows leave their dtype UNPINNED so pandas' inference changes
land visibly in the input column.
"""

import datetime
import sys
import unittest

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

if have_pandas:
    import pandas as pd
    from pandas.api.types import (
        is_bool_dtype,
        is_datetime64_dtype,
        is_dict_like,
        is_float_dtype,
        is_hashable,
        is_integer_dtype,
        is_list_like,
        is_number,
        is_numeric_dtype,
        is_object_dtype,
        is_scalar,
        is_timedelta64_dtype,
    )

    # Imported from the private module PySpark itself imports them from; is_sequence is not
    # exported by pandas.api.types at all.
    from pandas.core.dtypes.inference import is_integer, is_sequence
if have_pyarrow:
    import pyarrow as pa
if have_numpy:
    import numpy as np

COL_INPUT = "input"
COL_INPUT_TYPE = "input type"


class _PandasApiTypesTestBase(GoldenFileTestMixin, unittest.TestCase):
    """Shared cell helper for the pandas.api.types tests. Defines no test_* of its own."""

    @staticmethod
    def _predicate_cell(predicate, value) -> str:
        """
        Format ``predicate(value)``, or ``ERR@<ExceptionClass>`` if it raises. Only the
        predicate is guarded: a formatting error is a test bug, not a predicate signal.
        """
        try:
            result = predicate(value)
        except Exception as e:
            return f"ERR@{type(e).__name__}"
        return str(result)


class _DtypeCarrier:
    """
    An object that merely exposes .dtype, standing in for the pandas-on-Spark Series that
    `_bool_column_labels` (python/pyspark/pandas/frame.py) passes; a real one would need the
    Spark session these tests never start.
    """

    def __init__(self, dtype):
        self.dtype = dtype


@unittest.skipIf(
    not have_pyarrow or not have_pandas or not have_numpy,
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PandasApiTypesDtypeTests(_PandasApiTypesTestBase):
    """
    Tests the predicates that take a dtype or an array (arr_or_dtype). Rows are the dtypes
    PySpark meets -- numpy-native, nullable extension, arrow-backed -- as bare dtypes, plus a
    subset re-asked through each container shape PySpark passes.
    """

    def _predicates(self):
        """Ordered {column name: predicate}. Single source of truth for columns and dispatch."""
        return {
            "is_bool_dtype": is_bool_dtype,
            "is_integer_dtype": is_integer_dtype,
            "is_float_dtype": is_float_dtype,
            "is_numeric_dtype": is_numeric_dtype,
            "is_object_dtype": is_object_dtype,
            "is_datetime64_dtype": is_datetime64_dtype,
            "is_timedelta64_dtype": is_timedelta64_dtype,
        }

    def _dtype_rows(self):
        """
        Ordered {row name: value}; the row name spells the shape the dtype arrives in.
        """
        rows = {}

        # =====================================================================
        # Bare dtypes, ordered by logical type. Row names are pandas' own, which encode the
        # family: lower case is numpy-backed (int64), capitalised a nullable extension (Int64),
        # a [pyarrow] suffix an Arrow-backed one -- three distinct classes, all covered.
        # =====================================================================
        rows["bool:dtype"] = np.dtype("bool")
        rows["boolean:dtype"] = pd.BooleanDtype()
        rows["int64:dtype"] = np.dtype("int64")
        rows["Int64:dtype"] = pd.Int64Dtype()
        rows["int64[pyarrow]:dtype"] = pd.ArrowDtype(pa.int64())
        rows["float64:dtype"] = np.dtype("float64")
        rows["Float64:dtype"] = pd.Float64Dtype()
        rows["object:dtype"] = np.dtype("O")
        rows["string[python]:dtype"] = pd.StringDtype("python")
        rows["string[pyarrow]:dtype"] = pd.StringDtype("pyarrow")
        rows["category:dtype"] = pd.CategoricalDtype(["a", "b"])
        rows["datetime64[ns]:dtype"] = np.dtype("<M8[ns]")
        rows["datetime64[us]:dtype"] = np.dtype("<M8[us]")
        rows["datetime64[ns,UTC]:dtype"] = pd.DatetimeTZDtype("ns", "UTC")
        rows["timestamp[us][pyarrow]:dtype"] = pd.ArrowDtype(pa.timestamp("us"))
        rows["timedelta64[ns]:dtype"] = np.dtype("<m8[ns]")
        rows["duration[us][pyarrow]:dtype"] = pd.ArrowDtype(pa.duration("us"))

        # =====================================================================
        # Inferred dtypes, deliberately UNPINNED: the input column records what pandas chose.
        # =====================================================================
        rows["inferred(str):dtype"] = pd.Series(["a"]).dtype
        rows["inferred(datetime):dtype"] = pd.Series([datetime.datetime(2020, 1, 1)]).dtype
        rows["inferred(timedelta):dtype"] = pd.Series([datetime.timedelta(days=1)]).dtype

        # =====================================================================
        # Container shapes. A numpy array cannot hold an extension dtype, so the arrow-backed
        # and tz-aware rows have no ndarray form.
        # =====================================================================
        rows["int64:series"] = pd.Series([], dtype="int64")
        rows["int64:ndarray"] = np.array([], dtype="int64")
        rows["int64:carrier"] = _DtypeCarrier(np.dtype("int64"))
        rows["object:series"] = pd.Series([], dtype="O")
        rows["object:ndarray"] = np.array([], dtype="O")
        rows["object:carrier"] = _DtypeCarrier(np.dtype("O"))
        rows["int64[pyarrow]:series"] = pd.Series([], dtype=pd.ArrowDtype(pa.int64()))
        rows["int64[pyarrow]:carrier"] = _DtypeCarrier(pd.ArrowDtype(pa.int64()))
        rows["datetime64[ns,UTC]:series"] = pd.Series([], dtype=pd.DatetimeTZDtype("ns", "UTC"))
        rows["datetime64[ns,UTC]:carrier"] = _DtypeCarrier(pd.DatetimeTZDtype("ns", "UTC"))

        # No .dtype at all: answers a silent False, not an error, so a wrong object would
        # take the wrong branch quietly.
        rows["no-dtype:object"] = object()

        return rows

    @staticmethod
    def _input_cell(value) -> str:
        """
        Render as ``<shape>(<dtype>)``, reading the dtype off the value so an inferred row
        cannot keep a stale label.
        """
        if isinstance(value, pd.Series):
            return f"series({value.dtype})"
        elif isinstance(value, np.ndarray):
            return f"ndarray({value.dtype})"
        elif isinstance(value, _DtypeCarrier):
            return f"carrier({value.dtype})"
        elif isinstance(value, pd.StringDtype):
            # Every other dtype gives its own name here, but both string storages give plain
            # "string", so these two rows would be identical text. The storage separates them.
            return f"dtype({value}[{value.storage}])"
        elif isinstance(value, (np.dtype, pd.api.extensions.ExtensionDtype)):
            return f"dtype({value})"
        else:
            return f"other({type(value).__name__})"

    def test_dtype_predicates(self):
        """Test the arr_or_dtype predicates over the dtypes and shapes PySpark passes."""
        rows = self._dtype_rows()
        predicates = self._predicates()
        # pandas/numpy-version-specific expected cells; empty at the pandas 2.3 baseline.
        overrides: dict[tuple[str, str], str] = {}
        if LooseVersion(pd.__version__) >= LooseVersion("3.0.0"):
            # pandas 3 infers the new `str` dtype instead of object -- so is_object_dtype
            # FLIPS -- and us instead of ns. `str` is not an explicit StringDtype: same pyarrow
            # storage (always pyarrow, since the class skips without it), na_value nan not pd.NA.
            overrides[("inferred(str):dtype", COL_INPUT)] = "dtype(str[pyarrow])"
            overrides[("inferred(str):dtype", "is_object_dtype")] = "False"
            overrides[("inferred(datetime):dtype", COL_INPUT)] = "dtype(datetime64[us])"
            overrides[("inferred(timedelta):dtype", COL_INPUT)] = "dtype(timedelta64[us])"

        def compute_cell(row_name, col_name):
            value = rows[row_name]
            if col_name == COL_INPUT:
                return self._input_cell(value)
            elif col_name in predicates:
                return self._predicate_cell(predicates[col_name], value)
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=list(rows.keys()),
            col_names=[COL_INPUT, *predicates.keys()],
            compute_cell=compute_cell,
            golden_file_prefix="golden_pandas_api_types_dtype",
            index_name="dtype \\ predicate",
            overrides=overrides,
        )


@unittest.skipIf(
    not have_pandas or not have_numpy,
    pandas_requirement_message or numpy_requirement_message,
)
class PandasApiTypesObjectTests(_PandasApiTypesTestBase):
    """
    Tests the predicates that take an arbitrary object. PySpark asks these of user-supplied
    arguments to decide whether one value or many were passed, and whether a label is usable,
    so the rows are the argument shapes that reach those checks.
    """

    def _predicates(self):
        """Ordered {column name: predicate}. Single source of truth for columns and dispatch."""
        return {
            "is_list_like": is_list_like,
            "is_hashable": is_hashable,
            "is_dict_like": is_dict_like,
            "is_scalar": is_scalar,
            "is_number": is_number,
            "is_integer": is_integer,
            "is_sequence": is_sequence,
        }

    def _object_rows(self):
        """
        Ordered {row name: value} of the object shapes PySpark's callers actually supply.
        """
        rows = {}

        # Scalars, including numpy's own, which PySpark sees from a pandas column
        rows["int 5"] = 5
        rows["np.int64(5)"] = np.int64(5)
        rows["float 5.0"] = 5.0
        rows["bool True"] = True

        # The null family: four sentinels PySpark meets, which do not all answer alike
        rows["None"] = None
        rows["np.nan"] = np.nan
        rows["pd.NA"] = pd.NA
        rows["pd.NaT"] = pd.NaT

        # Text and bytes: iterable, but must not count as "many values"
        rows["str 'abc'"] = "abc"
        rows["bytes b'x'"] = b"x"

        # Containers. The label is the pandas-on-Spark Label type, asked about by BOTH
        # families, and it answers exactly as a plain tuple does -- which is the point.
        rows["list [1,2]"] = [1, 2]
        rows["tuple (1,)"] = (1,)
        rows["label ('a','b')"] = ("a", "b")
        # An unhashable element makes the tuple list_like yet NOT hashable -- the shape the
        # Index/Series name guard rejects, and pandas' own documented is_hashable example.
        rows["tuple ([1],)"] = ([1],)
        rows["dict {'a':1}"] = {"a": 1}
        rows["set {1,2}"] = {1, 2}
        # The one exhaustible row, safe to share across cells: every answer comes from the
        # protocol (__iter__, no __len__, hashes by identity), never from the contents.
        rows["generator"] = (x for x in [1])
        rows["range(3)"] = range(3)
        rows["slice(1,2)"] = slice(1, 2)

        # numpy and pandas objects an argument may turn out to be
        rows["np.ndarray([1,2])"] = np.array([1, 2])
        rows["pd.Series([1])"] = pd.Series([1])
        rows["pd.DataFrame"] = pd.DataFrame({"a": [1]})
        rows["pd.Index([1])"] = pd.Index([1])
        rows["pd.Categorical"] = pd.Categorical(["a"])

        # A dtype and a function: the only rows shared with the dtype family's inputs
        rows["np.dtype('int64')"] = np.dtype("int64")
        rows["callable len"] = len

        return rows

    def test_object_predicates(self):
        """Test the arbitrary-object predicates over the argument shapes PySpark receives."""
        rows = self._object_rows()
        predicates = self._predicates()
        # pandas/numpy-version-specific expected cells; empty at the pandas 2.3 baseline.
        overrides: dict[tuple[str, str], str] = {}
        if sys.version_info >= (3, 12):
            # slice became hashable in Python 3.12, and is_hashable is exactly "does hash()
            # raise", so this cell follows the interpreter. The golden holds the 3.11 answer.
            overrides[("slice(1,2)", "is_hashable")] = "True"

        def compute_cell(row_name, col_name):
            value = rows[row_name]
            if col_name == COL_INPUT_TYPE:
                return type(value).__name__
            elif col_name in predicates:
                return self._predicate_cell(predicates[col_name], value)
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=list(rows.keys()),
            col_names=[COL_INPUT_TYPE, *predicates.keys()],
            compute_cell=compute_cell,
            golden_file_prefix="golden_pandas_api_types_object",
            index_name="object \\ predicate",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
