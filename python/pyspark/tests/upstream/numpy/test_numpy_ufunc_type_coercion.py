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
Tests for NumPy's ufunc type coercion using golden file comparison.

pandas-on-Spark dispatches NumPy ufuncs to Spark expressions
(``python/pyspark/pandas/numpy_compat.py``) and gates the operand types each ufunc accepts
with ``_np_spark_accepted_types``, a table transcribed by hand from what NumPy accepts.
Nothing verifies that transcription, and ``dev/requirements.txt`` requires
``numpy>=1.23.2`` with no upper bound.  This test pins NumPy's side, so a release that
moves a coercion fails here instead of leaving the gate to over-reject (``TypeError`` on
working code) or under-reject (the silent cast the gate exists to prevent).  pandas' own
dispatch is covered separately, by ``python/pyspark/pandas/tests/test_numpy_compat.py``.

## Matrix

Rows are the ufuncs pandas-on-Spark dispatches, read from the mappings so a new entry in
``numpy_compat.py`` is covered here too.  A binary ufunc gets one row per operand position
(``ldexp[0]``, ``ldexp[1]``) with the other operand held at ``int64``; a two-output ufunc
(``frexp``, ``modf``) keeps one row and puts both dtypes in the cell.

Columns are the operand under test, sampled as 2-element arrays.  Nine name a dtype; the
four ``object:`` columns name an element type instead, because ``object`` is one dtype
holding many and NumPy dispatches on the element.  Scalars are out of scope, which keeps
NEP 50 value-based casting out of the matrix.

## Golden File Cell Format

- Coerced: ``float16`` - the dtype NumPy converted the operand to, e.g. ``np.sqrt`` gives
  ``float16`` on ``int8`` but ``float32`` on ``int16``
- Two outputs: ``(float16, int32)``
- Rejected: ``ERR@TypeError`` - the exception class name

Acceptance is implied rather than stored: a dtype means NumPy accepted the operand,
``ERR@*`` that it refused.

## NumPy Version Compatibility

The golden is generated on the newest NumPy, which is what an unpinned
``dev/requirements.txt`` install resolves to, and the inline ``overrides`` carries the cells that
differ on older ones.  Swept 2.0.0 through 2.4.6: only 2.0.x differs, in 19 cells, because
2.1 gave ``ceil``/``floor``/``trunc`` integer loops where 2.0 promoted to float.

These tests skip below 2.0 as ``test_pyarrow_array_cast.py`` does: 2.0 reworked promotion
(NEP 50), and 33 cells differ on the ``numpy==1.23.2`` minimum-dependency pin.

## Regenerating Golden Files

Set SPARK_GENERATE_GOLDEN_FILES=1 before running:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/numpy/test_numpy_ufunc_type_coercion.py

If package tabulate (https://pypi.org/project/tabulate/) is installed, it will also
regenerate the Markdown files.
"""

import unittest
import warnings
from datetime import date
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


@unittest.skipIf(
    not have_numpy
    or not have_pandas
    or not have_pyarrow
    or LooseVersion(np.__version__) < LooseVersion("2.0.0"),
    numpy_requirement_message
    or pandas_requirement_message
    or pyarrow_requirement_message
    or "NumPy 2.0.0 or greater required",
)
class NumPyUFuncTypeCoercionTests(GoldenFileTestMixin, unittest.TestCase):
    """Tests the dtype NumPy coerces each dispatched ufunc's operand to."""

    # ----- rows and columns -----

    @staticmethod
    def _get_rows():
        """Return {row name: (ufunc, position of the operand under test)}."""
        from pyspark.pandas.numpy_compat import (
            binary_np_spark_mappings,
            multi_output_np_spark_mappings,
            unary_np_spark_mappings,
        )

        keys = (
            set(unary_np_spark_mappings)
            | set(binary_np_spark_mappings)
            | set(multi_output_np_spark_mappings)
        )

        rows = {}
        for key in sorted(keys):
            ufunc = getattr(np, key, None)
            if not isinstance(ufunc, np.ufunc) or ufunc.__name__ != key:
                continue  # an alias, e.g. abs -> absolute
            if ufunc.nin == 1:
                rows[key] = (ufunc, 0)
            else:
                for position in range(ufunc.nin):
                    rows[f"{key}[{position}]"] = (ufunc, position)
        return rows

    @staticmethod
    def _get_samples():
        """Return {column name: the 2-element array that column's operand is tested with}."""
        samples = {
            dtype: np.ones(2, dtype=dtype)
            for dtype in [
                "bool",
                "int8",
                "int16",
                "int32",
                "int64",
                "uint64",
                "float16",
                "float32",
                "float64",
            ]
        }
        samples["object:Decimal"] = np.array([Decimal("1"), Decimal("2")], dtype=object)
        samples["object:str"] = np.array(["1", "2"], dtype=object)
        samples["datetime64[ns]"] = np.array(["2020-01-01", "2020-01-02"], dtype="datetime64[ns]")
        samples["object:date"] = np.array([date(2020, 1, 1), date(2020, 1, 2)], dtype=object)
        samples["object:None"] = np.array([None, None], dtype=object)
        return samples

    # ----- cell -----

    def _coerce(self, row, sample, partner):
        """
        Run one ufunc, with `sample` as the operand under test and `partner` in any other slot.

        A unary ufunc is called with `sample` alone; a binary one gets `sample` at the
        position this row tests and `partner` in the remaining position.

        Returns the coerced output dtype, "(dtype, dtype)" for a two-output ufunc, or
        "ERR@<exception class name>".
        """
        ufunc, position = row
        args = [sample if slot == position else partner for slot in range(ufunc.nin)]

        # arctanh(1) divides by zero, so suppress warnings: a runner started with -W error
        # would otherwise record one as an ERR cell.
        try:
            with np.errstate(all="ignore"), warnings.catch_warnings():
                warnings.simplefilter("ignore")
                result = ufunc(*args)
        except Exception as e:
            return f"ERR@{type(e).__name__}"
        if isinstance(result, tuple):  # frexp, modf
            return "(%s)" % ", ".join(str(np.asarray(out).dtype) for out in result)
        return str(np.asarray(result).dtype)

    # ----- test methods -----

    def test_ufunc_coercion_matrix(self):
        """Test the coerced operand dtype for every dispatched ufunc."""
        rows = self._get_rows()
        samples = self._get_samples()

        overrides: dict[tuple[str, str], str] = {}
        # NumPy 2.1 gave ceil/floor/trunc integer loops; 2.0 promoted the operand to float,
        # and answered gcd on an all-null object array instead of rejecting it.
        if LooseVersion(np.__version__) < LooseVersion("2.1.0"):
            promoted = {
                "bool": "float16",
                "int8": "float16",
                "int16": "float32",
                "int32": "float64",
                "int64": "float64",
                "uint64": "float64",
            }
            for ufunc in ("ceil", "floor", "trunc"):
                for dtype, result in promoted.items():
                    overrides[(ufunc, dtype)] = result
            overrides[("gcd[1]", "object:None")] = "object"

        self.compare_or_generate_golden_matrix(
            row_names=list(rows),
            col_names=list(samples),
            # Every operand not under test is held at int64, which every binary ufunc here
            # accepts, so an ERR cell is always about the column.
            compute_cell=lambda row_name, col_name: self._coerce(
                rows[row_name], samples[col_name], samples["int64"]
            ),
            golden_file_prefix="golden_numpy_ufunc_type_coercion",
            index_name="ufunc \\ operand dtype",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
