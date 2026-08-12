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
Tests for PyArrow Array.from_pandas() with non-default arguments, using golden file
comparison.

PySpark passes ``pa.Array.from_pandas(series, mask=mask, type=arrow_type, safe=safecheck)``
to convert a pandas Series into an Arrow array.  The bare-argument behavior is covered by
``test_pyarrow_array_from_pandas_default.py``, whose source Series inventory
(``_PyArrowFromPandasTestBase``) is reused here; this file records the non-default
arguments so CI fails loudly if they drift across pandas/PyArrow/NumPy upgrades.

## The ``mask`` argument

PySpark derives ``mask`` from how the Series is stored
(``conversion.py:435``, ``pandas/conversion.py:113``)::

    mask = None if hasattr(series.array, "__arrow_array__") else series.isnull()

- **numpy-backed** dtypes take ``mask=series.isnull()``, which agrees with the nulls
  ``from_pandas`` infers at ``mask=None``; this test pins that agreement.
- **protocol** dtypes (implementing ``__arrow_array__``) return a finished Arrow array with
  its own validity bitmap, so PyArrow rejects any mask -- even an all-False no-op -- with
  ``ValueError``; PySpark passes ``mask=None``.

So ``mask`` is fixed by the row's dtype, hence a column pair (``mask=None`` vs
``mask=isnull()``) rather than a matrix dimension.

## Golden File Cell Format

Each cell uses the value@type format:
- pandas Series: "python_list_repr@Series[dtype]"
- PyArrow Array: "python_list_repr@arrow_type"
- PyArrow ChunkedArray: "python_list_repr@chunked<arrow_type>"
- Error: "ERR@ExceptionClassName"

## Regenerating Golden Files

Set SPARK_GENERATE_GOLDEN_FILES=1 before running:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/pyarrow/test_pyarrow_array_from_pandas_non_default.py

## PyArrow and pandas Version Compatibility

The golden files capture behavior for specific PyArrow and pandas versions.
Regenerate when upgrading either dependency, as from_pandas() behavior may change.
The committed golden files were generated with pandas 2.3.3, pyarrow 24.0.0, and
numpy 2.4.1.
"""

import unittest

from pyspark.loose_version import LooseVersion
from pyspark.testing.utils import (
    have_numpy,
    have_pandas,
    have_pyarrow,
    numpy_requirement_message,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

# Imported as a module, not `from ... import _PyArrowFromPandasTestBase`, so that the
# non-default classes pick up only the shared base -- the default test class is not
# re-collected here.
from pyspark.tests.upstream.pyarrow import test_pyarrow_array_from_pandas_default

if have_pandas:
    import pandas as pd


@unittest.skipIf(
    not have_pyarrow or not have_pandas or not have_numpy,
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowArrayFromPandasMaskTests(
    test_pyarrow_array_from_pandas_default._PyArrowFromPandasTestBase
):
    """
    Tests pa.Array.from_pandas(series, mask=...) via golden file comparison.

    Reuses the full source inventory.  numpy-backed rows accept a mask and record equal
    results for mask=None and mask=isnull(); protocol rows reject a mask and record
    ERR@ValueError for mask=isnull().
    """

    COL_MASK_NONE = "mask=None"
    COL_MASK_ISNULL = "mask=isnull()"

    def test_from_pandas_mask(self):
        """Test pa.Array.from_pandas() with the mask argument against golden file."""
        sources = self._build_source_arrays()
        row_names = list(sources.keys())
        col_names = ["pandas series", self.COL_MASK_NONE, self.COL_MASK_ISNULL]

        # Version-specific expected values go here, keyed by (row, col), when a newer
        # pandas/PyArrow/NumPy legitimately changes a cell's output.
        overrides: dict[tuple[str, str], str] = {}
        if LooseVersion(pd.__version__) >= LooseVersion("3.0.0"):
            # The deliberately unpinned rows infer microseconds on pandas 3 where pandas 2
            # infers nanoseconds, and a categorical's values inherit the new str dtype's
            # large_string backing.  These stay numpy-backed, so both mask columns move
            # together and accept the mask.
            overrides[("datetime64:inferred", "pandas series")] = (
                "[Timestamp('2024-06-15 18:30:00')]@Series[datetime64[us]]"
            )
            overrides[("timedelta64:inferred", "pandas series")] = (
                "[Timedelta('1 days 00:00:00'), "
                "Timedelta('0 days 02:00:00')]@Series[timedelta64[us]]"
            )
            for col in (self.COL_MASK_NONE, self.COL_MASK_ISNULL):
                overrides[("datetime64:inferred", col)] = "[2024-06-15 18:30:00]@timestamp[us]"
                overrides[("timedelta64:inferred", col)] = "[1 day, 0:00:00, 2:00:00]@duration[us]"
                overrides[("category:standard", col)] = (
                    "[a, b, a]@dictionary<values=large_string, indices=int8, ordered=0>"
                )
                overrides[("category:nullable", col)] = (
                    "[a, None, b]@dictionary<values=large_string, indices=int8, ordered=0>"
                )
            # string:inferred is object (numpy-backed) on pandas 2 but the dedicated str
            # dtype (large_string, __arrow_array__) on pandas 3, so it moves from a
            # mask-accepting row to a protocol row: mask=isnull() flips to ERR@ValueError.
            overrides[("string:inferred", "pandas series")] = "['hello', 'world']@Series[str]"
            overrides[("string:inferred", self.COL_MASK_NONE)] = "[hello, world]@large_string"
            overrides[("string:inferred", self.COL_MASK_ISNULL)] = "ERR@ValueError"

        def compute_cell(row_name, col_name):
            series = sources[row_name]
            if col_name == "pandas series":
                return self.repr_value(series, max_len=0)
            elif col_name == self.COL_MASK_NONE:
                return self._from_pandas_cell(series, mask=None)
            elif col_name == self.COL_MASK_ISNULL:
                return self._from_pandas_cell(series, mask=series.isnull())
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_array_from_pandas_mask",
            index_name="test case",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
