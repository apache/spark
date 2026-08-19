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
    have_pyarrow,
    have_pandas,
    have_numpy,
    pyarrow_requirement_message,
    pandas_requirement_message,
    numpy_requirement_message,
)

# Imported as a module, not `from ... import _PyArrowFromPandasTestBase`, so that the
# non-default classes pick up only the shared base -- the default test class is not
# re-collected here.
from pyspark.tests.upstream.pyarrow import test_pyarrow_array_from_pandas_default

if have_pandas:
    import pandas as pd
if have_pyarrow:
    import pyarrow as pa


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


@unittest.skipIf(
    not have_pyarrow or not have_pandas or not have_numpy,
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowArrayFromPandasTypeScalarTests(
    test_pyarrow_array_from_pandas_default._PyArrowFromPandasTestBase
):
    """
    Tests pa.Array.from_pandas(series, type=..., safe=...) for SCALAR target types via golden
    file comparison.  Nested targets (list / map / struct) are covered by a separate class.

    Spark uses ``type=`` diagonally (source dtype and requested Arrow type share one
    schema), so this pins a focused set of rows that make type=/safe= observable rather
    than a dense source x target product: the off-diagonal cells (e.g. int -> large_binary)
    are pyarrow-construction trivia Spark never hits, and general conversion is
    pa.Array.cast's job.  safe=True/False are two methods and two goldens, not doubled
    columns.  ``mask`` stays None (fixed by storage backend, covered by the mask tests),
    isolating type=/safe=.
    """

    @staticmethod
    def _get_target_types():
        """Scalar to_arrow_type targets that discriminate type=/safe=, plus duration/time64
        for the timedelta and time-of-day diagonals."""
        return [
            pa.int8(),
            pa.int64(),
            pa.float32(),
            pa.timestamp("us"),
            pa.date32(),
            pa.duration("us"),
            pa.time64("ns"),
            pa.string(),
            pa.binary(),
        ]

    def _type_source_arrays(self):
        """
        Clean family representatives, the protocol rows that expose the safe= drop and
        SPARK-46776, and the shared coercion rows (reused so a changed cell is attributable
        across the default/mask/type goldens).
        """
        pool = {**self._numpy_backed_sources(), **self._protocol_sources()}
        selected = [
            # Clean family reps.
            "int64:standard",
            "float64:standard",
            "bool:standard",
            "object:string",
            "object:bytes",
            "date:standard",
            "object:datetime",
            "object:timedelta",
            "time:standard",
            # Protocol rows: Int64 = safe-drop contrast; last two = SPARK-46776's pyarrow < 19
            # followup (a narrower type is ignored on the __arrow_array__ path -> stored type).
            "Int64:standard",
            "string[pyarrow]:standard",
            "large_binary[pyarrow]:standard",
        ]
        sources = {name: pool[name] for name in selected}
        # Coercion group reused in full; the numpy/protocol rows above are cherry-picked.
        sources.update(self._coercion_sources())
        return sources

    def _compare_type_matrix(self, safe, golden_file_prefix, overrides):
        sources = self._type_source_arrays()
        target_types = self._get_target_types()
        target_names = [self.repr_type(t) for t in target_types]
        target_lookup = dict(zip(target_names, target_types))

        self.compare_or_generate_golden_matrix(
            row_names=list(sources.keys()),
            col_names=target_names,
            compute_cell=lambda src, tgt: self._from_pandas_cell(
                sources[src], type=target_lookup[tgt], safe=safe
            ),
            golden_file_prefix=golden_file_prefix,
            overrides=overrides,
        )

    def test_from_pandas_type_scalar_safe(self):
        """Test pa.Array.from_pandas(type=<scalar>, safe=True) against golden file."""
        # pyarrow < 19 ignores the requested type on the protocol path and returns the stored
        # type (the SPARK-46776 followup), for every target.  Other sources are version-stable.
        overrides: dict[tuple[str, str], str] = {}
        if LooseVersion(pa.__version__) < LooseVersion("19.0.0"):
            for col in [self.repr_type(t) for t in self._get_target_types()]:
                overrides[("string[pyarrow]:standard", col)] = "[hello, world]@large_string"
                overrides[("large_binary[pyarrow]:standard", col)] = (
                    "[b'hello', b'world']@large_binary"
                )
        self._compare_type_matrix(
            safe=True,
            golden_file_prefix="golden_pyarrow_array_from_pandas_type_scalar_safe",
            overrides=overrides,
        )

    def test_from_pandas_type_scalar_unsafe(self):
        """Test pa.Array.from_pandas(type=<scalar>, safe=False) against golden file."""
        # Same overrides as the safe method: the protocol drops the type request before any
        # cast, so safe=False changes none of these cells (SPARK-46776, pyarrow < 19 followup).
        overrides: dict[tuple[str, str], str] = {}
        if LooseVersion(pa.__version__) < LooseVersion("19.0.0"):
            for col in [self.repr_type(t) for t in self._get_target_types()]:
                overrides[("string[pyarrow]:standard", col)] = "[hello, world]@large_string"
                overrides[("large_binary[pyarrow]:standard", col)] = (
                    "[b'hello', b'world']@large_binary"
                )
        self._compare_type_matrix(
            safe=False,
            golden_file_prefix="golden_pyarrow_array_from_pandas_type_scalar_unsafe",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
