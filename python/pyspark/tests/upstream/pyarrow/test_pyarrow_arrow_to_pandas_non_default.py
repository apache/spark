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
Tests for PyArrow Array.to_pandas() with non-default arguments, using golden file
comparison.

PySpark relies on these arguments in production: when building pandas objects from
Arrow data (see ``python/pyspark/sql/pandas/conversion.py``), it calls
``to_pandas(coerce_temporal_nanoseconds=True)`` (and, when applicable,
``date_as_object=True``) so that the Arrow path produces the same
nanosecond-resolution ``datetime64[ns]`` / ``timedelta64[ns]`` values as the
non-Arrow path.  These tests record how each Arrow type behaves under those
arguments so CI fails loudly if the behavior drifts across pandas/PyArrow/NumPy
upgrades.

The shared ``_PyArrowToPandasTestBase`` holds the golden-file matrix driver and
the per-cell conversion helper.  Each concrete test class supplies its own source
arrays (the rows relevant to the argument under test) and one test method per
argument combination (each producing its own golden file).  New ``to_pandas``
arguments (e.g. ``zero_copy_only``) can be added as additional classes without
touching the existing ones.

## Golden File Cell Format

Each cell uses the value@type format:
- numpy ndarray: "python_list_repr@ndarray[dtype]"
- pandas Series: "python_list_repr@Series[dtype]"
- Error: "ERR@ExceptionClassName"

Values are formatted via tolist() for stable, Python-native representation.

## Regenerating Golden Files

Set SPARK_GENERATE_GOLDEN_FILES=1 before running:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/pyarrow/test_pyarrow_arrow_to_pandas_non_default.py

## PyArrow and pandas Version Compatibility

The golden files capture behavior for specific PyArrow and pandas versions.
Regenerate when upgrading either dependency, as to_pandas() behavior may change.
The committed golden files were generated with pandas 2.3.3, pyarrow 24.0.0, and
numpy 2.4.1.
"""

import datetime
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
from pyspark.testing.goldenutils import GoldenFileTestMixin

if have_pandas:
    import pandas as pd
if have_pyarrow:
    import pyarrow as pa
if have_numpy:
    import numpy as np


class _PyArrowToPandasTestBase(GoldenFileTestMixin, unittest.TestCase):
    """
    Shared machinery for pa.Array.to_pandas() golden file tests.

    Concrete subclasses provide their own ``_build_source_arrays`` (the rows) and
    one or more ``test_*`` methods that call ``compare_or_generate_golden_matrix``.
    This base defines no ``test_*`` methods, so it contributes no tests itself.
    """

    def _to_pandas_cell(self, arr, **to_pandas_kwargs) -> str:
        """
        Convert ``arr`` via ``to_pandas(**to_pandas_kwargs)`` and format the
        result as a golden-file cell, returning ``ERR@<ExceptionClass>`` if the
        conversion raises.
        """
        try:
            return self.repr_value(arr.to_pandas(**to_pandas_kwargs), max_len=0)
        except Exception as e:
            return f"ERR@{type(e).__name__}"

    @staticmethod
    def _numpy_shares_arrow_buffer(arr) -> str:
        """
        Independently verify whether ``arr.to_pandas()`` returns a numpy-backed
        pandas Series that physically SHARES memory with a source Arrow buffer
        (i.e. the conversion was genuinely zero-copy), rather than trusting
        PyArrow's own ``zero_copy_only`` verdict.

        This measures rather than assumes: it asks numpy directly, via
        ``np.shares_memory``, whether the Series' numpy data overlaps ANY buffer
        of the source array (across every chunk of a ``ChunkedArray``).  Every
        buffer is exposed as raw bytes with ``np.frombuffer`` and compared;
        ``np.shares_memory`` accounts for slice offsets and partial overlap, so
        this is robust where raw address equality is not, and it returns cleanly
        (no share) for object-dtype output, materialized nested types, and
        multi-chunk concatenation without needing to special-case them.

        Returns one of:
        - ``"shared"``     -- numpy data overlaps a source Arrow buffer (zero-copy)
        - ``"not-shared"`` -- numpy data is a distinct allocation (a copy)
        - ``"n/a (empty)"``-- the array has no elements, so there are no data
          bytes for which sharing is meaningful (``np.shares_memory`` on two
          empty buffers is vacuously false; label it rather than call it a copy)
        - ``"ERR@<Cls>"``  -- ``to_pandas`` itself raised
        """
        try:
            series = arr.to_pandas()
        except Exception as e:
            return f"ERR@{type(e).__name__}"

        if len(arr) == 0:
            return "n/a (empty)"

        npv = series.to_numpy()

        chunks = (
            [arr.chunk(i) for i in range(arr.num_chunks)]
            if isinstance(arr, pa.ChunkedArray)
            else [arr]
        )
        for chunk in chunks:
            for buffer in chunk.buffers():
                if buffer is None:
                    continue
                raw = np.frombuffer(buffer, dtype=np.uint8)
                if np.shares_memory(raw, npv):
                    return "shared"
        return "not-shared"


@unittest.skipIf(
    not have_pyarrow or not have_pandas or not have_numpy,
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowArrayToPandasCoerceTemporalTests(_PyArrowToPandasTestBase):
    """
    Tests pa.Array.to_pandas(coerce_temporal_nanoseconds=True) via golden file comparison.

    Covers the temporal Arrow types the argument affects (timestamp and duration in
    units s/ms/us/ns, tz-aware timestamp, date, and time), plus overflow cases and
    a few non-temporal control rows.  Each type is tested without nulls, with a null,
    and empty.

    The date rows are recorded under two output columns: the default
    ``date_as_object=True`` (object-dtype ``datetime.date``, which coercion does not
    affect) and ``date_as_object=False`` (the numeric ``datetime64[ns]`` path where
    coercion takes effect).
    """

    def _build_source_arrays(self):
        """Build an ordered dict of named source PyArrow arrays for testing."""
        sources = {}

        # =====================================================================
        # Timestamp types (the primary target of coerce_temporal_nanoseconds)
        # =====================================================================
        dt1 = datetime.datetime(2024, 1, 1, 12, 0, 0)
        dt2 = datetime.datetime(2024, 6, 15, 18, 30, 0)
        for unit in ["s", "ms", "us", "ns"]:
            sources[f"timestamp[{unit}]:standard"] = pa.array([dt1, dt2], pa.timestamp(unit))
            sources[f"timestamp[{unit}]:nullable"] = pa.array([dt1, None], pa.timestamp(unit))
            sources[f"timestamp[{unit}]:empty"] = pa.array([], pa.timestamp(unit))
        # Timestamp with timezone
        sources["timestamp[us,tz=UTC]:standard"] = pa.array(
            [dt1, dt2], pa.timestamp("us", tz="UTC")
        )
        sources["timestamp[us,tz=UTC]:nullable"] = pa.array(
            [dt1, None], pa.timestamp("us", tz="UTC")
        )
        sources["timestamp[us,tz=UTC]:empty"] = pa.array([], pa.timestamp("us", tz="UTC"))
        # Overflow: coercion to nanoseconds has a valid range (~1677-2262); a
        # far-future second-resolution timestamp cannot fit and should error.
        sources["timestamp[s]:overflow"] = pa.array(
            [datetime.datetime(2500, 1, 1)], pa.timestamp("s")
        )

        # =====================================================================
        # Duration types (also coerced to nanoseconds)
        # =====================================================================
        td1 = datetime.timedelta(days=1)
        td2 = datetime.timedelta(hours=2, minutes=30)
        for unit in ["s", "ms", "us", "ns"]:
            sources[f"duration[{unit}]:standard"] = pa.array([td1, td2], pa.duration(unit))
            sources[f"duration[{unit}]:nullable"] = pa.array([td1, None], pa.duration(unit))
            sources[f"duration[{unit}]:empty"] = pa.array([], pa.duration(unit))
        # Overflow: a duration beyond ~292 years exceeds the int64 nanosecond
        # range. Unlike timestamp overflow (which raises), coercing it silently
        # wraps around to a bogus value; this row pins that behavior.
        sources["duration[s]:overflow"] = pa.array(
            [datetime.timedelta(days=300 * 365)], pa.duration("s")
        )

        # =====================================================================
        # Date types. With the default date_as_object=True, pandas yields an
        # object-dtype Series of datetime.date, so coerce_temporal_nanoseconds
        # has nothing to coerce; the "date_as_object=False" column forces the
        # numeric datetime64[ns] path where the argument actually takes effect.
        # =====================================================================
        d1 = datetime.date(2024, 1, 1)
        d2 = datetime.date(2024, 6, 15)
        sources["date32:standard"] = pa.array([d1, d2], pa.date32())
        sources["date32:nullable"] = pa.array([d1, None], pa.date32())
        sources["date32:empty"] = pa.array([], pa.date32())
        sources["date64:standard"] = pa.array([d1, d2], pa.date64())
        sources["date64:nullable"] = pa.array([d1, None], pa.date64())
        sources["date64:empty"] = pa.array([], pa.date64())

        # =====================================================================
        # Time types (control: pandas yields object-dtype datetime.time, no
        # native time-of-day dtype, so coerce_temporal_nanoseconds is a no-op)
        # =====================================================================
        t1 = datetime.time(12, 30, 0)
        t2 = datetime.time(18, 45, 30)
        sources["time32[s]:standard"] = pa.array([t1, t2], pa.time32("s"))
        sources["time32[s]:nullable"] = pa.array([t1, None], pa.time32("s"))
        sources["time32[s]:empty"] = pa.array([], pa.time32("s"))
        sources["time32[ms]:standard"] = pa.array([t1, t2], pa.time32("ms"))
        sources["time32[ms]:nullable"] = pa.array([t1, None], pa.time32("ms"))
        sources["time32[ms]:empty"] = pa.array([], pa.time32("ms"))
        sources["time64[us]:standard"] = pa.array([t1, t2], pa.time64("us"))
        sources["time64[us]:nullable"] = pa.array([t1, None], pa.time64("us"))
        sources["time64[us]:empty"] = pa.array([], pa.time64("us"))
        sources["time64[ns]:standard"] = pa.array([t1, t2], pa.time64("ns"))
        sources["time64[ns]:nullable"] = pa.array([t1, None], pa.time64("ns"))
        sources["time64[ns]:empty"] = pa.array([], pa.time64("ns"))

        # =====================================================================
        # Non-temporal controls (unaffected by coerce_temporal_nanoseconds)
        # =====================================================================
        sources["int64:standard"] = pa.array([0, 1, -1], pa.int64())
        sources["int64:nullable"] = pa.array([0, 1, None], pa.int64())
        sources["float64:standard"] = pa.array([0.0, 1.5, -1.5], pa.float64())
        sources["string:standard"] = pa.array(["hello", "world", ""], pa.string())

        return sources

    # Output column recording to_pandas(coerce_temporal_nanoseconds=True).
    # date_as_object stays at its default (True).
    COL_PANDAS = "pandas series"

    # Output column recording to_pandas(coerce_temporal_nanoseconds=True,
    # date_as_object=False). date_as_object=False is the only path on which
    # coercion observably affects date types.
    COL_PANDAS_DATE_AS_OBJECT_FALSE = "pandas series (date_as_object=False)"

    def test_to_pandas_coerce_temporal_nanoseconds(self):
        """Test pa.Array.to_pandas(coerce_temporal_nanoseconds=True) against golden file."""
        sources = self._build_source_arrays()
        row_names = list(sources.keys())
        col_names = [
            "pyarrow array",
            self.COL_PANDAS,
            self.COL_PANDAS_DATE_AS_OBJECT_FALSE,
        ]

        # Version-specific expected values go here, keyed by (row, col), when a
        # newer pandas/PyArrow/NumPy legitimately changes a cell's output.
        # Add a LooseVersion-guarded block for each known drift.
        overrides: dict[tuple[str, str], str] = {}
        # Pandas 3 uses its dedicated string dtype for non-empty Arrow string arrays.
        if LooseVersion(pd.__version__) >= LooseVersion("3.0.0"):
            str_expected = "['hello', 'world', '']@Series[str]"
            overrides[("string:standard", self.COL_PANDAS)] = str_expected
            overrides[("string:standard", self.COL_PANDAS_DATE_AS_OBJECT_FALSE)] = str_expected

        def compute_cell(row_name, col_name):
            arr = sources[row_name]
            if col_name == "pyarrow array":
                return self.repr_value(arr, max_len=0)
            elif col_name == self.COL_PANDAS:
                return self._to_pandas_cell(arr, coerce_temporal_nanoseconds=True)
            elif col_name == self.COL_PANDAS_DATE_AS_OBJECT_FALSE:
                return self._to_pandas_cell(
                    arr, coerce_temporal_nanoseconds=True, date_as_object=False
                )
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_arrow_to_pandas_coerce_temporal",
            index_name="test case",
            overrides=overrides,
        )


@unittest.skipIf(
    not have_pyarrow or not have_pandas or not have_numpy,
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowArrayToPandasZeroCopyTests(_PyArrowToPandasTestBase):
    """
    Tests pa.Array.to_pandas(zero_copy_only=True) via golden file comparison.

    PySpark converts Arrow data to (numpy-backed) pandas objects throughout its
    conversion layer (``python/pyspark/sql/pandas/conversion.py``); whether a
    given Arrow type can make that conversion WITHOUT copying its buffers
    directly affects the memory and latency of ``toPandas`` and pandas UDFs.
    ``zero_copy_only=True`` makes PyArrow raise ``ArrowInvalid`` instead of
    silently copying, so it is the natural probe for "is this conversion
    zero-copy?".  These tests record, per Arrow type, whether the conversion is
    zero-copy so CI fails loudly if that changes across pandas/PyArrow/NumPy
    upgrades.

    Two output columns are recorded for each source array:

    - ``zero_copy_only=True``: PyArrow's own verdict -- ``Series[dtype]`` when the
      conversion is zero-copy, or ``ERR@ArrowInvalid`` when a copy is required.
    - ``numpy shares arrow buffer``: an INDEPENDENT verification (via
      ``np.shares_memory``) of whether the default ``to_pandas()`` result
      actually shares memory with the Arrow buffer.  This does not simply trust
      PyArrow's flag; it confirms the physical sharing.  The two columns are
      expected to agree except in vacuous or backend-specific cases (e.g. empty
      arrays, or pandas 3 arrow-backed string where the flag reports zero-copy
      but ``to_numpy()`` materializes) -- such differences are pinned as data.

    The row set targets the layout properties that determine zero-copy, rather
    than re-enumerating every Arrow type (that is covered by
    ``test_pyarrow_arrow_to_pandas_default.py``): fixed-width numerics with and
    without nulls, bool, string/binary, temporal types across units, sliced
    (offset) arrays, and single- vs multi-chunk ChunkedArrays.
    """

    def _build_source_arrays(self):
        """Build an ordered dict of named source PyArrow arrays for testing."""
        sources = {}

        # =====================================================================
        # Fixed-width numerics WITHOUT nulls: the canonical zero-copy case --
        # the Arrow value buffer already matches numpy's layout exactly.
        # =====================================================================
        for bits, pa_type in [
            (8, pa.int8()),
            (16, pa.int16()),
            (32, pa.int32()),
            (64, pa.int64()),
        ]:
            sources[f"int{bits}:no-null"] = pa.array([0, 1, -1], pa_type)
        sources["uint32:no-null"] = pa.array([0, 1, 2], pa.uint32())
        sources["float32:no-null"] = pa.array([0.0, 1.5, -1.5], pa.float32())
        sources["float64:no-null"] = pa.array([0.0, 1.5, -1.5], pa.float64())

        # =====================================================================
        # Same numerics WITH a null: Arrow tracks nulls in a separate validity
        # bitmap that numpy has no equivalent for, so pandas must promote/rebuild
        # (e.g. int -> float64 with NaN) -- a copy.
        # =====================================================================
        sources["int32:with-null"] = pa.array([0, 1, None], pa.int32())
        sources["int64:with-null"] = pa.array([0, 1, None], pa.int64())
        sources["float64:with-null"] = pa.array([0.0, 1.5, None], pa.float64())

        # =====================================================================
        # Boolean: Arrow stores bits (1 bit/value); numpy uses 1 byte/value, so
        # the buffer must be unpacked -- always a copy, even without nulls.
        # =====================================================================
        sources["bool:no-null"] = pa.array([True, False, True], pa.bool_())
        sources["bool:with-null"] = pa.array([True, None, False], pa.bool_())

        # =====================================================================
        # String / binary: variable-length data materialized into an object
        # array of Python str/bytes -- always a copy.
        # =====================================================================
        sources["string:no-null"] = pa.array(["hello", "world", ""], pa.string())
        sources["binary:no-null"] = pa.array([b"hello", b"world"], pa.binary())

        # =====================================================================
        # Temporal types. Fixed-width timestamp/duration convert zero-copy
        # without nulls (they map to numpy datetime64/timedelta64); date and
        # time map to object dtype and therefore always copy.  A nullable
        # timestamp requires a copy (bitmap reconciliation, like the numerics).
        # =====================================================================
        dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
        for unit in ["s", "ms", "us", "ns"]:
            sources[f"timestamp[{unit}]:no-null"] = pa.array([dt, dt], pa.timestamp(unit))
        sources["timestamp[us]:with-null"] = pa.array([dt, None], pa.timestamp("us"))
        td = datetime.timedelta(days=1)
        sources["duration[us]:no-null"] = pa.array([td, td], pa.duration("us"))
        sources["date32:no-null"] = pa.array(
            [datetime.date(2024, 1, 1), datetime.date(2024, 6, 15)], pa.date32()
        )
        sources["time64[us]:no-null"] = pa.array(
            [datetime.time(12, 30), datetime.time(18, 45)], pa.time64("us")
        )

        # =====================================================================
        # Sliced (offset) arrays: still zero-copy when the slice views a
        # contiguous no-null primitive region, but the numpy data starts partway
        # into the parent buffer -- the case that defeats naive address equality.
        # =====================================================================
        sources["int64:sliced"] = pa.array(list(range(10)), pa.int64()).slice(2, 3)
        sources["int64:sliced-with-null"] = pa.array([1, 2, None, 4, 5], pa.int64()).slice(1, 3)

        # =====================================================================
        # ChunkedArray: a single chunk is zero-copy, but multiple chunks must be
        # concatenated into one contiguous numpy buffer -- a copy.  This is the
        # common shape in real PySpark, where each partition is its own chunk.
        # =====================================================================
        sources["int64:single-chunk"] = pa.chunked_array([pa.array([1, 2, 3], pa.int64())])
        sources["int64:multi-chunk"] = pa.chunked_array(
            [pa.array([1, 2], pa.int64()), pa.array([3, 4], pa.int64())]
        )

        # =====================================================================
        # Empty and nested (controls). Empty reports zero-copy (nothing to copy)
        # but shares no buffer; nested types materialize to object -- a copy.
        # =====================================================================
        sources["int64:empty"] = pa.array([], pa.int64())
        sources["string:empty"] = pa.array([], pa.string())
        sources["list<int64>:no-null"] = pa.array([[1, 2], [3]], pa.list_(pa.int64()))
        sources["struct:no-null"] = pa.array([{"x": 1}, {"x": 2}], pa.struct([("x", pa.int64())]))

        return sources

    # Output column recording PyArrow's own zero-copy verdict.
    COL_ZERO_COPY_ONLY = "zero_copy_only=True"

    # Output column independently verifying physical memory sharing.
    COL_SHARES_BUFFER = "numpy shares arrow buffer"

    def test_to_pandas_zero_copy_only(self):
        """Test pa.Array.to_pandas(zero_copy_only=True) against golden file."""
        sources = self._build_source_arrays()
        row_names = list(sources.keys())
        col_names = [
            "pyarrow array",
            self.COL_ZERO_COPY_ONLY,
            self.COL_SHARES_BUFFER,
        ]

        # Version-specific expected values go here, keyed by (row, col), when a
        # newer pandas/PyArrow/NumPy legitimately changes a cell's output.
        overrides: dict[tuple[str, str], str] = {}
        # Pandas 3 makes Arrow string arrays convert zero-copy (dedicated string
        # dtype), so zero_copy_only=True succeeds instead of raising.  The
        # independent buffer check still reports not-shared because to_numpy()
        # materializes the string dtype to object.
        if LooseVersion(pd.__version__) >= LooseVersion("3.0.0"):
            overrides[("string:no-null", self.COL_ZERO_COPY_ONLY)] = (
                "['hello', 'world', '']@Series[str]"
            )

        def compute_cell(row_name, col_name):
            arr = sources[row_name]
            if col_name == "pyarrow array":
                return self.repr_value(arr, max_len=0)
            elif col_name == self.COL_ZERO_COPY_ONLY:
                return self._to_pandas_cell(arr, zero_copy_only=True)
            elif col_name == self.COL_SHARES_BUFFER:
                return self._numpy_shares_arrow_buffer(arr)
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_arrow_to_pandas_zero_copy",
            index_name="test case",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
