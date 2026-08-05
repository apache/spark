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
arguments (e.g. ``zero_copy_only``, ``integer_object_nulls``) can be added as
additional classes without touching the existing ones.

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

# Imported as a module, not `from ... import PyArrowArrayToPandasDefaultTests`, so
# that unittest does not collect and re-run the default test class from this module.
from pyspark.tests.upstream.pyarrow import test_pyarrow_arrow_to_pandas_default

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
    def _arrow_buffers(arrow_obj):
        """
        Every non-null buffer backing ``arrow_obj``.

        A ChunkedArray has no buffers of its own -- its data lives in its chunks --
        so it is expanded first.  ``None`` entries (e.g. an absent validity bitmap)
        are skipped.
        """
        if isinstance(arrow_obj, pa.ChunkedArray):
            chunks = [arrow_obj.chunk(i) for i in range(arrow_obj.num_chunks)]
        else:
            chunks = [arrow_obj]

        buffers = []
        for chunk in chunks:
            for buffer in chunk.buffers():
                if buffer is not None:
                    buffers.append(buffer)
        return buffers

    @classmethod
    def _verify_zero_copy(cls, arr, **to_pandas_kwargs) -> str:
        """
        Independently verify whether ``to_pandas`` reused ``arr``'s buffers,
        instead of trusting PyArrow's own ``zero_copy_only`` verdict.

        The check inspects whatever storage pandas returned rather than assuming a
        numpy-backed Series, so it stays correct as pandas moves more dtypes to
        Arrow-backed storage.

        Returns ``"zero-copy"``, ``"partial-copy"``, ``"copied"``, or
        ``"ERR@<ExceptionClass>"``.
        """
        try:
            series = arr.to_pandas(**to_pandas_kwargs)
        except Exception as e:
            return f"ERR@{type(e).__name__}"

        backing_array = series.array

        # Arrow-backed result: compare buffer addresses, reading the stored data
        # back through the public __arrow_array__ protocol.  to_numpy() would
        # materialize a copy here and wrongly report no sharing.  Keying on the
        # protocol rather than on ArrowDtype also covers dtypes that are
        # Arrow-backed without being ArrowDtype, such as pandas 3's string.
        #
        # The result has several buffers (validity, offsets, values) and can borrow
        # some while allocating others, so count how many it borrowed rather than
        # stopping at the first match.  The result's buffers are the denominator
        # because the question is what pandas had to allocate.
        if hasattr(backing_array, "__arrow_array__"):
            source_addresses = {buffer.address for buffer in cls._arrow_buffers(arr)}
            stored_buffers = cls._arrow_buffers(pa.array(backing_array))
            borrowed = 0
            for buffer in stored_buffers:
                if buffer.address in source_addresses:
                    borrowed += 1
            if borrowed == len(stored_buffers):
                return "zero-copy"
            return "partial-copy" if borrowed else "copied"

        # numpy-backed result: np.shares_memory accounts for slice offsets, so it
        # is robust where raw address equality is not.
        pandas_values = series.to_numpy()
        for buffer in cls._arrow_buffers(arr):
            if np.shares_memory(np.frombuffer(buffer, dtype=np.uint8), pandas_values):
                return "zero-copy"

        # Zero bytes cannot overlap, so an empty result is zero-copy if the numpy
        # array still borrows the Arrow array (numpy's .base is its memory owner).
        if len(arr) == 0:
            owner = pandas_values
            while (base := getattr(owner, "base", None)) is not None:
                if base is arr:
                    return "zero-copy"
                owner = base

        return "copied"


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

    Three output columns are recorded for each source array:

    - ``zero_copy_only=False``: the default, where PyArrow silently copies when a
      view is not possible.  This always succeeds, and records the resulting dtype.
    - ``zero_copy_only=True``: PyArrow's own verdict -- ``Series[dtype]`` when the
      conversion is zero-copy, or ``ERR@ArrowInvalid`` when a copy is required.
    - ``verified zero-copy``: an INDEPENDENT check of whether the conversion
      actually reused the Arrow buffers, rather than trusting PyArrow's flag.
      ``zero-copy`` when every buffer the result needs was borrowed, ``copied``
      when none were, and ``partial-copy`` when only some were (e.g. the values
      reused but the offsets reallocated).  The last two columns are expected to
      agree, but are recorded separately rather than asserted equal, because they
      do not always: a tz-aware timestamp reports ``zero_copy_only=True`` while
      pandas materializes it into a ``DatetimeTZDtype`` array that shares nothing.
      Pinning both as data makes such disagreements visible instead of hiding them
      behind an assertion.

    The row set mirrors ``test_pyarrow_arrow_to_pandas_default.py``'s rows exactly
    -- every Arrow type it covers, in its standard / nullable / empty variants --
    so both golden files pin the same types, and appends the layout variants that
    only matter for zero-copy: sliced (offset) arrays and single- vs multi-chunk
    ChunkedArrays.

    A second test method repeats those rows with ``types_mapper=pd.ArrowDtype``, which
    asks pandas to keep pointing at the Arrow buffers instead of materializing them
    into NumPy -- avoiding the copy is the point of that backend.  Its own golden file
    records the result, so the two can be read side by side.  PySpark takes this path
    in ``ArrowArrayToPandasConversion.convert_numpy``
    (``python/pyspark/sql/conversion.py``).
    """

    def _build_source_arrays(self):
        """
        Reuse every row of the sibling default test, then add the layout variants
        that only matter for zero-copy.

        Sharing the row set keeps both golden files pinning the same types, and
        means a type added to the default test is covered here automatically.
        A slice still views a contiguous no-null region, so it stays zero-copy even
        though its data starts partway into the parent buffer.  A single chunk is
        zero-copy, but multiple chunks must be concatenated into one contiguous
        numpy buffer -- a copy.  Multi-chunk is the common shape in real PySpark,
        where each partition arrives as its own chunk.
        """
        default_tests = test_pyarrow_arrow_to_pandas_default.PyArrowArrayToPandasDefaultTests
        sources = default_tests._build_source_arrays(self)

        sources["int64:sliced"] = pa.array(list(range(10)), pa.int64()).slice(2, 3)
        sources["int64:sliced-with-null"] = pa.array([1, 2, None, 4, 5], pa.int64()).slice(1, 3)
        sources["int64:single-chunk"] = pa.chunked_array([pa.array([1, 2, 3], pa.int64())])
        sources["int64:multi-chunk"] = pa.chunked_array(
            [pa.array([1, 2], pa.int64()), pa.array([3, 4], pa.int64())]
        )

        return sources

    # Output column for the default zero_copy_only=False: PyArrow silently copies
    # when a view is not possible, so this always succeeds and records the dtype.
    COL_ZERO_COPY_OFF = "zero_copy_only=False"

    # Output column for zero_copy_only=True: PyArrow raises ArrowInvalid instead
    # of copying, so this records its verdict on whether a view was possible.
    COL_ZERO_COPY_ON = "zero_copy_only=True"

    # Output column independently verifying that buffers were actually reused.
    COL_VERIFIED = "verified zero-copy"

    def test_to_pandas_zero_copy_only(self):
        """Test pa.Array.to_pandas(zero_copy_only=True/False) against golden file."""
        sources = self._build_source_arrays()
        row_names = list(sources.keys())
        col_names = [
            "pyarrow array",
            self.COL_ZERO_COPY_OFF,
            self.COL_ZERO_COPY_ON,
            self.COL_VERIFIED,
        ]

        # Version-specific expected values go here, keyed by (row, col), when a
        # newer pandas/PyArrow/NumPy legitimately changes a cell's output.
        overrides: dict[tuple[str, str], str] = {}
        # Pandas 3 renders non-empty Arrow string arrays with its dedicated string
        # dtype, so the copying conversion reports Series[str] instead of object.
        if LooseVersion(pd.__version__) >= LooseVersion("3.0.0"):
            # Pandas stores that dtype as large_string, so `string` keeps its values
            # but has its 32-bit offsets rebuilt as 64-bit, while `large_string`
            # already matches and passes through untouched.
            non_empty_strings = [
                ("string:standard", "['hello', 'world', '']@Series[str]", "partial-copy"),
                ("string:nullable", "['hello', nan, 'world']@Series[str]", "partial-copy"),
                ("large_string:standard", "['hello', 'world']@Series[str]", "zero-copy"),
                ("large_string:nullable", "['hello', nan]@Series[str]", "zero-copy"),
            ]
            for row, expected, _ in non_empty_strings:
                overrides[(row, self.COL_ZERO_COPY_OFF)] = expected

            # Only from PyArrow 24 is that string conversion actually Arrow-backed,
            # so zero_copy_only succeeds and the buffers are genuinely reused.  On
            # PyArrow < 24 it still materializes, so the pandas 2 expectations
            # (ERR@ArrowInvalid / copied) remain correct and need no override.
            if LooseVersion(pa.__version__) >= LooseVersion("24.0.0"):
                for row, expected, verified in non_empty_strings:
                    overrides[(row, self.COL_ZERO_COPY_ON)] = expected
                    overrides[(row, self.COL_VERIFIED)] = verified
                # Empty arrays also gain the string dtype in PyArrow 24.  Offsets are
                # the only buffer an empty result has, so rebuilding them shares
                # nothing at all -- fully copied rather than partial.
                for row, verified in [
                    ("string:empty", "copied"),
                    ("large_string:empty", "zero-copy"),
                ]:
                    overrides[(row, self.COL_ZERO_COPY_OFF)] = "[]@Series[str]"
                    overrides[(row, self.COL_ZERO_COPY_ON)] = "[]@Series[str]"
                    overrides[(row, self.COL_VERIFIED)] = verified

        def compute_cell(row_name, col_name):
            arr = sources[row_name]
            if col_name == "pyarrow array":
                return self.repr_value(arr, max_len=0)
            elif col_name == self.COL_ZERO_COPY_OFF:
                return self._to_pandas_cell(arr, zero_copy_only=False)
            elif col_name == self.COL_ZERO_COPY_ON:
                return self._to_pandas_cell(arr, zero_copy_only=True)
            elif col_name == self.COL_VERIFIED:
                return self._verify_zero_copy(arr)
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

    # Output columns for the Arrow-backed conversion.  Both zero_copy_only states are
    # recorded even though the flag is expected to make no difference here
    COL_ARROW_ZERO_COPY_OFF = "types_mapper=pd.ArrowDtype, zero_copy_only=False"
    COL_ARROW_ZERO_COPY_ON = "types_mapper=pd.ArrowDtype, zero_copy_only=True"
    COL_ARROW_VERIFIED = "verified zero-copy"

    def test_to_pandas_zero_copy_only_arrow_backed(self):
        """Test pa.Array.to_pandas(types_mapper=pd.ArrowDtype) against golden file."""
        sources = self._build_source_arrays()
        row_names = list(sources.keys())
        col_names = [
            "pyarrow array",
            self.COL_ARROW_ZERO_COPY_OFF,
            self.COL_ARROW_ZERO_COPY_ON,
            self.COL_ARROW_VERIFIED,
        ]

        # Version-specific expected values go here, keyed by (row, col), when a
        # newer pandas/PyArrow/NumPy legitimately changes a cell's output.
        overrides: dict[tuple[str, str], str] = {}

        def compute_cell(row_name, col_name):
            arr = sources[row_name]
            if col_name == "pyarrow array":
                return self.repr_value(arr, max_len=0)
            elif col_name == self.COL_ARROW_ZERO_COPY_OFF:
                return self._to_pandas_cell(arr, types_mapper=pd.ArrowDtype, zero_copy_only=False)
            elif col_name == self.COL_ARROW_ZERO_COPY_ON:
                return self._to_pandas_cell(arr, types_mapper=pd.ArrowDtype, zero_copy_only=True)
            elif col_name == self.COL_ARROW_VERIFIED:
                return self._verify_zero_copy(arr, types_mapper=pd.ArrowDtype)
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_arrow_to_pandas_zero_copy_arrow_backed",
            index_name="test case",
            overrides=overrides,
        )


@unittest.skipIf(
    not have_pyarrow or not have_pandas or not have_numpy,
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowArrayToPandasIntegerObjectNullsTests(_PyArrowToPandasTestBase):
    """
    Tests pa.Array.to_pandas(integer_object_nulls=True) via golden file comparison.

    numpy integers have no null, so by default PyArrow widens a null-bearing integer
    array to ``float64`` with ``NaN`` -- which cannot hold every int64 exactly, so a
    large value silently changes.  ``integer_object_nulls=True`` keeps ``object``
    dtype (Python ``int`` and ``None``) instead, preserving the values.

    PySpark passes it in ``ArrowArrayToPandasConversion.convert_legacy``
    (``python/pyspark/sql/conversion.py``), bundled with ``date_as_object`` and
    ``coerce_temporal_nanoseconds``, then narrows the object Series to a nullable
    extension dtype (``Int8Dtype`` .. ``Int64Dtype``) -- the only bridge from Arrow to
    those dtypes that avoids ``float64``.

    Three output columns are recorded per source array: the argument off, on, and the
    full ``pandas_options`` dict ``convert_legacy`` passes.  The last is not a
    duplicate of the second -- ``coerce_temporal_nanoseconds`` shifts the temporal
    rows to ``ns`` -- and pinning the call as Spark makes it also catches PyArrow
    changing the ``date_as_object=True`` default it relies on.

    The row set mirrors ``test_pyarrow_arrow_to_pandas_default.py``'s rows so both
    golden files pin the same types, and appends the integer variants those rows do
    not reach (see ``_build_source_arrays``).
    """

    def _build_source_arrays(self):
        """
        Reuse every row of the sibling default test, then add the integer variants
        that only matter for this argument: the shared rows' nullable values are
        small enough to survive ``float64``, and their nested rows are missing whole
        sub-lists rather than a single integer inside one.
        """
        default_tests = test_pyarrow_arrow_to_pandas_default.PyArrowArrayToPandasDefaultTests
        sources = default_tests._build_source_arrays(self)

        # Each width's min and max alongside a null.  The shared rows use small values,
        # which float64 represents exactly; only at 64 bits does the range exceed its
        # 53-bit mantissa and the value itself change.
        for pa_type in [
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.uint8(),
            pa.uint16(),
            pa.uint32(),
            pa.uint64(),
        ]:
            if pa.types.is_signed_integer(pa_type):
                bounds = [2 ** (pa_type.bit_width - 1) - 1, -(2 ** (pa_type.bit_width - 1))]
            else:
                bounds = [2**pa_type.bit_width - 1, 0]
            sources[f"{pa_type}:extremes-nullable"] = pa.array(bounds + [None], pa_type)

        # Every value is null, so there is no integer left to convert.
        sources["int64:all-null"] = pa.array([None, None], pa.int64())
        # Values split across buffers, with the null in one chunk and not the other.
        # Reachable in production: cogrouped applyInPandas is the one UDF path that does
        # not call combine_chunks() first.
        sources["int64:multi-chunk-nullable"] = pa.chunked_array(
            [pa.array([1, None], pa.int64()), pa.array([2], pa.int64())]
        )

        # Nested types whose null is an integer ELEMENT, not a missing sub-list: the
        # shared rows only cover the latter, which this argument does not affect.
        # These are also the types convert_legacy still serves.
        sources["list<int64>:null-element"] = pa.array([[1, None], [2, 3]], pa.list_(pa.int64()))
        sources["list<int64>:null-element-extreme"] = pa.array(
            [[2**63 - 1, None]], pa.list_(pa.int64())
        )
        sources["large_list<int64>:null-element"] = pa.array([[1, None]], pa.large_list(pa.int64()))
        sources["fixed_size_list<int64>[3]:null-element"] = pa.array(
            [[1, None, 3]], pa.list_(pa.int64(), 3)
        )
        sources["list<list<int64>>:null-element"] = pa.array(
            [[[1, None], [2]]], pa.list_(pa.list_(pa.int64()))
        )
        sources["struct:null-int-field"] = pa.array(
            [{"x": 1, "y": "a"}, {"x": None, "y": "b"}],
            pa.struct([("x", pa.int64()), ("y", pa.string())]),
        )
        sources["map<string,int64>:null-value"] = pa.array(
            [[("a", 1), ("b", None)]], pa.map_(pa.string(), pa.int64())
        )
        # Control: dictionary encoding stores the distinct values once plus an index per
        # row, so the null lives in the indices and the int64 values hold none.  With no
        # null to represent there, the argument has nothing to decide -- both columns
        # stay category, whose own values also remain int64.
        sources["dictionary<int64>:nullable"] = pa.array(
            [1, None, 1], pa.int64()
        ).dictionary_encode()

        return sources

    # Output column for the default: a null-bearing integer array widens to float64.
    COL_INTEGER_OBJECT_NULLS_OFF = "integer_object_nulls=False"
    # Output column for the argument on: the result stays object dtype.
    COL_INTEGER_OBJECT_NULLS_ON = "integer_object_nulls=True"
    # Output column for all three arguments as convert_legacy passes them together.
    COL_SPARK_PANDAS_OPTIONS = "spark pandas_options"

    # Kept as one dict so the column cannot drift from the call site it mirrors.
    SPARK_PANDAS_OPTIONS = {
        "date_as_object": True,
        "coerce_temporal_nanoseconds": True,
        "integer_object_nulls": True,
    }

    def test_to_pandas_integer_object_nulls(self):
        """Test pa.Array.to_pandas(integer_object_nulls=True/False) against golden file."""
        sources = self._build_source_arrays()
        row_names = list(sources.keys())
        col_names = [
            "pyarrow array",
            self.COL_INTEGER_OBJECT_NULLS_OFF,
            self.COL_INTEGER_OBJECT_NULLS_ON,
            self.COL_SPARK_PANDAS_OPTIONS,
        ]

        # Version-specific expected values go here, keyed by (row, col), when a newer
        # pandas/PyArrow/NumPy legitimately changes a cell's output.  This argument does
        # not touch strings, so a string row shifts in every output column at once.
        overrides: dict[tuple[str, str], str] = {}

        def override_outputs(row: str, expected: str) -> None:
            for col in col_names[1:]:  # every column but the "pyarrow array" input
                overrides[(row, col)] = expected

        pandas_3_or_later = LooseVersion(pd.__version__) >= LooseVersion("3.0.0")
        pyarrow_24_or_later = LooseVersion(pa.__version__) >= LooseVersion("24.0.0")

        # Pandas 3 renders Arrow string arrays with its dedicated string dtype.
        if pandas_3_or_later:
            override_outputs("string:standard", "['hello', 'world', '']@Series[str]")
            override_outputs("string:nullable", "['hello', nan, 'world']@Series[str]")
            override_outputs("large_string:standard", "['hello', 'world']@Series[str]")
            override_outputs("large_string:nullable", "['hello', nan]@Series[str]")

        # Empty ones stay object until PyArrow 24, so the baseline holds before that.
        # Spark supports PyArrow 18+, so both branches are reachable.
        if pandas_3_or_later and pyarrow_24_or_later:
            override_outputs("string:empty", "[]@Series[str]")
            override_outputs("large_string:empty", "[]@Series[str]")

        def compute_cell(row_name, col_name):
            arr = sources[row_name]
            if col_name == "pyarrow array":
                return self.repr_value(arr, max_len=0)
            elif col_name == self.COL_INTEGER_OBJECT_NULLS_OFF:
                return self._to_pandas_cell(arr, integer_object_nulls=False)
            elif col_name == self.COL_INTEGER_OBJECT_NULLS_ON:
                return self._to_pandas_cell(arr, integer_object_nulls=True)
            elif col_name == self.COL_SPARK_PANDAS_OPTIONS:
                return self._to_pandas_cell(arr, **self.SPARK_PANDAS_OPTIONS)
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_arrow_to_pandas_integer_object_nulls",
            index_name="test case",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
