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

The shared ``_PyArrowToPandasTestBase`` (in ``test_pyarrow_arrow_to_pandas_default.py``)
holds the conversion helper and the source-array inventory.  Each class reuses the
inventory whole or by group and adds a test method per argument combination (its own
golden file), so a new argument is a new class rather than an edit to the existing ones.

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
    have_numpy,
    have_pandas,
    have_pyarrow,
    numpy_requirement_message,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

# Import the shared base (which defines no test_* methods), not a concrete test class,
# so that unittest does not collect and re-run the default file's tests here.
from pyspark.tests.upstream.pyarrow.test_pyarrow_arrow_to_pandas_default import (
    _PyArrowToPandasTestBase,
)

if have_pandas:
    import pandas as pd
if have_pyarrow:
    import pyarrow as pa
if have_numpy:
    import numpy as np


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
        """
        Reuse the base's temporal group (the types this argument targets), then add
        coercion-specific overflow rows, chunked timestamps, and non-temporal controls.
        """
        sources = self._temporal_sources()

        # Overflow: coercion to nanoseconds has a valid range (~1677-2262); a
        # far-future second-resolution timestamp cannot fit and should error.
        sources["timestamp[s]:overflow"] = pa.array(
            [datetime.datetime(2500, 1, 1)], pa.timestamp("s")
        )
        # Overflow: a duration beyond ~292 years exceeds the int64 nanosecond
        # range. Unlike timestamp overflow (which raises), coercing it silently
        # wraps around to a bogus value; this row pins that behavior.
        sources["duration[s]:overflow"] = pa.array(
            [datetime.timedelta(days=300 * 365)], pa.duration("s")
        )

        # Chunked timestamps: the base's _chunked_sources has no temporal rows, so pin
        # here that a chunked (non-ns) timestamp coerces to the same datetime64[ns] as
        # the equivalent contiguous Array.
        dt1 = datetime.datetime(2024, 1, 1, 12, 0, 0)
        dt2 = datetime.datetime(2024, 6, 15, 18, 30, 0)
        sources["timestamp[us]:single-chunk"] = pa.chunked_array(
            [pa.array([dt1, dt2], pa.timestamp("us"))]
        )
        sources["timestamp[us]:multi-chunk"] = pa.chunked_array(
            [pa.array([dt1], pa.timestamp("us")), pa.array([dt2], pa.timestamp("us"))]
        )

        # Non-temporal controls (unaffected by coerce_temporal_nanoseconds).
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
    -- every Arrow type it covers, in its standard / nullable / empty variants, plus
    the shared ChunkedArray layouts -- so both golden files pin the same types, and
    appends the one layout variant that only matters for zero-copy: sliced (offset)
    arrays.

    A second test method repeats those rows with ``types_mapper=pd.ArrowDtype``, which
    asks pandas to keep pointing at the Arrow buffers instead of materializing them
    into NumPy -- avoiding the copy is the point of that backend.  Its own golden file
    records the result, so the two can be read side by side.  PySpark takes this path
    in ``ArrowArrayToPandasConversion.convert_numpy``
    (``python/pyspark/sql/conversion.py``).
    """

    def _build_source_arrays(self):
        """
        Reuse the base's full inventory, then add the sliced layout variants that only
        matter for zero-copy: a slice views a contiguous no-null region, so it stays
        zero-copy even though its data starts partway into the parent buffer.  The
        chunk-count variants live in the base's ``_chunked_sources`` so every golden
        records their zero-copy behavior, not just this one.
        """
        sources = super()._build_source_arrays()

        sources["int64:sliced"] = pa.array(list(range(10)), pa.int64()).slice(2, 3)
        sources["int64:sliced-with-null"] = pa.array([1, 2, None, 4, 5], pa.int64()).slice(1, 3)

        return sources

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
                ("string:single-chunk", "['a', 'b']@Series[str]", "partial-copy"),
                ("string:multi-chunk", "['a', 'b', 'c']@Series[str]", "partial-copy"),
                ("string:multi-chunk-nullable", "['a', nan, 'c']@Series[str]", "partial-copy"),
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
        Reuse the base's full inventory, then add the integer variants it does not
        reach: its nullable values are small enough to survive ``float64``, and its
        nested rows are missing whole sub-lists rather than a single integer inside one.
        """
        sources = super()._build_source_arrays()

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
            override_outputs("string:single-chunk", "['a', 'b']@Series[str]")
            override_outputs("string:multi-chunk", "['a', 'b', 'c']@Series[str]")
            override_outputs("string:multi-chunk-nullable", "['a', nan, 'c']@Series[str]")

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


@unittest.skipIf(
    not have_pyarrow or not have_pandas or not have_numpy,
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowChunkedArrayToPandasMemoryFlagsTests(_PyArrowToPandasTestBase):
    """
    Tests pa.ChunkedArray.to_pandas() under the memory-tuning arguments
    ``self_destruct`` / ``split_blocks`` / ``use_threads`` via golden file comparison.

    Spark sets all three together for ``df.toPandas()`` when Arrow self-destruct is
    enabled (``python/pyspark/sql/pandas/conversion.py``), freeing each column's buffers
    as it is converted to keep peak memory near one column.  They form one unit --
    freeing (``self_destruct``) needs per-column blocks (``split_blocks``) and
    single-threaded conversion (``use_threads=False``) -- so this golden records the
    bundle (``spark memory options``) against ``default`` rather than a column per flag.
    The path always converts a ``pa.ChunkedArray`` (``Table.column(i)``), hence the rows.

    The arguments tune HOW the conversion runs, not WHAT it produces, so the bundle is
    expected to match the default.  Pinning that equivalence catches drift: if these
    arguments ever alter the output -- or ``self_destruct`` actually consumes the input,
    which today it does not at this level -- a cell moves.  A final column records whether
    the source is still readable after ``self_destruct=True``, since Spark treats freeing
    as an optional optimization, not a contract.  Each cell rebuilds its source from a
    factory so a destructive ``self_destruct`` cannot corrupt another cell.
    """

    def _memory_flag_source_factories(self):
        """
        Named factories, each returning a FRESH ChunkedArray per call, since
        ``self_destruct=True`` may consume its input.  Rows span the chunk-count axis
        across numeric, variable-width, and nested types (buffer layout differs by type).
        """
        struct_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        return {
            "int64:single-chunk": lambda: pa.chunked_array([pa.array([1, 2, 3], pa.int64())]),
            "int64:multi-chunk": lambda: pa.chunked_array(
                [pa.array([1, 2], pa.int64()), pa.array([3, 4], pa.int64())]
            ),
            "int64:multi-chunk-nullable": lambda: pa.chunked_array(
                [pa.array([1, None], pa.int64()), pa.array([2], pa.int64())]
            ),
            "int64:multi-chunk-with-empty": lambda: pa.chunked_array(
                [pa.array([1, 2], pa.int64()), pa.array([], pa.int64()), pa.array([3], pa.int64())]
            ),
            "int64:empty-chunk": lambda: pa.chunked_array([pa.array([], pa.int64())]),
            "float64:multi-chunk": lambda: pa.chunked_array(
                [pa.array([1.5, 2.5], pa.float64()), pa.array([3.5], pa.float64())]
            ),
            "string:multi-chunk": lambda: pa.chunked_array(
                [pa.array(["a", "b"], pa.string()), pa.array(["c"], pa.string())]
            ),
            "string:multi-chunk-nullable": lambda: pa.chunked_array(
                [pa.array(["a", None], pa.string()), pa.array(["c"], pa.string())]
            ),
            "list<int64>:multi-chunk": lambda: pa.chunked_array(
                [
                    pa.array([[1, 2], [3]], pa.list_(pa.int64())),
                    pa.array([[4]], pa.list_(pa.int64())),
                ]
            ),
            "struct:multi-chunk": lambda: pa.chunked_array(
                [
                    pa.array([{"x": 1, "y": "a"}], struct_type),
                    pa.array([{"x": 2, "y": "b"}], struct_type),
                ]
            ),
        }

    # Default (no memory arguments) -- the baseline the bundle should match.
    COL_DEFAULT = "default"
    # All three flags as convert_arrow_table_to_pandas passes them together.
    COL_SPARK_MEMORY_OPTIONS = "spark memory options"
    # Whether the source ChunkedArray is still readable after self_destruct=True.
    COL_SOURCE_READABLE = "source readable after self_destruct"

    # Kept as one dict so the column cannot drift from the call site it mirrors
    # (python/pyspark/sql/pandas/conversion.py).
    SPARK_MEMORY_OPTIONS = {
        "self_destruct": True,
        "split_blocks": True,
        "use_threads": False,
    }

    def _source_readable_after_self_destruct(self, factory) -> str:
        """
        Convert a fresh source with ``self_destruct=True``, then report whether it is
        still readable: ``readable``, or ``unreadable@<ExceptionClass>`` if freed.
        """
        arr = factory()
        try:
            arr.to_pandas(self_destruct=True)
        except Exception as e:
            return f"ERR@{type(e).__name__}"
        try:
            arr.to_pylist()
            return "readable"
        except Exception as e:
            return f"unreadable@{type(e).__name__}"

    def test_to_pandas_memory_flags(self):
        """Test pa.ChunkedArray.to_pandas() under the memory-tuning arguments."""
        factories = self._memory_flag_source_factories()
        row_names = list(factories.keys())
        col_names = [
            "pyarrow array",
            self.COL_DEFAULT,
            self.COL_SPARK_MEMORY_OPTIONS,
            self.COL_SOURCE_READABLE,
        ]

        # Version-specific expected values go here, keyed by (row, col), when a newer
        # pandas/PyArrow/NumPy legitimately changes a cell's output.  These arguments do
        # not touch strings, so a string row shifts in both value columns at once.
        overrides: dict[tuple[str, str], str] = {}
        if LooseVersion(pd.__version__) >= LooseVersion("3.0.0"):
            value_cols = [self.COL_DEFAULT, self.COL_SPARK_MEMORY_OPTIONS]
            for row, expected in [
                ("string:multi-chunk", "['a', 'b', 'c']@Series[str]"),
                ("string:multi-chunk-nullable", "['a', nan, 'c']@Series[str]"),
            ]:
                for col in value_cols:
                    overrides[(row, col)] = expected

        def compute_cell(row_name, col_name):
            factory = factories[row_name]
            if col_name == "pyarrow array":
                return self.repr_value(factory(), max_len=0)
            elif col_name == self.COL_DEFAULT:
                return self._to_pandas_cell(factory())
            elif col_name == self.COL_SPARK_MEMORY_OPTIONS:
                return self._to_pandas_cell(factory(), **self.SPARK_MEMORY_OPTIONS)
            elif col_name == self.COL_SOURCE_READABLE:
                return self._source_readable_after_self_destruct(factory)
            else:
                raise ValueError(f"unknown column: {col_name}")

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_chunked_array_to_pandas_memory_flags",
            index_name="test case",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
