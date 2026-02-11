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
Tests for PyArrow's pa.Array.cast() method using golden file comparison.

This test suite covers both safe=True (default) and safe=False modes:
- safe=True: Checks for overflows and unsafe conversions, raises errors on failure
- safe=False: Allows potentially unsafe conversions (truncation, overflow wrapping)

Each mode generates separate golden files to capture their different behaviors.

## Golden File Cell Format

Each cell in the golden file uses the value@type format:

- Success: [0, 1, null]@int16 — element values via scalar.as_py() and Arrow type after cast
- Failure: ERR@ArrowNotImplementedError — the exception class name

## Regenerating Golden Files

Set SPARK_GENERATE_GOLDEN_FILES=1 before running:

    SPARK_GENERATE_GOLDEN_FILES=1 python -m pytest \\
        python/pyspark/tests/upstream/pyarrow/test_pyarrow_array_cast.py

If package tabulate (https://pypi.org/project/tabulate/) is installed,
it will also regenerate the Markdown files.

## PyArrow Version Compatibility

The golden files capture behavior for a specific PyArrow version.
Regenerate when upgrading PyArrow, as cast support may change between versions.

Some known version-dependent behaviors:
| Feature                                 | PyArrow < 19   | PyArrow 19-20  | PyArrow >= 21  |
|-----------------------------------------|----------------|----------------|----------------|
| struct cast: field name mismatch        | ArrowTypeError | supported      | supported      |
| struct cast: field reorder              | ArrowTypeError | ArrowTypeError | supported      |
| float16 scalar.as_py()                  | np.float16     | np.float16     | Python float   |
| pa.array(floats, pa.float16()) natively | requires numpy | requires numpy | native         |
"""

import inspect
import os
import unittest
from decimal import Decimal
from typing import Callable, List, Optional

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

if have_pyarrow:
    import pyarrow as pa
if have_numpy:
    import numpy as np


# ============================================================
# Base Test Class
# ============================================================


class _PyArrowCastTestBase(GoldenFileTestMixin, unittest.TestCase):
    """Base class for PyArrow cast golden file tests with shared helpers."""

    @staticmethod
    def _make_float16_array(values):
        """
        Create a float16 PyArrow array from Python float values.

        PyArrow < 21 requires numpy.float16 instances to create float16 arrays,
        while PyArrow >= 21 accepts Python floats directly.
        """
        if LooseVersion(pa.__version__) >= LooseVersion("21.0.0"):
            return pa.array(values, pa.float16())
        else:
            import numpy as np

            np_values = [np.float16(v) if v is not None else None for v in values]
            return pa.array(np_values, pa.float16())

    def _try_cast(self, src_arr, tgt_type, safe=True):
        """
        Try casting a source array to target type and return a value@type string.

        Uses repr_value() from GoldenFileTestMixin, which formats PyArrow arrays
        as "[val1, val2, null]@arrow_type" using each scalar's as_py() value.

        Parameters
        ----------
        src_arr : pa.Array
            Source PyArrow array to cast
        tgt_type : pa.DataType
            Target PyArrow type
        safe : bool, default True
            If True, check for overflows and unsafe conversions.
            If False, allow potentially unsafe conversions.

        Returns
        -------
        str
            On success: "[val1, val2, null]@arrow_type"
                e.g. "[0, 1, -1, 127, -128, null]@int16"
            On failure: "ERR@<exception_class_name>"
                e.g. "ERR@ArrowNotImplementedError"
        """
        try:
            result = src_arr.cast(tgt_type, safe=safe)
            return self.repr_value(result, max_len=0)
        except Exception as e:
            return f"ERR@{type(e).__name__}"

    def compare_or_generate_golden_matrix(
        self,
        row_names: List[str],
        col_names: List[str],
        compute_cell: Callable[[str, str], str],
        golden_file_prefix: str,
        index_name: str = "source \\ target",
        overrides: Optional[dict[tuple[str, str], str]] = None,
    ) -> None:
        """
        Run a matrix of computations and compare against (or generate) a golden file.

        1. If SPARK_GENERATE_GOLDEN_FILES=1, compute every cell, build a
           DataFrame, and save it as the new golden CSV / Markdown file.
        2. Otherwise, load the existing golden file and assert that every cell
           matches the freshly computed value.

        Parameters
        ----------
        row_names : list[str]
            Ordered row labels (becomes the DataFrame index).
        col_names : list[str]
            Ordered column labels.
        compute_cell : (row_name, col_name) -> str
            Function that computes the string result for one cell.
        golden_file_prefix : str
            Prefix for the golden CSV/MD files (without extension).
            Files are placed in the same directory as the concrete test file.
        index_name : str, default "source \\ target"
            Name for the index column in the golden file.
        overrides : dict[(row, col) -> str], optional
            Version-specific expected values that take precedence over the golden
            file.  Use this to document known behavioral differences across
            library versions (e.g. PyArrow 18 vs 22) directly in the test code,
            so that the same golden file works for multiple versions.
        """
        generating = self.is_generating_golden()

        test_dir = os.path.dirname(inspect.getfile(type(self)))
        golden_csv = os.path.join(test_dir, f"{golden_file_prefix}.csv")
        golden_md = os.path.join(test_dir, f"{golden_file_prefix}.md")

        golden = None
        if not generating:
            golden = self.load_golden_csv(golden_csv)

        errors = []
        results = {}

        for row_name in row_names:
            for col_name in col_names:
                result = compute_cell(row_name, col_name)
                results[(row_name, col_name)] = result

                if not generating:
                    if overrides and (row_name, col_name) in overrides:
                        expected = overrides[(row_name, col_name)]
                    else:
                        expected = golden.loc[row_name, col_name]
                    if expected != result:
                        errors.append(
                            f"{row_name} -> {col_name}: " f"expected '{expected}', got '{result}'"
                        )

        if generating:
            import pandas as pd

            index = pd.Index(row_names, name=index_name)
            df = pd.DataFrame(index=index)
            for col_name in col_names:
                df[col_name] = [results[(row, col_name)] for row in row_names]
            self.save_golden(df, golden_csv, golden_md)
        else:
            self.assertEqual(
                len(errors),
                0,
                f"\n{len(errors)} golden file mismatches:\n" + "\n".join(errors),
            )


# ============================================================
# Scalar Type Cast Tests
# ============================================================


@unittest.skipIf(
    not have_pyarrow
    or not have_pandas
    or not have_numpy
    or LooseVersion(np.__version__) < LooseVersion("2.0.0"),
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowScalarTypeCastTests(_PyArrowCastTestBase):
    """
    Tests all scalar-to-scalar type cast combinations via golden file comparison.

    Covers:
    - Integers: int8, int16, int32, int64, uint8, uint16, uint32, uint64
    - Floats: float16, float32, float64
    - Boolean: bool
    - Strings: string, large_string
    - Binary: binary, large_binary, fixed_size_binary
    - Decimal: decimal128, decimal256
    - Date: date32, date64
    - Timestamp: timestamp(s/ms/us/ns), with/without timezone
    - Duration: duration(s/ms/us/ns)
    - Time: time32(s/ms), time64(us/ns)
    """

    # ----- source case helpers -----

    def _signed_int_cases(self, pa_type, max_val, min_val):
        name = self.repr_type(pa_type)
        return [
            (f"{name}:standard", pa.array([0, 1, None], pa_type)),
            (f"{name}:negative", pa.array([-1, None], pa_type)),
            (f"{name}:max_min", pa.array([max_val, min_val, None], pa_type)),
        ]

    def _unsigned_int_cases(self, pa_type, max_val):
        name = self.repr_type(pa_type)
        return [
            (f"{name}:standard", pa.array([0, 1, None], pa_type)),
            (f"{name}:max", pa.array([max_val, None], pa_type)),
        ]

    def _standard_negative_cases(self, pa_type):
        name = self.repr_type(pa_type)
        return [
            (f"{name}:standard", pa.array([0, 1, None], pa_type)),
            (f"{name}:negative", pa.array([-1, None], pa_type)),
        ]

    # ----- target types -----

    @staticmethod
    def _get_target_types():
        return [
            # Integers
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.uint8(),
            pa.uint16(),
            pa.uint32(),
            pa.uint64(),
            # Floats
            pa.float16(),
            pa.float32(),
            pa.float64(),
            # Boolean
            pa.bool_(),
            # Strings
            pa.string(),
            pa.large_string(),
            # Binary
            pa.binary(),
            pa.large_binary(),
            pa.binary(16),
            # Decimal
            pa.decimal128(38, 10),
            pa.decimal256(76, 10),
            # Date
            pa.date32(),
            pa.date64(),
            # Timestamp (NTZ)
            pa.timestamp("s"),
            pa.timestamp("ms"),
            pa.timestamp("us"),
            pa.timestamp("ns"),
            # Timestamp (UTC)
            pa.timestamp("s", tz="UTC"),
            pa.timestamp("ms", tz="UTC"),
            pa.timestamp("us", tz="UTC"),
            pa.timestamp("ns", tz="UTC"),
            # Timestamp (other TZ)
            pa.timestamp("s", tz="America/New_York"),
            pa.timestamp("s", tz="Asia/Shanghai"),
            # Duration
            pa.duration("s"),
            pa.duration("ms"),
            pa.duration("us"),
            pa.duration("ns"),
            # Time
            pa.time32("s"),
            pa.time32("ms"),
            pa.time64("us"),
            pa.time64("ns"),
        ]

    # ----- source arrays -----

    def _get_source_arrays(self):
        """
        Create test arrays for all scalar types, split into separate edge-case
        categories.

        Each source type has multiple test cases so that edge-case values don't
        mask the behavior of other values.  For example int8:standard tests
        [0, 1, None] and int8:negative tests [-1, None] separately, so you can
        see that int8:standard -> uint8 succeeds while int8:negative -> uint8
        fails.
        """
        cases = []

        # --- Signed integers: standard, negative, max_min ---
        cases += self._signed_int_cases(pa.int8(), 127, -128)
        cases += self._signed_int_cases(pa.int16(), 32767, -32768)
        cases += self._signed_int_cases(pa.int32(), 2147483647, -2147483648)
        cases += self._signed_int_cases(pa.int64(), 2147483647, -2147483648)

        # --- Unsigned integers: standard, max ---
        cases += self._unsigned_int_cases(pa.uint8(), 255)
        cases += self._unsigned_int_cases(pa.uint16(), 65535)
        cases += self._unsigned_int_cases(pa.uint32(), 4294967295)
        cases += self._unsigned_int_cases(pa.uint64(), 4294967295)

        # --- Floats: standard, special, fractional ---
        f16_name = self.repr_type(pa.float16())
        cases += [
            (f"{f16_name}:standard", self._make_float16_array([0.0, 1.5, -1.5, None])),
            (
                f"{f16_name}:special",
                self._make_float16_array([float("inf"), float("nan"), None]),
            ),
            (f"{f16_name}:fractional", self._make_float16_array([0.1, 0.9, None])),
        ]
        for ftype in [pa.float32(), pa.float64()]:
            fname = self.repr_type(ftype)
            cases += [
                (f"{fname}:standard", pa.array([0.0, 1.5, -1.5, None], ftype)),
                (
                    f"{fname}:special",
                    pa.array([float("inf"), float("-inf"), float("nan"), None], ftype),
                ),
                (f"{fname}:fractional", pa.array([0.1, 0.9, None], ftype)),
            ]

        # --- Boolean ---
        cases += [
            (f"{self.repr_type(pa.bool_())}:standard", pa.array([True, False, None], pa.bool_()))
        ]

        # --- Strings: numeric, alpha, unicode ---
        for stype in [pa.string(), pa.large_string()]:
            sname = self.repr_type(stype)
            cases += [
                (f"{sname}:numeric", pa.array(["0", "1", "-1", None], stype)),
                (f"{sname}:alpha", pa.array(["abc", "", None], stype)),
                (
                    f"{sname}:unicode",
                    pa.array(
                        ["\u4f60\u597d", "\u0645\u0631\u062d\u0628\u0627", "\U0001f389", None],
                        stype,
                    ),
                ),
            ]

        # --- Binary ---
        for btype in [pa.binary(), pa.large_binary()]:
            bname = self.repr_type(btype)
            cases += [
                (f"{bname}:standard", pa.array([b"\x00", b"\xff", b"hello", b"", None], btype)),
            ]
        fsb_type = pa.binary(16)
        cases += [
            (
                f"{self.repr_type(fsb_type)}:standard",
                pa.array([b"0123456789abcdef", b"\x00" * 16, None], fsb_type),
            ),
        ]

        # --- Decimal: standard, large ---
        for dtype in [pa.decimal128(38, 10), pa.decimal256(76, 10)]:
            dname = self.repr_type(dtype)
            cases += [
                (
                    f"{dname}:standard",
                    pa.array([Decimal("0"), Decimal("1.5"), Decimal("-1.5"), None], dtype),
                ),
                (f"{dname}:large", pa.array([Decimal("9999999999"), None], dtype)),
            ]

        # --- Date: standard, negative ---
        for dtype in [pa.date32(), pa.date64()]:
            dname = self.repr_type(dtype)
            if dtype == pa.date32():
                cases += [
                    (f"{dname}:standard", pa.array([0, 1, None], dtype)),
                    (f"{dname}:negative", pa.array([-1, None], dtype)),
                ]
            else:
                cases += [
                    (f"{dname}:standard", pa.array([0, 86400000, None], dtype)),
                    (f"{dname}:negative", pa.array([-86400000, None], dtype)),
                ]

        # --- Timestamps (all 10 variants): standard, negative ---
        ts_types = [
            pa.timestamp("s"),
            pa.timestamp("ms"),
            pa.timestamp("us"),
            pa.timestamp("ns"),
            pa.timestamp("s", tz="UTC"),
            pa.timestamp("ms", tz="UTC"),
            pa.timestamp("us", tz="UTC"),
            pa.timestamp("ns", tz="UTC"),
            pa.timestamp("s", tz="America/New_York"),
            pa.timestamp("s", tz="Asia/Shanghai"),
        ]
        for ttype in ts_types:
            cases += self._standard_negative_cases(ttype)

        # --- Duration: standard, negative ---
        for unit in ["s", "ms", "us", "ns"]:
            cases += self._standard_negative_cases(pa.duration(unit))

        # --- Time: standard, noon ---
        time_defs = [
            (pa.time32("s"), 1, 43200),
            (pa.time32("ms"), 1000, 43200000),
            (pa.time64("us"), 1000000, 43200000000),
            (pa.time64("ns"), 1000000000, 43200000000000),
        ]
        for ttype, one_unit, noon_val in time_defs:
            tname = self.repr_type(ttype)
            cases += [
                (f"{tname}:standard", pa.array([0, one_unit, None], ttype)),
                (f"{tname}:noon", pa.array([noon_val, None], ttype)),
            ]

        source_names = [name for name, _ in cases]
        source_arrays = dict(cases)
        return source_names, source_arrays

    # ----- version overrides -----

    @classmethod
    def _version_overrides_safe(cls):
        """
        Build overrides for known PyArrow version-dependent behaviors (safe=True mode).

        PyArrow < 21: str(scalar) for float16 uses numpy's formatting
        (via np.float16), which may vary across numpy versions.
        The golden file uses PyArrow >= 21 output (Python float).
        We compute the expected values dynamically to handle this difference.
        """
        overrides = {}
        if LooseVersion(pa.__version__) < LooseVersion("21.0.0"):
            F16 = "float16"

            def f16_repr(values):
                arr = cls._make_float16_array(values)
                return cls.repr_value(arr, max_len=0)

            frac = f16_repr([0.1, 0.9, None])
            overrides.update(
                {
                    ("int16:max_min", F16): f16_repr([32767.0, -32768.0, None]),
                    ("float16:fractional", F16): frac,
                    ("float32:fractional", F16): frac,
                    ("float64:fractional", F16): frac,
                }
            )
        return overrides

    @classmethod
    def _version_overrides_unsafe(cls):
        """
        Build overrides for known PyArrow version-dependent behaviors (safe=False mode).

        PyArrow < 21: str(scalar) for float16 uses numpy's formatting
        (via np.float16). Same dynamic computation approach as safe=True mode.
        Additional overrides may be needed for different PyArrow versions
        as safe=False behavior varies across versions.
        """
        overrides = {}
        if LooseVersion(pa.__version__) < LooseVersion("21.0.0"):
            F16 = "float16"

            def f16_repr(values):
                arr = cls._make_float16_array(values)
                return cls.repr_value(arr, max_len=0)

            frac = f16_repr([0.1, 0.9, None])
            overrides.update(
                {
                    ("int16:max_min", F16): f16_repr([32767.0, -32768.0, None]),
                    ("float16:fractional", F16): frac,
                    ("float32:fractional", F16): frac,
                    ("float64:fractional", F16): frac,
                }
            )
        # Additional overrides will be discovered during cross-version testing
        return overrides

    # ----- test methods -----

    def test_scalar_cast_matrix(self):
        """Test all scalar-to-scalar type cast combinations with safe=True."""
        source_names, source_arrays = self._get_source_arrays()
        target_types = self._get_target_types()
        target_names = [self.repr_type(t) for t in target_types]
        target_lookup = dict(zip(target_names, target_types))

        self.compare_or_generate_golden_matrix(
            row_names=source_names,
            col_names=target_names,
            compute_cell=lambda src, tgt: self._try_cast(
                source_arrays[src], target_lookup[tgt], safe=True
            ),
            golden_file_prefix="golden_pyarrow_scalar_cast_safe",
            overrides=self._version_overrides_safe(),
        )

    def test_scalar_cast_matrix_unsafe(self):
        """Test all scalar-to-scalar type cast combinations with safe=False."""
        source_names, source_arrays = self._get_source_arrays()
        target_types = self._get_target_types()
        target_names = [self.repr_type(t) for t in target_types]
        target_lookup = dict(zip(target_names, target_types))

        self.compare_or_generate_golden_matrix(
            row_names=source_names,
            col_names=target_names,
            compute_cell=lambda src, tgt: self._try_cast(
                source_arrays[src], target_lookup[tgt], safe=False
            ),
            golden_file_prefix="golden_pyarrow_scalar_cast_unsafe",
            overrides=self._version_overrides_unsafe(),
        )


# ============================================================
# Nested Type Cast Tests
# ============================================================


@unittest.skipIf(
    not have_pyarrow
    or not have_pandas
    or not have_numpy
    or LooseVersion(np.__version__) < LooseVersion("2.0.0"),
    pyarrow_requirement_message or pandas_requirement_message or numpy_requirement_message,
)
class PyArrowNestedTypeCastTests(_PyArrowCastTestBase):
    """
    Tests nested/container type cast combinations via golden file comparison.

    Covers:
    - List variants: list, large_list, fixed_size_list
    - Map: map<key, value>
    - Struct: struct<fields...>
    - Container to scalar (should fail)
    """

    # ----- target types -----

    @staticmethod
    def _get_target_types():
        return [
            # List variants
            pa.list_(pa.int32()),
            pa.list_(pa.int64()),
            pa.list_(pa.string()),
            pa.large_list(pa.int32()),
            pa.large_list(pa.int64()),
            pa.list_(pa.int32(), 2),
            pa.list_(pa.int32(), 3),
            # Map
            pa.map_(pa.string(), pa.int32()),
            pa.map_(pa.string(), pa.int64()),
            # Struct variants: same names, type change, reorder, name mismatch
            pa.struct([("x", pa.int32()), ("y", pa.string())]),
            pa.struct([("x", pa.int64()), ("y", pa.string())]),
            pa.struct([("y", pa.string()), ("x", pa.int32())]),
            pa.struct([("a", pa.int32()), ("b", pa.string())]),
            # Scalar types (container -> scalar should fail)
            pa.string(),
            pa.int32(),
        ]

    # ----- source arrays -----

    def _get_source_arrays(self):
        """
        Create test arrays for nested/container types, split into edge-case
        categories.
        """
        list_int_type = pa.list_(pa.int32())
        struct_type = pa.struct([("x", pa.int32()), ("y", pa.string())])
        list_struct_type = pa.list_(struct_type)
        large_list_type = pa.large_list(pa.int32())
        fsl_type = pa.list_(pa.int32(), 2)
        map_type = pa.map_(pa.string(), pa.int32())

        rt = self.repr_type
        cases = [
            # --- list<int32> ---
            (f"{rt(list_int_type)}:standard", pa.array([[1, 2, 3], None], list_int_type)),
            (f"{rt(list_int_type)}:empty", pa.array([[], None], list_int_type)),
            (
                f"{rt(list_int_type)}:null_elem",
                pa.array([[None], [1, None, 3], None], list_int_type),
            ),
            # --- list<struct> ---
            (
                f"{rt(list_struct_type)}:standard",
                pa.array([[{"x": 1, "y": "a"}], None], list_struct_type),
            ),
            (
                f"{rt(list_struct_type)}:null_fields",
                pa.array([[{"x": None, "y": None}], [], None], list_struct_type),
            ),
            # --- large_list<int32> ---
            (f"{rt(large_list_type)}:standard", pa.array([[1, 2, 3], None], large_list_type)),
            (f"{rt(large_list_type)}:empty", pa.array([[], None], large_list_type)),
            (f"{rt(large_list_type)}:null_elem", pa.array([[None], None], large_list_type)),
            # --- fixed_size_list<int32, 2> ---
            (f"{rt(fsl_type)}:standard", pa.array([[1, 2], [3, 4], None], fsl_type)),
            (f"{rt(fsl_type)}:null_elem", pa.array([[None, None], None], fsl_type)),
            # --- map<string, int32> ---
            (f"{rt(map_type)}:standard", pa.array([[("a", 1), ("b", 2)], None], map_type)),
            (f"{rt(map_type)}:empty", pa.array([[], None], map_type)),
            # --- struct<x: int32, y: string> ---
            (f"{rt(struct_type)}:standard", pa.array([{"x": 1, "y": "a"}, None], struct_type)),
            (
                f"{rt(struct_type)}:null_fields",
                pa.array([{"x": None, "y": None}, None], struct_type),
            ),
        ]

        source_names = [name for name, _ in cases]
        source_arrays = dict(cases)
        return source_names, source_arrays

    # ----- version overrides -----

    @staticmethod
    def _version_overrides_safe():
        """
        Build overrides for known PyArrow version-dependent behaviors (safe=True mode).

        PyArrow < 21: struct field reordering during cast is not supported,
            raises ArrowTypeError instead.
        PyArrow < 19: struct field name mismatch during cast is not supported,
            raises ArrowTypeError instead.
        """
        overrides = {}
        struct_sources = [
            "struct<x: int32, y: string>:standard",
            "struct<x: int32, y: string>:null_fields",
        ]
        # PyArrow < 21: struct field reorder not supported
        if LooseVersion(pa.__version__) < LooseVersion("21.0.0"):
            for src in struct_sources:
                overrides[(src, "struct<y: string, x: int32>")] = "ERR@ArrowTypeError"
        # PyArrow < 19: struct field name mismatch not supported
        if LooseVersion(pa.__version__) < LooseVersion("19.0.0"):
            for src in struct_sources:
                overrides[(src, "struct<a: int32, b: string>")] = "ERR@ArrowTypeError"
        return overrides

    @staticmethod
    def _version_overrides_unsafe():
        """
        Build overrides for known PyArrow version-dependent behaviors (safe=False mode).

        Same version-dependent struct behaviors apply in unsafe mode.
        Additional overrides may be needed for different PyArrow versions
        as safe=False behavior varies across versions.
        """
        overrides = {}
        struct_sources = [
            "struct<x: int32, y: string>:standard",
            "struct<x: int32, y: string>:null_fields",
        ]
        # PyArrow < 21: struct field reorder not supported
        if LooseVersion(pa.__version__) < LooseVersion("21.0.0"):
            for src in struct_sources:
                overrides[(src, "struct<y: string, x: int32>")] = "ERR@ArrowTypeError"
        # PyArrow < 19: struct field name mismatch not supported
        if LooseVersion(pa.__version__) < LooseVersion("19.0.0"):
            for src in struct_sources:
                overrides[(src, "struct<a: int32, b: string>")] = "ERR@ArrowTypeError"
        # Additional overrides will be discovered during cross-version testing
        return overrides

    # ----- test methods -----

    def test_nested_cast_matrix(self):
        """Test all nested type cast combinations with safe=True."""
        source_names, source_arrays = self._get_source_arrays()
        target_types = self._get_target_types()
        target_names = [self.repr_type(t) for t in target_types]
        target_lookup = dict(zip(target_names, target_types))

        self.compare_or_generate_golden_matrix(
            row_names=source_names,
            col_names=target_names,
            compute_cell=lambda src, tgt: self._try_cast(
                source_arrays[src], target_lookup[tgt], safe=True
            ),
            golden_file_prefix="golden_pyarrow_nested_cast_safe",
            overrides=self._version_overrides_safe(),
        )

    def test_nested_cast_matrix_unsafe(self):
        """Test all nested type cast combinations with safe=False."""
        source_names, source_arrays = self._get_source_arrays()
        target_types = self._get_target_types()
        target_names = [self.repr_type(t) for t in target_types]
        target_lookup = dict(zip(target_names, target_types))

        self.compare_or_generate_golden_matrix(
            row_names=source_names,
            col_names=target_names,
            compute_cell=lambda src, tgt: self._try_cast(
                source_arrays[src], target_lookup[tgt], safe=False
            ),
            golden_file_prefix="golden_pyarrow_nested_cast_unsafe",
            overrides=self._version_overrides_unsafe(),
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
