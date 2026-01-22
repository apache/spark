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
Tests for PyArrow's pa.Array.cast() method with default parameters only.

## Numerical Type Conversion Matrix (pa.Array.cast with default safe=True)

### Covered Types:
- **Signed Integers**: int8, int16, int32, int64
- **Unsigned Integers**: uint8, uint16, uint32, uint64
- **Floats**: float16, float32, float64
"""

import platform
import unittest

from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message


def _get_float_to_int_boundary_expected(int_type):
    """
    Get the expected result for float-to-int boundary cast.

    Float-to-int casts at boundary values have platform-dependent behavior:
    - On macOS arm64: the cast succeeds with rounding to int max
    - On Linux x86_64: the cast fails with ArrowInvalid

    This is due to differences in floating-point precision and rounding
    behavior across CPU architectures and compilers.
    """
    import pyarrow as pa

    system = platform.system()
    machine = platform.machine()

    # Calculate int max based on type
    int_max_values = {
        pa.int32(): 2**31 - 1,  # 2147483647
        pa.int64(): 2**63 - 1,  # 9223372036854775807
    }
    int_max = int_max_values[int_type]

    # macOS arm64 rounds to int max; Linux x86_64 raises ArrowInvalid
    if system == "Darwin" and machine == "arm64":
        return pa.array([int_max], int_type)
    else:
        # Default to ArrowInvalid for Linux/x86_64 and other platforms
        return pa.lib.ArrowInvalid


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowNumericalCastTests(unittest.TestCase):
    """
    Tests for PyArrow numerical type casts with safe=True.

    Each test method contains a dict mapping target types to test cases.
    Each test case is a tuple: (source_array, expected_result_or_exception)
    """

    # Source types (all scalar types)
    SOURCE_TYPES = [
        # Numerical
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float32",
        "float64",
        # Non-numerical
        "bool",
        "string",
        "large_string",
        "binary",
        "large_binary",
        "fixed_size_binary_16",
        "decimal128",
        "decimal256",
        "date32",
        "date64",
        "timestamp_s",
        "timestamp_ms",
        "timestamp_us",
        "timestamp_ns",
        "timestamp_s_tz",
        "timestamp_ms_tz",
        "timestamp_us_tz",
        "timestamp_ns_tz",
        "timestamp_s_tz_ny",
        "timestamp_s_tz_shanghai",
        "duration_s",
        "duration_ms",
        "duration_us",
        "duration_ns",
        "time32_s",
        "time32_ms",
        "time64_us",
        "time64_ns",
    ]

    # Class-level storage for test results
    _cast_results = {}

    @classmethod
    def setUpClass(cls):
        """Initialize the cast results matrix."""
        cls._cast_results = {}

    @classmethod
    def tearDownClass(cls):
        """Print the cast matrix golden file after all tests complete."""
        if not cls._cast_results:
            return

        # Get target types from first source's results (all sources test same targets)
        target_types = list(cls._cast_results.get(cls.SOURCE_TYPES[0], {}).keys())
        if not target_types:
            return

        print("\n" + "=" * 80)
        print("PyArrow Cast Matrix (Y=lossless, L=lossy, N=not supported)")
        print("=" * 80)

        header = "src\\tgt," + ",".join(target_types)
        print(header)

        for src in cls.SOURCE_TYPES:
            row = (
                src
                + ","
                + ",".join(cls._cast_results.get(src, {}).get(tgt, "-") for tgt in target_types)
            )
            print(row)

        print("=" * 80)

    def _arrays_equal_nan_aware(self, arr1, arr2):
        """Compare two PyArrow arrays, treating NaN == NaN."""
        import math

        if len(arr1) != len(arr2):
            return False
        if arr1.type != arr2.type:
            return False

        for i in range(len(arr1)):
            v1 = arr1[i].as_py()
            v2 = arr2[i].as_py()

            if v1 is None and v2 is None:
                continue
            if v1 is None or v2 is None:
                return False
            if isinstance(v1, float) and isinstance(v2, float):
                if math.isnan(v1) and math.isnan(v2):
                    continue
            if v1 != v2:
                return False
        return True

    def _run_cast_tests(self, casts_dict, source_type_name):
        """Run cast tests for a single source type."""
        import pyarrow as pa

        # All scalar types (non-nested)
        ALL_SCALAR_TYPES = {
            # Integers
            "int8": pa.int8(),
            "int16": pa.int16(),
            "int32": pa.int32(),
            "int64": pa.int64(),
            "uint8": pa.uint8(),
            "uint16": pa.uint16(),
            "uint32": pa.uint32(),
            "uint64": pa.uint64(),
            # Floats
            "float16": pa.float16(),
            "float32": pa.float32(),
            "float64": pa.float64(),
            # Boolean
            "bool": pa.bool_(),
            # Strings
            "string": pa.string(),
            "large_string": pa.large_string(),
            # Binary
            "binary": pa.binary(),
            "large_binary": pa.large_binary(),
            "fixed_size_binary_16": pa.binary(16),
            # Decimal
            "decimal128": pa.decimal128(38, 10),
            "decimal256": pa.decimal256(76, 10),
            # Date
            "date32": pa.date32(),
            "date64": pa.date64(),
            # Timestamp (NTZ - no timezone)
            "timestamp_s": pa.timestamp("s"),
            "timestamp_ms": pa.timestamp("ms"),
            "timestamp_us": pa.timestamp("us"),
            "timestamp_ns": pa.timestamp("ns"),
            # Timestamp (with timezone - UTC)
            "timestamp_s_tz": pa.timestamp("s", tz="UTC"),
            "timestamp_ms_tz": pa.timestamp("ms", tz="UTC"),
            "timestamp_us_tz": pa.timestamp("us", tz="UTC"),
            "timestamp_ns_tz": pa.timestamp("ns", tz="UTC"),
            # Timestamp (with timezone - other timezones)
            "timestamp_s_tz_ny": pa.timestamp("s", tz="America/New_York"),
            "timestamp_s_tz_shanghai": pa.timestamp("s", tz="Asia/Shanghai"),
            # Duration
            "duration_s": pa.duration("s"),
            "duration_ms": pa.duration("ms"),
            "duration_us": pa.duration("us"),
            "duration_ns": pa.duration("ns"),
            # Time
            "time32_s": pa.time32("s"),
            "time32_ms": pa.time32("ms"),
            "time64_us": pa.time64("us"),
            "time64_ns": pa.time64("ns"),
        }

        type_map = ALL_SCALAR_TYPES

        failed_cases = []

        # Track cast results for golden file: Y=lossless, L=lossy, N=not supported
        # For each target type, track if there are success cases and/or failure cases
        target_results = {}  # tgt_type -> {"success": bool, "failure": bool}

        for tgt_type_name, test_pairs in casts_dict.items():
            tgt_pa_type = type_map.get(tgt_type_name)
            if tgt_pa_type is None:
                continue

            if tgt_type_name not in target_results:
                target_results[tgt_type_name] = {"success": False, "failure": False}

            for i, (src_arr, expected) in enumerate(test_pairs):
                case_id = f"{source_type_name}->{tgt_type_name}[{i}]"

                try:
                    if isinstance(expected, type) and issubclass(expected, Exception):
                        target_results[tgt_type_name]["failure"] = True
                        try:
                            result = src_arr.cast(tgt_pa_type)
                            failed_cases.append(
                                f"{case_id}: expected {expected.__name__}, got success: {result.to_pylist()}"
                            )
                        except expected:
                            pass
                        except Exception as e:
                            failed_cases.append(
                                f"{case_id}: expected {expected.__name__}, got {type(e).__name__}: {e}"
                            )
                    elif isinstance(expected, pa.Array):
                        target_results[tgt_type_name]["success"] = True
                        try:
                            result = src_arr.cast(tgt_pa_type)
                            if not self._arrays_equal_nan_aware(result, expected):
                                failed_cases.append(
                                    f"{case_id}: mismatch, expected {expected.to_pylist()}, got {result.to_pylist()}"
                                )
                        except Exception as e:
                            failed_cases.append(
                                f"{case_id}: expected success, got {type(e).__name__}: {e}"
                            )
                except Exception as e:
                    failed_cases.append(f"{case_id}: test error: {e}")

        # Record results in class-level storage for golden file
        if source_type_name not in self.__class__._cast_results:
            self.__class__._cast_results[source_type_name] = {}

        for tgt_type_name, result in target_results.items():
            if result["success"] and result["failure"]:
                self.__class__._cast_results[source_type_name][tgt_type_name] = "L"
            elif result["success"]:
                self.__class__._cast_results[source_type_name][tgt_type_name] = "Y"
            elif result["failure"]:
                self.__class__._cast_results[source_type_name][tgt_type_name] = "N"

        self.assertEqual(
            len(failed_cases),
            0,
            f"\n{source_type_name}: {len(failed_cases)} failed:\n" + "\n".join(failed_cases[:10]),
        )

    # ============================================================
    # int8 casts
    # ============================================================
    def test_int8_casts(self):
        """Test int8 -> all primitive types. Boundaries: -128 to 127"""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            # int8 -> integers
            "int8": [
                (pa.array([0, 1, -1, None], pa.int8()), pa.array([0, 1, -1, None], pa.int8())),
                (pa.array([127, -128], pa.int8()), pa.array([127, -128], pa.int8())),
                (pa.array([], pa.int8()), pa.array([], pa.int8())),
            ],
            "int16": [
                (pa.array([0, 1, -1, None], pa.int8()), pa.array([0, 1, -1, None], pa.int16())),
                (pa.array([127, -128], pa.int8()), pa.array([127, -128], pa.int16())),
                (pa.array([], pa.int8()), pa.array([], pa.int16())),
            ],
            "int32": [
                (pa.array([0, 1, -1, None], pa.int8()), pa.array([0, 1, -1, None], pa.int32())),
                (pa.array([127, -128], pa.int8()), pa.array([127, -128], pa.int32())),
                (pa.array([], pa.int8()), pa.array([], pa.int32())),
            ],
            "int64": [
                (pa.array([0, 1, -1, None], pa.int8()), pa.array([0, 1, -1, None], pa.int64())),
                (pa.array([127, -128], pa.int8()), pa.array([127, -128], pa.int64())),
                (pa.array([], pa.int8()), pa.array([], pa.int64())),
            ],
            "uint8": [
                (pa.array([0, 1, 127, None], pa.int8()), pa.array([0, 1, 127, None], pa.uint8())),
                (pa.array([-1], pa.int8()), pa.lib.ArrowInvalid),
                (pa.array([-128], pa.int8()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                (pa.array([0, 1, 127, None], pa.int8()), pa.array([0, 1, 127, None], pa.uint16())),
                (pa.array([-1], pa.int8()), pa.lib.ArrowInvalid),
                (pa.array([-128], pa.int8()), pa.lib.ArrowInvalid),
            ],
            "uint32": [
                (pa.array([0, 1, 127, None], pa.int8()), pa.array([0, 1, 127, None], pa.uint32())),
                (pa.array([-1], pa.int8()), pa.lib.ArrowInvalid),
                (pa.array([-128], pa.int8()), pa.lib.ArrowInvalid),
            ],
            "uint64": [
                (pa.array([0, 1, 127, None], pa.int8()), pa.array([0, 1, 127, None], pa.uint64())),
                (pa.array([-1], pa.int8()), pa.lib.ArrowInvalid),
                (pa.array([-128], pa.int8()), pa.lib.ArrowInvalid),
            ],
            # int8 -> floats
            "float16": [
                (
                    pa.array([0, 1, -1, None], pa.int8()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float16()),
                ),
                (pa.array([127, -128], pa.int8()), pa.array([127.0, -128.0], pa.float16())),
                (pa.array([], pa.int8()), pa.array([], pa.float16())),
            ],
            "float32": [
                (
                    pa.array([0, 1, -1, None], pa.int8()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float32()),
                ),
                (pa.array([127, -128], pa.int8()), pa.array([127.0, -128.0], pa.float32())),
                (pa.array([], pa.int8()), pa.array([], pa.float32())),
            ],
            "float64": [
                (
                    pa.array([0, 1, -1, None], pa.int8()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float64()),
                ),
                (pa.array([127, -128], pa.int8()), pa.array([127.0, -128.0], pa.float64())),
                (pa.array([], pa.int8()), pa.array([], pa.float64())),
            ],
            # int8 -> bool (0->False, non-zero->True)
            "bool": [
                (
                    pa.array([0, 1, -1, None], pa.int8()),
                    pa.array([False, True, True, None], pa.bool_()),
                ),
                (pa.array([127, -128], pa.int8()), pa.array([True, True], pa.bool_())),
                (pa.array([], pa.int8()), pa.array([], pa.bool_())),
            ],
            # int8 -> string
            "string": [
                (
                    pa.array([0, 1, -1, None], pa.int8()),
                    pa.array(["0", "1", "-1", None], pa.string()),
                ),
                (pa.array([127, -128], pa.int8()), pa.array(["127", "-128"], pa.string())),
                (pa.array([], pa.int8()), pa.array([], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([0, 1, -1, None], pa.int8()),
                    pa.array(["0", "1", "-1", None], pa.large_string()),
                ),
                (pa.array([127, -128], pa.int8()), pa.array(["127", "-128"], pa.large_string())),
                (pa.array([], pa.int8()), pa.array([], pa.large_string())),
            ],
            # int8 -> binary (not supported)
            "binary": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            # int8 -> decimal
            "decimal128": [
                (
                    pa.array([0, 1, -1, None], pa.int8()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal128(38, 10)
                    ),
                ),
                (
                    pa.array([127, -128], pa.int8()),
                    pa.array([Decimal("127"), Decimal("-128")], pa.decimal128(38, 10)),
                ),
                (pa.array([], pa.int8()), pa.array([], pa.decimal128(38, 10))),
            ],
            "decimal256": [
                (
                    pa.array([0, 1, -1, None], pa.int8()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal256(76, 10)
                    ),
                ),
                (
                    pa.array([127, -128], pa.int8()),
                    pa.array([Decimal("127"), Decimal("-128")], pa.decimal256(76, 10)),
                ),
                (pa.array([], pa.int8()), pa.array([], pa.decimal256(76, 10))),
            ],
            "fixed_size_binary_16": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            # int8 -> date (not supported)
            "date32": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            # int8 -> timestamp (not supported)
            "timestamp_s": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)
            ],
            # int8 -> duration (not supported)
            "duration_s": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            # int8 -> time (not supported)
            "time32_s": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "int8")

    # ============================================================
    # int16 casts
    # ============================================================
    def test_int16_casts(self):
        """Test int16 -> all primitive types. Boundaries: -32768 to 32767"""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            # int16 -> integers
            "int8": [
                (
                    pa.array([0, 1, -1, 127, -128, None], pa.int16()),
                    pa.array([0, 1, -1, 127, -128, None], pa.int8()),
                ),
                (pa.array([128], pa.int16()), pa.lib.ArrowInvalid),
                (pa.array([-129], pa.int16()), pa.lib.ArrowInvalid),
            ],
            "int16": [
                (pa.array([0, 1, -1, None], pa.int16()), pa.array([0, 1, -1, None], pa.int16())),
                (pa.array([32767, -32768], pa.int16()), pa.array([32767, -32768], pa.int16())),
                (pa.array([], pa.int16()), pa.array([], pa.int16())),
            ],
            "int32": [
                (pa.array([0, 1, -1, None], pa.int16()), pa.array([0, 1, -1, None], pa.int32())),
                (pa.array([32767, -32768], pa.int16()), pa.array([32767, -32768], pa.int32())),
                (pa.array([], pa.int16()), pa.array([], pa.int32())),
            ],
            "int64": [
                (pa.array([0, 1, -1, None], pa.int16()), pa.array([0, 1, -1, None], pa.int64())),
                (pa.array([32767, -32768], pa.int16()), pa.array([32767, -32768], pa.int64())),
                (pa.array([], pa.int16()), pa.array([], pa.int64())),
            ],
            "uint8": [
                (pa.array([0, 1, 255, None], pa.int16()), pa.array([0, 1, 255, None], pa.uint8())),
                (pa.array([256], pa.int16()), pa.lib.ArrowInvalid),
                (pa.array([-1], pa.int16()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                (
                    pa.array([0, 1, 32767, None], pa.int16()),
                    pa.array([0, 1, 32767, None], pa.uint16()),
                ),
                (pa.array([-1], pa.int16()), pa.lib.ArrowInvalid),
                (pa.array([-32768], pa.int16()), pa.lib.ArrowInvalid),
            ],
            "uint32": [
                (
                    pa.array([0, 1, 32767, None], pa.int16()),
                    pa.array([0, 1, 32767, None], pa.uint32()),
                ),
                (pa.array([-1], pa.int16()), pa.lib.ArrowInvalid),
                (pa.array([-32768], pa.int16()), pa.lib.ArrowInvalid),
            ],
            "uint64": [
                (
                    pa.array([0, 1, 32767, None], pa.int16()),
                    pa.array([0, 1, 32767, None], pa.uint64()),
                ),
                (pa.array([-1], pa.int16()), pa.lib.ArrowInvalid),
                (pa.array([-32768], pa.int16()), pa.lib.ArrowInvalid),
            ],
            # int16 -> floats
            "float16": [
                (
                    pa.array([0, 1, -1, None], pa.int16()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float16()),
                ),
                (pa.array([2048], pa.int16()), pa.array([2048.0], pa.float16())),
                (pa.array([32767], pa.int16()), pa.array([32768.0], pa.float16())),  # rounds
            ],
            "float32": [
                (
                    pa.array([0, 1, -1, None], pa.int16()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float32()),
                ),
                (
                    pa.array([32767, -32768], pa.int16()),
                    pa.array([32767.0, -32768.0], pa.float32()),
                ),
                (pa.array([], pa.int16()), pa.array([], pa.float32())),
            ],
            "float64": [
                (
                    pa.array([0, 1, -1, None], pa.int16()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float64()),
                ),
                (
                    pa.array([32767, -32768], pa.int16()),
                    pa.array([32767.0, -32768.0], pa.float64()),
                ),
                (pa.array([], pa.int16()), pa.array([], pa.float64())),
            ],
            # int16 -> bool
            "bool": [
                (
                    pa.array([0, 1, -1, None], pa.int16()),
                    pa.array([False, True, True, None], pa.bool_()),
                ),
                (pa.array([32767, -32768], pa.int16()), pa.array([True, True], pa.bool_())),
                (pa.array([], pa.int16()), pa.array([], pa.bool_())),
            ],
            # int16 -> string
            "string": [
                (
                    pa.array([0, 1, -1, None], pa.int16()),
                    pa.array(["0", "1", "-1", None], pa.string()),
                ),
                (pa.array([32767, -32768], pa.int16()), pa.array(["32767", "-32768"], pa.string())),
                (pa.array([], pa.int16()), pa.array([], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([0, 1, -1, None], pa.int16()),
                    pa.array(["0", "1", "-1", None], pa.large_string()),
                ),
                (
                    pa.array([32767, -32768], pa.int16()),
                    pa.array(["32767", "-32768"], pa.large_string()),
                ),
                (pa.array([], pa.int16()), pa.array([], pa.large_string())),
            ],
            # int16 -> binary (not supported)
            "binary": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            # int16 -> decimal
            "decimal128": [
                (
                    pa.array([0, 1, -1, None], pa.int16()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal128(38, 10)
                    ),
                ),
                (
                    pa.array([32767, -32768], pa.int16()),
                    pa.array([Decimal("32767"), Decimal("-32768")], pa.decimal128(38, 10)),
                ),
                (pa.array([], pa.int16()), pa.array([], pa.decimal128(38, 10))),
            ],
            "decimal256": [
                (
                    pa.array([0, 1, -1, None], pa.int16()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal256(76, 10)
                    ),
                ),
                (
                    pa.array([32767, -32768], pa.int16()),
                    pa.array([Decimal("32767"), Decimal("-32768")], pa.decimal256(76, 10)),
                ),
                (pa.array([], pa.int16()), pa.array([], pa.decimal256(76, 10))),
            ],
            "fixed_size_binary_16": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            # int16 -> date/timestamp/duration/time (not supported)
            "date32": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([0], pa.int16()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "int16")

    # ============================================================
    # int32 casts
    # ============================================================
    def test_int32_casts(self):
        """Test int32 -> all scalar types. Boundaries: -2147483648 to 2147483647"""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            "int8": [
                (
                    pa.array([0, 1, -1, 127, -128, None], pa.int32()),
                    pa.array([0, 1, -1, 127, -128, None], pa.int8()),
                ),
                (pa.array([128], pa.int32()), pa.lib.ArrowInvalid),
                (pa.array([2147483647], pa.int32()), pa.lib.ArrowInvalid),
            ],
            "int16": [
                (
                    pa.array([0, 1, -1, 32767, -32768, None], pa.int32()),
                    pa.array([0, 1, -1, 32767, -32768, None], pa.int16()),
                ),
                (pa.array([32768], pa.int32()), pa.lib.ArrowInvalid),
                (pa.array([-32769], pa.int32()), pa.lib.ArrowInvalid),
            ],
            "int32": [
                (pa.array([0, 1, -1, None], pa.int32()), pa.array([0, 1, -1, None], pa.int32())),
                (
                    pa.array([2147483647, -2147483648], pa.int32()),
                    pa.array([2147483647, -2147483648], pa.int32()),
                ),
                (pa.array([], pa.int32()), pa.array([], pa.int32())),
            ],
            "int64": [
                (pa.array([0, 1, -1, None], pa.int32()), pa.array([0, 1, -1, None], pa.int64())),
                (
                    pa.array([2147483647, -2147483648], pa.int32()),
                    pa.array([2147483647, -2147483648], pa.int64()),
                ),
                (pa.array([None, None], pa.int32()), pa.array([None, None], pa.int64())),
            ],
            "uint8": [
                (pa.array([0, 1, 255, None], pa.int32()), pa.array([0, 1, 255, None], pa.uint8())),
                (pa.array([256], pa.int32()), pa.lib.ArrowInvalid),
                (pa.array([-1], pa.int32()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                (
                    pa.array([0, 1, 65535, None], pa.int32()),
                    pa.array([0, 1, 65535, None], pa.uint16()),
                ),
                (pa.array([65536], pa.int32()), pa.lib.ArrowInvalid),
                (pa.array([-1], pa.int32()), pa.lib.ArrowInvalid),
            ],
            "uint32": [
                (
                    pa.array([0, 1, 2147483647, None], pa.int32()),
                    pa.array([0, 1, 2147483647, None], pa.uint32()),
                ),
                (pa.array([-1], pa.int32()), pa.lib.ArrowInvalid),
                (pa.array([-2147483648], pa.int32()), pa.lib.ArrowInvalid),
            ],
            "uint64": [
                (
                    pa.array([0, 1, 2147483647, None], pa.int32()),
                    pa.array([0, 1, 2147483647, None], pa.uint64()),
                ),
                (pa.array([-1], pa.int32()), pa.lib.ArrowInvalid),
                (pa.array([-2147483648], pa.int32()), pa.lib.ArrowInvalid),
            ],
            "float16": [
                (
                    pa.array([0, 1, -1, None], pa.int32()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float16()),
                ),
                (pa.array([2048], pa.int32()), pa.array([2048.0], pa.float16())),
                (pa.array([2147483647], pa.int32()), pa.lib.ArrowInvalid),
            ],
            "float32": [
                (
                    pa.array([0, 1, -1, None], pa.int32()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float32()),
                ),
                (pa.array([16777216], pa.int32()), pa.array([16777216.0], pa.float32())),
                (pa.array([2147483647], pa.int32()), pa.lib.ArrowInvalid),
            ],
            "float64": [
                (
                    pa.array([0, 1, -1, None], pa.int32()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float64()),
                ),
                (
                    pa.array([2147483647, -2147483648], pa.int32()),
                    pa.array([2147483647.0, -2147483648.0], pa.float64()),
                ),
                (pa.array([None, None], pa.int32()), pa.array([None, None], pa.float64())),
            ],
            # === Non-numerical types ===
            "bool": [
                (
                    pa.array([0, 1, -1, None], pa.int32()),
                    pa.array([False, True, True, None], pa.bool_()),
                ),
                (
                    pa.array([2147483647, -2147483648], pa.int32()),
                    pa.array([True, True], pa.bool_()),
                ),
                (pa.array([], pa.int32()), pa.array([], pa.bool_())),
            ],
            "string": [
                (
                    pa.array([0, 1, -1, None], pa.int32()),
                    pa.array(["0", "1", "-1", None], pa.string()),
                ),
                (
                    pa.array([2147483647, -2147483648], pa.int32()),
                    pa.array(["2147483647", "-2147483648"], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([0, 1, -1, None], pa.int32()),
                    pa.array(["0", "1", "-1", None], pa.large_string()),
                ),
            ],
            "binary": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "decimal128": [
                (
                    pa.array([0, 1, -1, None], pa.int32()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal128(38, 10)
                    ),
                ),
                (
                    pa.array([2147483647, -2147483648], pa.int32()),
                    pa.array(
                        [Decimal("2147483647"), Decimal("-2147483648")], pa.decimal128(38, 10)
                    ),
                ),
            ],
            "decimal256": [
                (
                    pa.array([0, 1, -1, None], pa.int32()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal256(76, 10)
                    ),
                ),
                (
                    pa.array([2147483647], pa.int32()),
                    pa.array([Decimal("2147483647")], pa.decimal256(76, 10)),
                ),
            ],
            # int32 -> date32/time32 supported (same underlying storage)
            "date32": [
                # date32: days since 1970-01-01. Valid range: ~-2M to ~2M years
                (
                    pa.array([0, 1, 19000, None], pa.int32()),
                    pa.array([0, 1, 19000, None], pa.date32()),
                ),  # 19000 = ~2022
                (
                    pa.array([-1, -365], pa.int32()),
                    pa.array([-1, -365], pa.date32()),
                ),  # negative = before 1970
            ],
            "date64": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [
                # time32[s]: seconds since midnight
                (
                    pa.array([0, 3600, 86399, None], pa.int32()),
                    pa.array([0, 3600, 86399, None], pa.time32("s")),
                ),
                # overflow wraps around (not error): 86400 -> 00:00:00, -1 -> 23:59:59
                (pa.array([86400], pa.int32()), pa.array([0], pa.time32("s"))),
                (pa.array([-1], pa.int32()), pa.array([86399], pa.time32("s"))),
            ],
            "time32_ms": [
                # time32[ms]: milliseconds since midnight
                (
                    pa.array([0, 1000, 86399999, None], pa.int32()),
                    pa.array([0, 1000, 86399999, None], pa.time32("ms")),
                ),
                # overflow wraps around
                (pa.array([86400000], pa.int32()), pa.array([0], pa.time32("ms"))),
                (pa.array([-1], pa.int32()), pa.array([86399999], pa.time32("ms"))),
            ],
            "time64_us": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([0], pa.int32()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "int32")

    # ============================================================
    # int64 casts
    # ============================================================
    def test_int64_casts(self):
        """Test int64 -> all scalar types. Boundaries: -9223372036854775808 to 9223372036854775807"""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            "int8": [
                (
                    pa.array([0, 1, -1, 127, -128, None], pa.int64()),
                    pa.array([0, 1, -1, 127, -128, None], pa.int8()),
                ),
                (pa.array([128], pa.int64()), pa.lib.ArrowInvalid),
                (pa.array([9223372036854775807], pa.int64()), pa.lib.ArrowInvalid),
            ],
            "int16": [
                (
                    pa.array([0, 1, -1, 32767, -32768, None], pa.int64()),
                    pa.array([0, 1, -1, 32767, -32768, None], pa.int16()),
                ),
                (pa.array([32768], pa.int64()), pa.lib.ArrowInvalid),
                (pa.array([-32769], pa.int64()), pa.lib.ArrowInvalid),
            ],
            "int32": [
                (
                    pa.array([0, 1, -1, 2147483647, -2147483648, None], pa.int64()),
                    pa.array([0, 1, -1, 2147483647, -2147483648, None], pa.int32()),
                ),
                (pa.array([2147483648], pa.int64()), pa.lib.ArrowInvalid),
                (pa.array([-2147483649], pa.int64()), pa.lib.ArrowInvalid),
            ],
            "int64": [
                (pa.array([0, 1, -1, None], pa.int64()), pa.array([0, 1, -1, None], pa.int64())),
                (
                    pa.array([9223372036854775807, -9223372036854775808], pa.int64()),
                    pa.array([9223372036854775807, -9223372036854775808], pa.int64()),
                ),
                (pa.array([], pa.int64()), pa.array([], pa.int64())),
            ],
            "uint8": [
                (pa.array([0, 1, 255, None], pa.int64()), pa.array([0, 1, 255, None], pa.uint8())),
                (pa.array([256], pa.int64()), pa.lib.ArrowInvalid),
                (pa.array([-1], pa.int64()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                (
                    pa.array([0, 1, 65535, None], pa.int64()),
                    pa.array([0, 1, 65535, None], pa.uint16()),
                ),
                (pa.array([65536], pa.int64()), pa.lib.ArrowInvalid),
                (pa.array([-1], pa.int64()), pa.lib.ArrowInvalid),
            ],
            "uint32": [
                (
                    pa.array([0, 1, 4294967295, None], pa.int64()),
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                ),
                (pa.array([4294967296], pa.int64()), pa.lib.ArrowInvalid),
                (pa.array([-1], pa.int64()), pa.lib.ArrowInvalid),
            ],
            "uint64": [
                (
                    pa.array([0, 1, 9223372036854775807, None], pa.int64()),
                    pa.array([0, 1, 9223372036854775807, None], pa.uint64()),
                ),
                (pa.array([-1], pa.int64()), pa.lib.ArrowInvalid),
                (pa.array([-9223372036854775808], pa.int64()), pa.lib.ArrowInvalid),
            ],
            "float16": [
                (
                    pa.array([0, 1, -1, None], pa.int64()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float16()),
                ),
                (pa.array([2048], pa.int64()), pa.array([2048.0], pa.float16())),
                (pa.array([9223372036854775807], pa.int64()), pa.lib.ArrowInvalid),
            ],
            "float32": [
                (
                    pa.array([0, 1, -1, None], pa.int64()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float32()),
                ),
                (pa.array([16777216], pa.int64()), pa.array([16777216.0], pa.float32())),
                (pa.array([9223372036854775807], pa.int64()), pa.lib.ArrowInvalid),
            ],
            "float64": [
                (
                    pa.array([0, 1, -1, None], pa.int64()),
                    pa.array([0.0, 1.0, -1.0, None], pa.float64()),
                ),
                (
                    pa.array([9007199254740992], pa.int64()),
                    pa.array([9007199254740992.0], pa.float64()),
                ),
                (pa.array([9007199254740993], pa.int64()), pa.lib.ArrowInvalid),
            ],
            # === Non-numerical types ===
            "bool": [
                (
                    pa.array([0, 1, -1, None], pa.int64()),
                    pa.array([False, True, True, None], pa.bool_()),
                ),
                (pa.array([9223372036854775807], pa.int64()), pa.array([True], pa.bool_())),
                (pa.array([], pa.int64()), pa.array([], pa.bool_())),
            ],
            "string": [
                (
                    pa.array([0, 1, -1, None], pa.int64()),
                    pa.array(["0", "1", "-1", None], pa.string()),
                ),
                (
                    pa.array([9223372036854775807, -9223372036854775808], pa.int64()),
                    pa.array(["9223372036854775807", "-9223372036854775808"], pa.string()),
                ),
                (pa.array([], pa.int64()), pa.array([], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([0, 1, -1, None], pa.int64()),
                    pa.array(["0", "1", "-1", None], pa.large_string()),
                ),
                (
                    pa.array([9223372036854775807], pa.int64()),
                    pa.array(["9223372036854775807"], pa.large_string()),
                ),
            ],
            "binary": [(pa.array([0], pa.int64()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0], pa.int64()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [(pa.array([0], pa.int64()), pa.lib.ArrowNotImplementedError)],
            "decimal128": [
                (
                    pa.array([0, 1, -1, None], pa.int64()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal128(38, 10)
                    ),
                ),
                (
                    pa.array([9223372036854775807], pa.int64()),
                    pa.array([Decimal("9223372036854775807")], pa.decimal128(38, 10)),
                ),
            ],
            "decimal256": [
                (
                    pa.array([0, 1, -1, None], pa.int64()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal256(76, 10)
                    ),
                ),
                (
                    pa.array([-9223372036854775808], pa.int64()),
                    pa.array([Decimal("-9223372036854775808")], pa.decimal256(76, 10)),
                ),
            ],
            "date32": [(pa.array([0], pa.int64()), pa.lib.ArrowNotImplementedError)],
            # int64 -> date64/timestamp/duration/time64 supported (same underlying storage)
            "date64": [
                # date64: milliseconds since 1970-01-01
                (
                    pa.array([0, 86400000, None], pa.int64()),
                    pa.array([0, 86400000, None], pa.date64()),
                ),  # day 0, day 1
                (
                    pa.array([1640995200000], pa.int64()),
                    pa.array([1640995200000], pa.date64()),
                ),  # 2022-01-01
                (
                    pa.array([-86400000], pa.int64()),
                    pa.array([-86400000], pa.date64()),
                ),  # 1969-12-31
            ],
            "timestamp_s": [
                # timestamp[s]: seconds since epoch
                (
                    pa.array([0, 1, -1, None], pa.int64()),
                    pa.array([0, 1, -1, None], pa.timestamp("s")),
                ),
                (
                    pa.array([1640995200], pa.int64()),
                    pa.array([1640995200], pa.timestamp("s")),
                ),  # 2022-01-01
            ],
            "timestamp_ms": [
                (
                    pa.array([0, 1000, -1000, None], pa.int64()),
                    pa.array([0, 1000, -1000, None], pa.timestamp("ms")),
                ),
                (
                    pa.array([1640995200000], pa.int64()),
                    pa.array([1640995200000], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array([0, 1000000, None], pa.int64()),
                    pa.array([0, 1000000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([0, 1000000000, None], pa.int64()),
                    pa.array([0, 1000000000, None], pa.timestamp("ns")),
                ),
            ],
            # int64 -> timestamp with timezone (target type uses UTC from ALL_SCALAR_TYPES)
            # Note: Different timezones would require separate type definitions
            "timestamp_s_tz": [
                (
                    pa.array([0, 1, -1, None], pa.int64()),
                    pa.array([0, 1, -1, None], pa.timestamp("s", tz="UTC")),
                ),
                (
                    pa.array([1640995200], pa.int64()),
                    pa.array([1640995200], pa.timestamp("s", tz="UTC")),
                ),  # 2022-01-01
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([0, 1000, None], pa.int64()),
                    pa.array([0, 1000, None], pa.timestamp("ms", tz="UTC")),
                ),
                (
                    pa.array([1640995200000], pa.int64()),
                    pa.array([1640995200000], pa.timestamp("ms", tz="UTC")),
                ),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([0, 1000000, None], pa.int64()),
                    pa.array([0, 1000000, None], pa.timestamp("us", tz="UTC")),
                ),
                (
                    pa.array([1640995200000000], pa.int64()),
                    pa.array([1640995200000000], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([0, 1000000000, None], pa.int64()),
                    pa.array([0, 1000000000, None], pa.timestamp("ns", tz="UTC")),
                ),
                (
                    pa.array([1640995200000000000], pa.int64()),
                    pa.array([1640995200000000000], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            # Different timezones
            "timestamp_s_tz_ny": [
                (
                    pa.array([0, 1640995200, None], pa.int64()),
                    pa.array([0, 1640995200, None], pa.timestamp("s", tz="America/New_York")),
                ),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([0, 1640995200, None], pa.int64()),
                    pa.array([0, 1640995200, None], pa.timestamp("s", tz="Asia/Shanghai")),
                ),
            ],
            "duration_s": [
                (
                    pa.array([0, 3600, 86400, None], pa.int64()),
                    pa.array([0, 3600, 86400, None], pa.duration("s")),
                ),
                (pa.array([-1], pa.int64()), pa.array([-1], pa.duration("s"))),  # negative duration
            ],
            "duration_ms": [
                (
                    pa.array([0, 1000, None], pa.int64()),
                    pa.array([0, 1000, None], pa.duration("ms")),
                ),
            ],
            "duration_us": [
                (
                    pa.array([0, 1000000, None], pa.int64()),
                    pa.array([0, 1000000, None], pa.duration("us")),
                ),
            ],
            "duration_ns": [
                (
                    pa.array([0, 1000000000, None], pa.int64()),
                    pa.array([0, 1000000000, None], pa.duration("ns")),
                ),
            ],
            "time32_s": [(pa.array([0], pa.int64()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([0], pa.int64()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [
                # time64[us]: microseconds since midnight
                (
                    pa.array([0, 1000000, 86399999999, None], pa.int64()),
                    pa.array([0, 1000000, 86399999999, None], pa.time64("us")),
                ),
                # overflow wraps around
                (pa.array([86400000000], pa.int64()), pa.array([0], pa.time64("us"))),
                (pa.array([-1], pa.int64()), pa.array([86399999999], pa.time64("us"))),
            ],
            "time64_ns": [
                # time64[ns]: nanoseconds since midnight
                (
                    pa.array([0, 1000000000, None], pa.int64()),
                    pa.array([0, 1000000000, None], pa.time64("ns")),
                ),
                # overflow wraps around
                (pa.array([86400000000000], pa.int64()), pa.array([0], pa.time64("ns"))),
            ],
        }
        self._run_cast_tests(casts, "int64")

    # ============================================================
    # uint8 casts
    # ============================================================
    def test_uint8_casts(self):
        """Test uint8 -> all scalar types. Boundaries: 0 to 255"""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            "int8": [
                (pa.array([0, 1, 127, None], pa.uint8()), pa.array([0, 1, 127, None], pa.int8())),
                (pa.array([128], pa.uint8()), pa.lib.ArrowInvalid),
                (pa.array([255], pa.uint8()), pa.lib.ArrowInvalid),
            ],
            "int16": [
                (pa.array([0, 1, 255, None], pa.uint8()), pa.array([0, 1, 255, None], pa.int16())),
                (pa.array([128], pa.uint8()), pa.array([128], pa.int16())),
                (pa.array([None, None], pa.uint8()), pa.array([None, None], pa.int16())),
            ],
            "int32": [
                (pa.array([0, 1, 255, None], pa.uint8()), pa.array([0, 1, 255, None], pa.int32())),
                (pa.array([128], pa.uint8()), pa.array([128], pa.int32())),
                (pa.array([None, None], pa.uint8()), pa.array([None, None], pa.int32())),
            ],
            "int64": [
                (pa.array([0, 1, 255, None], pa.uint8()), pa.array([0, 1, 255, None], pa.int64())),
                (pa.array([128], pa.uint8()), pa.array([128], pa.int64())),
                (pa.array([None, None], pa.uint8()), pa.array([None, None], pa.int64())),
            ],
            "uint8": [
                (pa.array([0, 1, 255, None], pa.uint8()), pa.array([0, 1, 255, None], pa.uint8())),
                (pa.array([128], pa.uint8()), pa.array([128], pa.uint8())),
                (pa.array([], pa.uint8()), pa.array([], pa.uint8())),
            ],
            "uint16": [
                (pa.array([0, 1, 255, None], pa.uint8()), pa.array([0, 1, 255, None], pa.uint16())),
                (pa.array([128], pa.uint8()), pa.array([128], pa.uint16())),
                (pa.array([None, None], pa.uint8()), pa.array([None, None], pa.uint16())),
            ],
            "uint32": [
                (pa.array([0, 1, 255, None], pa.uint8()), pa.array([0, 1, 255, None], pa.uint32())),
                (pa.array([128], pa.uint8()), pa.array([128], pa.uint32())),
                (pa.array([None, None], pa.uint8()), pa.array([None, None], pa.uint32())),
            ],
            "uint64": [
                (pa.array([0, 1, 255, None], pa.uint8()), pa.array([0, 1, 255, None], pa.uint64())),
                (pa.array([128], pa.uint8()), pa.array([128], pa.uint64())),
                (pa.array([None, None], pa.uint8()), pa.array([None, None], pa.uint64())),
            ],
            "float16": [
                (
                    pa.array([0, 1, 255, None], pa.uint8()),
                    pa.array([0.0, 1.0, 255.0, None], pa.float16()),
                ),
                (pa.array([128], pa.uint8()), pa.array([128.0], pa.float16())),
                (pa.array([None, None], pa.uint8()), pa.array([None, None], pa.float16())),
            ],
            "float32": [
                (
                    pa.array([0, 1, 255, None], pa.uint8()),
                    pa.array([0.0, 1.0, 255.0, None], pa.float32()),
                ),
                (pa.array([128], pa.uint8()), pa.array([128.0], pa.float32())),
                (pa.array([None, None], pa.uint8()), pa.array([None, None], pa.float32())),
            ],
            "float64": [
                (
                    pa.array([0, 1, 255, None], pa.uint8()),
                    pa.array([0.0, 1.0, 255.0, None], pa.float64()),
                ),
                (pa.array([128], pa.uint8()), pa.array([128.0], pa.float64())),
                (pa.array([None, None], pa.uint8()), pa.array([None, None], pa.float64())),
            ],
            # === Non-numerical types ===
            "bool": [
                (
                    pa.array([0, 1, 255, None], pa.uint8()),
                    pa.array([False, True, True, None], pa.bool_()),
                ),
                (pa.array([128], pa.uint8()), pa.array([True], pa.bool_())),
                (pa.array([], pa.uint8()), pa.array([], pa.bool_())),
            ],
            "string": [
                (
                    pa.array([0, 1, 255, None], pa.uint8()),
                    pa.array(["0", "1", "255", None], pa.string()),
                ),
                (pa.array([128], pa.uint8()), pa.array(["128"], pa.string())),
                (pa.array([], pa.uint8()), pa.array([], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([0, 1, 255, None], pa.uint8()),
                    pa.array(["0", "1", "255", None], pa.large_string()),
                ),
                (pa.array([128], pa.uint8()), pa.array(["128"], pa.large_string())),
            ],
            "binary": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "decimal128": [
                (
                    pa.array([0, 1, 255, None], pa.uint8()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("255"), None], pa.decimal128(38, 10)
                    ),
                ),
                (pa.array([128], pa.uint8()), pa.array([Decimal("128")], pa.decimal128(38, 10))),
            ],
            "decimal256": [
                (
                    pa.array([0, 1, 255, None], pa.uint8()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("255"), None], pa.decimal256(76, 10)
                    ),
                ),
                (pa.array([128], pa.uint8()), pa.array([Decimal("128")], pa.decimal256(76, 10))),
            ],
            "date32": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([0], pa.uint8()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "uint8")

    # ============================================================
    # uint16 casts
    # ============================================================
    def test_uint16_casts(self):
        """Test uint16 -> all scalar types. Boundaries: 0 to 65535"""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            "int8": [
                (pa.array([0, 1, 127, None], pa.uint16()), pa.array([0, 1, 127, None], pa.int8())),
                (pa.array([128], pa.uint16()), pa.lib.ArrowInvalid),
                (pa.array([65535], pa.uint16()), pa.lib.ArrowInvalid),
            ],
            "int16": [
                (
                    pa.array([0, 1, 32767, None], pa.uint16()),
                    pa.array([0, 1, 32767, None], pa.int16()),
                ),
                (pa.array([32768], pa.uint16()), pa.lib.ArrowInvalid),
                (pa.array([65535], pa.uint16()), pa.lib.ArrowInvalid),
            ],
            "int32": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array([0, 1, 65535, None], pa.int32()),
                ),
                (pa.array([32768], pa.uint16()), pa.array([32768], pa.int32())),
                (pa.array([None, None], pa.uint16()), pa.array([None, None], pa.int32())),
            ],
            "int64": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array([0, 1, 65535, None], pa.int64()),
                ),
                (pa.array([32768], pa.uint16()), pa.array([32768], pa.int64())),
                (pa.array([None, None], pa.uint16()), pa.array([None, None], pa.int64())),
            ],
            "uint8": [
                (pa.array([0, 1, 255, None], pa.uint16()), pa.array([0, 1, 255, None], pa.uint8())),
                (pa.array([256], pa.uint16()), pa.lib.ArrowInvalid),
                (pa.array([65535], pa.uint16()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array([0, 1, 65535, None], pa.uint16()),
                ),
                (pa.array([32768], pa.uint16()), pa.array([32768], pa.uint16())),
                (pa.array([], pa.uint16()), pa.array([], pa.uint16())),
            ],
            "uint32": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array([0, 1, 65535, None], pa.uint32()),
                ),
                (pa.array([32768], pa.uint16()), pa.array([32768], pa.uint32())),
                (pa.array([None, None], pa.uint16()), pa.array([None, None], pa.uint32())),
            ],
            "uint64": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array([0, 1, 65535, None], pa.uint64()),
                ),
                (pa.array([32768], pa.uint16()), pa.array([32768], pa.uint64())),
                (pa.array([None, None], pa.uint16()), pa.array([None, None], pa.uint64())),
            ],
            "float16": [
                (pa.array([0, 1, None], pa.uint16()), pa.array([0.0, 1.0, None], pa.float16())),
                (pa.array([2048], pa.uint16()), pa.array([2048.0], pa.float16())),
                (pa.array([65535], pa.uint16()), pa.array([float("inf")], pa.float16())),
            ],
            "float32": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array([0.0, 1.0, 65535.0, None], pa.float32()),
                ),
                (pa.array([32768], pa.uint16()), pa.array([32768.0], pa.float32())),
                (pa.array([None, None], pa.uint16()), pa.array([None, None], pa.float32())),
            ],
            "float64": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array([0.0, 1.0, 65535.0, None], pa.float64()),
                ),
                (pa.array([32768], pa.uint16()), pa.array([32768.0], pa.float64())),
                (pa.array([None, None], pa.uint16()), pa.array([None, None], pa.float64())),
            ],
            # === Non-numerical types ===
            "bool": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array([False, True, True, None], pa.bool_()),
                ),
                (pa.array([32768], pa.uint16()), pa.array([True], pa.bool_())),
                (pa.array([], pa.uint16()), pa.array([], pa.bool_())),
            ],
            "string": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array(["0", "1", "65535", None], pa.string()),
                ),
                (pa.array([32768], pa.uint16()), pa.array(["32768"], pa.string())),
                (pa.array([], pa.uint16()), pa.array([], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array(["0", "1", "65535", None], pa.large_string()),
                ),
                (pa.array([32768], pa.uint16()), pa.array(["32768"], pa.large_string())),
            ],
            "binary": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "decimal128": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("65535"), None], pa.decimal128(38, 10)
                    ),
                ),
                (
                    pa.array([32768], pa.uint16()),
                    pa.array([Decimal("32768")], pa.decimal128(38, 10)),
                ),
            ],
            "decimal256": [
                (
                    pa.array([0, 1, 65535, None], pa.uint16()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("65535"), None], pa.decimal256(76, 10)
                    ),
                ),
                (
                    pa.array([32768], pa.uint16()),
                    pa.array([Decimal("32768")], pa.decimal256(76, 10)),
                ),
            ],
            "date32": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([0], pa.uint16()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "uint16")

    # ============================================================
    # uint32 casts
    # ============================================================
    def test_uint32_casts(self):
        """Test uint32 -> all scalar types. Boundaries: 0 to 4294967295"""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            "int8": [
                (pa.array([0, 1, 127, None], pa.uint32()), pa.array([0, 1, 127, None], pa.int8())),
                (pa.array([128], pa.uint32()), pa.lib.ArrowInvalid),
                (pa.array([4294967295], pa.uint32()), pa.lib.ArrowInvalid),
            ],
            "int16": [
                (
                    pa.array([0, 1, 32767, None], pa.uint32()),
                    pa.array([0, 1, 32767, None], pa.int16()),
                ),
                (pa.array([32768], pa.uint32()), pa.lib.ArrowInvalid),
                (pa.array([4294967295], pa.uint32()), pa.lib.ArrowInvalid),
            ],
            "int32": [
                (
                    pa.array([0, 1, 2147483647, None], pa.uint32()),
                    pa.array([0, 1, 2147483647, None], pa.int32()),
                ),
                (pa.array([2147483648], pa.uint32()), pa.lib.ArrowInvalid),
                (pa.array([4294967295], pa.uint32()), pa.lib.ArrowInvalid),
            ],
            "int64": [
                (
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                    pa.array([0, 1, 4294967295, None], pa.int64()),
                ),
                (pa.array([2147483648], pa.uint32()), pa.array([2147483648], pa.int64())),
                (pa.array([None, None], pa.uint32()), pa.array([None, None], pa.int64())),
            ],
            "uint8": [
                (pa.array([0, 1, 255, None], pa.uint32()), pa.array([0, 1, 255, None], pa.uint8())),
                (pa.array([256], pa.uint32()), pa.lib.ArrowInvalid),
                (pa.array([4294967295], pa.uint32()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                (
                    pa.array([0, 1, 65535, None], pa.uint32()),
                    pa.array([0, 1, 65535, None], pa.uint16()),
                ),
                (pa.array([65536], pa.uint32()), pa.lib.ArrowInvalid),
                (pa.array([4294967295], pa.uint32()), pa.lib.ArrowInvalid),
            ],
            "uint32": [
                (
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                ),
                (pa.array([2147483648], pa.uint32()), pa.array([2147483648], pa.uint32())),
                (pa.array([], pa.uint32()), pa.array([], pa.uint32())),
            ],
            "uint64": [
                (
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                    pa.array([0, 1, 4294967295, None], pa.uint64()),
                ),
                (pa.array([2147483648], pa.uint32()), pa.array([2147483648], pa.uint64())),
                (pa.array([None, None], pa.uint32()), pa.array([None, None], pa.uint64())),
            ],
            "float16": [
                (pa.array([0, 1, None], pa.uint32()), pa.array([0.0, 1.0, None], pa.float16())),
                (pa.array([2048], pa.uint32()), pa.array([2048.0], pa.float16())),
                (pa.array([4294967295], pa.uint32()), pa.lib.ArrowInvalid),
            ],
            "float32": [
                (pa.array([0, 1, None], pa.uint32()), pa.array([0.0, 1.0, None], pa.float32())),
                (pa.array([16777216], pa.uint32()), pa.array([16777216.0], pa.float32())),
                (pa.array([4294967295], pa.uint32()), pa.lib.ArrowInvalid),
            ],
            "float64": [
                (
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                    pa.array([0.0, 1.0, 4294967295.0, None], pa.float64()),
                ),
                (pa.array([2147483648], pa.uint32()), pa.array([2147483648.0], pa.float64())),
                (pa.array([None, None], pa.uint32()), pa.array([None, None], pa.float64())),
            ],
            # === Non-numerical types ===
            "bool": [
                (
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                    pa.array([False, True, True, None], pa.bool_()),
                ),
                (pa.array([2147483648], pa.uint32()), pa.array([True], pa.bool_())),
                (pa.array([], pa.uint32()), pa.array([], pa.bool_())),
            ],
            "string": [
                (
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                    pa.array(["0", "1", "4294967295", None], pa.string()),
                ),
                (pa.array([2147483648], pa.uint32()), pa.array(["2147483648"], pa.string())),
                (pa.array([], pa.uint32()), pa.array([], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                    pa.array(["0", "1", "4294967295", None], pa.large_string()),
                ),
                (pa.array([2147483648], pa.uint32()), pa.array(["2147483648"], pa.large_string())),
            ],
            "binary": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "decimal128": [
                (
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("4294967295"), None],
                        pa.decimal128(38, 10),
                    ),
                ),
                (
                    pa.array([2147483648], pa.uint32()),
                    pa.array([Decimal("2147483648")], pa.decimal128(38, 10)),
                ),
            ],
            "decimal256": [
                (
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("4294967295"), None],
                        pa.decimal256(76, 10),
                    ),
                ),
                (
                    pa.array([2147483648], pa.uint32()),
                    pa.array([Decimal("2147483648")], pa.decimal256(76, 10)),
                ),
            ],
            "date32": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([0], pa.uint32()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "uint32")

    # ============================================================
    # uint64 casts
    # ============================================================
    def test_uint64_casts(self):
        """Test uint64 -> all scalar types. Boundaries: 0 to 18446744073709551615"""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            "int8": [
                (pa.array([0, 1, 127, None], pa.uint64()), pa.array([0, 1, 127, None], pa.int8())),
                (pa.array([128], pa.uint64()), pa.lib.ArrowInvalid),
                (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
            ],
            "int16": [
                (
                    pa.array([0, 1, 32767, None], pa.uint64()),
                    pa.array([0, 1, 32767, None], pa.int16()),
                ),
                (pa.array([32768], pa.uint64()), pa.lib.ArrowInvalid),
                (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
            ],
            "int32": [
                (
                    pa.array([0, 1, 2147483647, None], pa.uint64()),
                    pa.array([0, 1, 2147483647, None], pa.int32()),
                ),
                (pa.array([2147483648], pa.uint64()), pa.lib.ArrowInvalid),
                (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
            ],
            "int64": [
                (
                    pa.array([0, 1, 9223372036854775807, None], pa.uint64()),
                    pa.array([0, 1, 9223372036854775807, None], pa.int64()),
                ),
                (pa.array([9223372036854775808], pa.uint64()), pa.lib.ArrowInvalid),
                (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
            ],
            "uint8": [
                (pa.array([0, 1, 255, None], pa.uint64()), pa.array([0, 1, 255, None], pa.uint8())),
                (pa.array([256], pa.uint64()), pa.lib.ArrowInvalid),
                (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                (
                    pa.array([0, 1, 65535, None], pa.uint64()),
                    pa.array([0, 1, 65535, None], pa.uint16()),
                ),
                (pa.array([65536], pa.uint64()), pa.lib.ArrowInvalid),
                (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
            ],
            "uint32": [
                (
                    pa.array([0, 1, 4294967295, None], pa.uint64()),
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                ),
                (pa.array([4294967296], pa.uint64()), pa.lib.ArrowInvalid),
                (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
            ],
            "uint64": [
                (
                    pa.array([0, 1, 18446744073709551615, None], pa.uint64()),
                    pa.array([0, 1, 18446744073709551615, None], pa.uint64()),
                ),
                (
                    pa.array([9223372036854775808], pa.uint64()),
                    pa.array([9223372036854775808], pa.uint64()),
                ),
                (pa.array([], pa.uint64()), pa.array([], pa.uint64())),
            ],
            "float16": [
                (pa.array([0, 1, None], pa.uint64()), pa.array([0.0, 1.0, None], pa.float16())),
                (pa.array([2048], pa.uint64()), pa.array([2048.0], pa.float16())),
                (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
            ],
            "float32": [
                (pa.array([0, 1, None], pa.uint64()), pa.array([0.0, 1.0, None], pa.float32())),
                (pa.array([16777216], pa.uint64()), pa.array([16777216.0], pa.float32())),
                (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
            ],
            "float64": [
                (pa.array([0, 1, None], pa.uint64()), pa.array([0.0, 1.0, None], pa.float64())),
                (
                    pa.array([9007199254740992], pa.uint64()),
                    pa.array([9007199254740992.0], pa.float64()),
                ),
                (pa.array([9007199254740993], pa.uint64()), pa.lib.ArrowInvalid),
            ],
            # === Non-numerical types ===
            "bool": [
                (
                    pa.array([0, 1, 18446744073709551615, None], pa.uint64()),
                    pa.array([False, True, True, None], pa.bool_()),
                ),
                (pa.array([9223372036854775808], pa.uint64()), pa.array([True], pa.bool_())),
                (pa.array([], pa.uint64()), pa.array([], pa.bool_())),
            ],
            "string": [
                (pa.array([0, 1, None], pa.uint64()), pa.array(["0", "1", None], pa.string())),
                (
                    pa.array([18446744073709551615], pa.uint64()),
                    pa.array(["18446744073709551615"], pa.string()),
                ),
                (pa.array([], pa.uint64()), pa.array([], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([0, 1, None], pa.uint64()),
                    pa.array(["0", "1", None], pa.large_string()),
                ),
                (
                    pa.array([18446744073709551615], pa.uint64()),
                    pa.array(["18446744073709551615"], pa.large_string()),
                ),
            ],
            "binary": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "decimal128": [
                (
                    pa.array([0, 1, None], pa.uint64()),
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal128(38, 10)),
                ),
                (
                    pa.array([18446744073709551615], pa.uint64()),
                    pa.array([Decimal("18446744073709551615")], pa.decimal128(38, 10)),
                ),
            ],
            "decimal256": [
                (
                    pa.array([0, 1, None], pa.uint64()),
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal256(76, 10)),
                ),
                (
                    pa.array([18446744073709551615], pa.uint64()),
                    pa.array([Decimal("18446744073709551615")], pa.decimal256(76, 10)),
                ),
            ],
            "date32": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([0], pa.uint64()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "uint64")

    # ============================================================
    # float16 casts
    # ============================================================
    def test_float16_casts(self):
        """Test float16 -> all numerical types.

        float16 characteristics:
        - Range: ~6.1e-5 to ~6.5e4 (positive normalized)
        - Precision: ~3 decimal digits
        - Max value: 65504
        - Min positive normal: ~6.1e-5
        - Subnormal min: ~5.96e-8
        """
        import pyarrow as pa

        # float16 special values
        f16_max = 65504.0
        f16_min_normal = 6.103515625e-05  # smallest positive normal

        casts = {
            "int8": [
                (
                    pa.array([0.0, 1.0, -1.0, 127.0, -128.0, None], pa.float16()),
                    pa.array([0, 1, -1, 127, -128, None], pa.int8()),
                ),
                # fractional values -> ArrowInvalid
                (pa.array([1.5, -0.5], pa.float16()), pa.lib.ArrowInvalid),
                # overflow int8 range
                (pa.array([128.0], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([-129.0], pa.float16()), pa.lib.ArrowInvalid),
                # special: NaN, Inf -> ArrowInvalid for int
                (pa.array([float("nan")], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([float("inf")], pa.float16()), pa.lib.ArrowInvalid),
            ],
            "int16": [
                # Note: float16 can only represent integers exactly up to 2048
                (
                    pa.array([0.0, 1.0, -1.0, 2048.0, -2048.0, None], pa.float16()),
                    pa.array([0, 1, -1, 2048, -2048, None], pa.int16()),
                ),
                (pa.array([1.5], pa.float16()), pa.lib.ArrowInvalid),
                # float16 max (65504) overflows int16 max (32767)
                (pa.array([f16_max], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([float("nan")], pa.float16()), pa.lib.ArrowInvalid),
            ],
            "int32": [
                (
                    pa.array([0.0, 1.0, -1.0, f16_max, None], pa.float16()),
                    pa.array([0, 1, -1, 65504, None], pa.int32()),
                ),
                (pa.array([1.5], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([float("inf")], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([float("nan")], pa.float16()), pa.lib.ArrowInvalid),
            ],
            "int64": [
                (
                    pa.array([0.0, 1.0, -1.0, f16_max, None], pa.float16()),
                    pa.array([0, 1, -1, 65504, None], pa.int64()),
                ),
                (pa.array([1.5], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([float("-inf")], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([float("nan")], pa.float16()), pa.lib.ArrowInvalid),
            ],
            "uint8": [
                (
                    pa.array([0.0, 1.0, 255.0, None], pa.float16()),
                    pa.array([0, 1, 255, None], pa.uint8()),
                ),
                # negative values -> ArrowInvalid for unsigned
                (pa.array([-1.0], pa.float16()), pa.lib.ArrowInvalid),
                # overflow uint8 range
                (pa.array([256.0], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([float("nan")], pa.float16()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                # Note: float16 can only represent integers exactly up to 2048
                (
                    pa.array([0.0, 1.0, 2048.0, None], pa.float16()),
                    pa.array([0, 1, 2048, None], pa.uint16()),
                ),
                (pa.array([-1.0], pa.float16()), pa.lib.ArrowInvalid),
                # fractional
                (pa.array([1.5], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([float("inf")], pa.float16()), pa.lib.ArrowInvalid),
            ],
            "uint32": [
                (
                    pa.array([0.0, 1.0, f16_max, None], pa.float16()),
                    pa.array([0, 1, 65504, None], pa.uint32()),
                ),
                (pa.array([-1.0], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([1.5], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([float("nan")], pa.float16()), pa.lib.ArrowInvalid),
            ],
            "uint64": [
                (
                    pa.array([0.0, 1.0, f16_max, None], pa.float16()),
                    pa.array([0, 1, 65504, None], pa.uint64()),
                ),
                (pa.array([-1.0], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([1.5], pa.float16()), pa.lib.ArrowInvalid),
                (pa.array([float("-inf")], pa.float16()), pa.lib.ArrowInvalid),
            ],
            "float16": [
                # identity cast: normal values + special values
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float16()),
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float16()),
                ),
                # special values: NaN, Inf, -Inf
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float16()),
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float16()),
                ),
                # boundary values
                (
                    pa.array([f16_max, -f16_max, f16_min_normal], pa.float16()),
                    pa.array([f16_max, -f16_max, f16_min_normal], pa.float16()),
                ),
            ],
            "float32": [
                # upcast preserves all values exactly
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float16()),
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float32()),
                ),
                # special values preserved
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float16()),
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float32()),
                ),
                # boundary values
                (
                    pa.array([f16_max, f16_min_normal], pa.float16()),
                    pa.array([f16_max, f16_min_normal], pa.float32()),
                ),
            ],
            "float64": [
                # upcast preserves all values exactly
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float16()),
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float64()),
                ),
                # special values preserved
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float16()),
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float64()),
                ),
                # boundary values
                (
                    pa.array([f16_max, f16_min_normal], pa.float16()),
                    pa.array([f16_max, f16_min_normal], pa.float64()),
                ),
            ],
            # === Non-numerical types ===
            "bool": [
                # float16 -> bool not supported
                (pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError),
            ],
            "string": [
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, None], pa.float16()),
                    pa.array(["0", "1", "-1", "1.5", None], pa.string()),
                ),
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float16()),
                    pa.array(["nan", "inf", "-inf"], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([0.0, 1.0, -1.0, None], pa.float16()),
                    pa.array(["0", "1", "-1", None], pa.large_string()),
                ),
            ],
            "binary": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [
                (pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)
            ],
            "decimal128": [
                (pa.array([0.0, 1.0, -1.0, None], pa.float16()), pa.lib.ArrowNotImplementedError),
            ],
            "decimal256": [
                (pa.array([0.0, 1.0, -1.0, None], pa.float16()), pa.lib.ArrowNotImplementedError),
            ],
            "date32": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([0.0], pa.float16()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "float16")

    # ============================================================
    # float32 casts
    # ============================================================
    def test_float32_casts(self):
        """Test float32 -> all numerical types.

        float32 characteristics:
        - Range: ~1.2e-38 to ~3.4e38 (positive normalized)
        - Precision: ~7 decimal digits
        - Max value: ~3.4028235e38
        - Min positive normal: ~1.175494e-38
        - Subnormal min: ~1.4e-45
        """
        import pyarrow as pa
        from decimal import Decimal

        # float32 special values
        f32_max = 3.4028235e38
        f32_min_normal = 1.1754944e-38
        f16_max = 65504.0  # for overflow to float16

        casts = {
            "int8": [
                (
                    pa.array([0.0, 1.0, -1.0, 127.0, -128.0, None], pa.float32()),
                    pa.array([0, 1, -1, 127, -128, None], pa.int8()),
                ),
                (
                    pa.array([1.5, 128.0, float("nan")], pa.float32()),
                    pa.lib.ArrowInvalid,
                ),  # fractional/overflow/special
            ],
            "int16": [
                (
                    pa.array([0.0, 1.0, -1.0, 32767.0, -32768.0, None], pa.float32()),
                    pa.array([0, 1, -1, 32767, -32768, None], pa.int16()),
                ),
                (pa.array([1.5, 32768.0, float("inf")], pa.float32()), pa.lib.ArrowInvalid),
            ],
            "int32": [
                (
                    pa.array([0.0, 1.0, -1.0, None], pa.float32()),
                    pa.array([0, 1, -1, None], pa.int32()),
                ),
                # float32 2147483648.0 boundary: behavior varies across environments
                (
                    pa.array([2147483648.0], pa.float32()),
                    _get_float_to_int_boundary_expected(pa.int32()),
                ),
                (
                    pa.array([1.5, 3e9, float("nan")], pa.float32()),
                    pa.lib.ArrowInvalid,
                ),  # fractional/overflow/special
            ],
            "int64": [
                (
                    pa.array([0.0, 1.0, -1.0, None], pa.float32()),
                    pa.array([0, 1, -1, None], pa.int64()),
                ),
                (pa.array([1.5, f32_max, float("inf")], pa.float32()), pa.lib.ArrowInvalid),
            ],
            "uint8": [
                (
                    pa.array([0.0, 1.0, 255.0, None], pa.float32()),
                    pa.array([0, 1, 255, None], pa.uint8()),
                ),
                (pa.array([-1.0, 256.0, float("nan")], pa.float32()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                (
                    pa.array([0.0, 1.0, 65535.0, None], pa.float32()),
                    pa.array([0, 1, 65535, None], pa.uint16()),
                ),
                (pa.array([-1.0, 65536.0, float("inf")], pa.float32()), pa.lib.ArrowInvalid),
            ],
            "uint32": [
                (pa.array([0.0, 1.0, None], pa.float32()), pa.array([0, 1, None], pa.uint32())),
                (pa.array([-1.0, 1.5, float("nan")], pa.float32()), pa.lib.ArrowInvalid),
            ],
            "uint64": [
                (pa.array([0.0, 1.0, None], pa.float32()), pa.array([0, 1, None], pa.uint64())),
                (pa.array([-1.0, 1.5, float("-inf")], pa.float32()), pa.lib.ArrowInvalid),
            ],
            "float16": [
                # values within float16 range
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float32()),
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float16()),
                ),
                # special values preserved
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float32()),
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float16()),
                ),
                # OVERFLOW: beyond float16 max -> Inf
                (
                    pa.array([100000.0, -100000.0, f32_max], pa.float32()),
                    pa.array([float("inf"), float("-inf"), float("inf")], pa.float16()),
                ),
                # boundary: exactly float16 max
                (pa.array([f16_max], pa.float32()), pa.array([f16_max], pa.float16())),
            ],
            "float32": [
                # identity cast: normal + special + boundary
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float32()),
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float32()),
                ),
                # special values
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float32()),
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float32()),
                ),
                # boundary values
                (
                    pa.array([f32_max, -f32_max, f32_min_normal], pa.float32()),
                    pa.array([f32_max, -f32_max, f32_min_normal], pa.float32()),
                ),
            ],
            "float64": [
                # upcast preserves values exactly
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float32()),
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float64()),
                ),
                # special values preserved
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float32()),
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float64()),
                ),
                # PRECISION EXPANSION: float32 -> float64 reveals rounding
                (
                    pa.array([1234.5678, 0.1], pa.float32()),
                    pa.array([1234.5677490234375, 0.10000000149011612], pa.float64()),
                ),
                # boundary values show true representation
                (
                    pa.array([f32_max, f32_min_normal], pa.float32()),
                    pa.array([3.4028234663852886e38, 1.1754943508222875e-38], pa.float64()),
                ),
            ],
            # === Non-numerical types ===
            "bool": [
                (
                    pa.array([0.0, 1.0, -1.0, None], pa.float32()),
                    pa.array([False, True, True, None], pa.bool_()),
                ),
                (
                    pa.array([0.5, 100.0, -0.1], pa.float32()),
                    pa.array([True, True, True], pa.bool_()),
                ),
                # NaN -> True (non-zero), Inf -> True
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float32()),
                    pa.array([True, True, True], pa.bool_()),
                ),
            ],
            "string": [
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, 1e10, None], pa.float32()),
                    pa.array(["0", "1", "-1", "1.5", "1e+10", None], pa.string()),
                ),
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float32()),
                    pa.array(["nan", "inf", "-inf"], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([0.0, 1.0, float("nan"), None], pa.float32()),
                    pa.array(["0", "1", "nan", None], pa.large_string()),
                ),
            ],
            "binary": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [
                (pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)
            ],
            "decimal128": [
                (
                    pa.array([0.0, 1.0, -1.0, None], pa.float32()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal128(38, 10)
                    ),
                ),
                (
                    pa.array([1.5, -2.5], pa.float32()),
                    pa.array([Decimal("1.5"), Decimal("-2.5")], pa.decimal128(38, 10)),
                ),
                (pa.array([float("nan")], pa.float32()), pa.lib.ArrowInvalid),
            ],
            "decimal256": [
                (
                    pa.array([0.0, 1.0, -1.0, None], pa.float32()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal256(76, 10)
                    ),
                ),
                (pa.array([float("inf")], pa.float32()), pa.lib.ArrowInvalid),
            ],
            "date32": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([0.0], pa.float32()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "float32")

    # ============================================================
    # float64 casts
    # ============================================================
    def test_float64_casts(self):
        """Test float64 -> all numerical types.

        float64 characteristics:
        - Range: ~2.2e-308 to ~1.8e308 (positive normalized)
        - Precision: ~15-17 decimal digits
        - Max value: ~1.7976931348623157e308
        - Min positive normal: ~2.2250738585072014e-308
        - Subnormal min: ~5e-324
        """
        import pyarrow as pa
        from decimal import Decimal

        # float64 special values
        f64_max = 1.7976931348623157e308
        f64_min_normal = 2.2250738585072014e-308
        f32_max = 3.4028235e38
        f16_max = 65504.0

        casts = {
            "int8": [
                (
                    pa.array([0.0, 1.0, -1.0, 127.0, -128.0, None], pa.float64()),
                    pa.array([0, 1, -1, 127, -128, None], pa.int8()),
                ),
                # fractional
                (pa.array([1.5, 0.1, 127.9], pa.float64()), pa.lib.ArrowInvalid),
                # overflow
                (pa.array([128.0], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([-129.0], pa.float64()), pa.lib.ArrowInvalid),
                # === SPECIAL VALUES: NaN, Inf, -Inf -> ArrowInvalid for int ===
                (pa.array([float("nan")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("inf")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("-inf")], pa.float64()), pa.lib.ArrowInvalid),
                # mixed array with special value fails
                (pa.array([1.0, float("nan")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([0.0, float("inf")], pa.float64()), pa.lib.ArrowInvalid),
            ],
            "int16": [
                (
                    pa.array([0.0, 1.0, -1.0, 32767.0, -32768.0, None], pa.float64()),
                    pa.array([0, 1, -1, 32767, -32768, None], pa.int16()),
                ),
                (pa.array([1.5], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([32768.0], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([-32769.0], pa.float64()), pa.lib.ArrowInvalid),
                # === SPECIAL VALUES ===
                (pa.array([float("nan")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("inf")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("-inf")], pa.float64()), pa.lib.ArrowInvalid),
            ],
            "int32": [
                (
                    pa.array([0.0, 1.0, -1.0, 2147483647.0, -2147483648.0, None], pa.float64()),
                    pa.array([0, 1, -1, 2147483647, -2147483648, None], pa.int32()),
                ),
                (pa.array([1.5], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([2147483648.0], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([-2147483649.0], pa.float64()), pa.lib.ArrowInvalid),
                # === SPECIAL VALUES ===
                (pa.array([float("nan")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("inf")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("-inf")], pa.float64()), pa.lib.ArrowInvalid),
            ],
            "int64": [
                (
                    pa.array([0.0, 1.0, -1.0, None], pa.float64()),
                    pa.array([0, 1, -1, None], pa.int64()),
                ),
                (pa.array([1.5], pa.float64()), pa.lib.ArrowInvalid),
                # float64 9223372036854775808.0 boundary: behavior varies across environments
                (
                    pa.array([9223372036854775808.0], pa.float64()),
                    _get_float_to_int_boundary_expected(pa.int64()),
                ),
                # truly overflow - values beyond int64 range
                (pa.array([1e19], pa.float64()), pa.lib.ArrowInvalid),
                # === SPECIAL VALUES ===
                (pa.array([float("nan")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("inf")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("-inf")], pa.float64()), pa.lib.ArrowInvalid),
                # large but valid: 2^53 is exactly representable in float64
                (
                    pa.array([9007199254740992.0], pa.float64()),
                    pa.array([9007199254740992], pa.int64()),
                ),
            ],
            "uint8": [
                (
                    pa.array([0.0, 1.0, 255.0, None], pa.float64()),
                    pa.array([0, 1, 255, None], pa.uint8()),
                ),
                (pa.array([-1.0], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([256.0], pa.float64()), pa.lib.ArrowInvalid),
                # === SPECIAL VALUES: all fail for unsigned ===
                (pa.array([float("nan")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("inf")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("-inf")], pa.float64()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                (
                    pa.array([0.0, 1.0, 65535.0, None], pa.float64()),
                    pa.array([0, 1, 65535, None], pa.uint16()),
                ),
                (pa.array([-1.0], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([65536.0], pa.float64()), pa.lib.ArrowInvalid),
                # === SPECIAL VALUES ===
                (pa.array([float("inf")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("-inf")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("nan")], pa.float64()), pa.lib.ArrowInvalid),
            ],
            "uint32": [
                (
                    pa.array([0.0, 1.0, 4294967295.0, None], pa.float64()),
                    pa.array([0, 1, 4294967295, None], pa.uint32()),
                ),
                (pa.array([-1.0], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([4294967296.0], pa.float64()), pa.lib.ArrowInvalid),
                # === SPECIAL VALUES ===
                (pa.array([float("nan")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("inf")], pa.float64()), pa.lib.ArrowInvalid),
                (pa.array([float("-inf")], pa.float64()), pa.lib.ArrowInvalid),
            ],
            "uint64": [
                (
                    pa.array([0.0, 1.0, 9007199254740992.0, None], pa.float64()),
                    pa.array([0, 1, 9007199254740992, None], pa.uint64()),
                ),
                (pa.array([-1.0, 1.5, float("nan")], pa.float64()), pa.lib.ArrowInvalid),
            ],
            "float16": [
                # normal values + special values
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float64()),
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float16()),
                ),
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float64()),
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float16()),
                ),
                # OVERFLOW: beyond float16 max -> Inf
                (
                    pa.array([100000.0, -100000.0, f64_max], pa.float64()),
                    pa.array([float("inf"), float("-inf"), float("inf")], pa.float16()),
                ),
                # boundary + precision loss
                (
                    pa.array([f16_max, 1.0001], pa.float64()),
                    pa.array([f16_max, 1.0], pa.float16()),
                ),  # 1.0001 rounds to 1.0
            ],
            "float32": [
                # normal values + special values
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float64()),
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float32()),
                ),
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float64()),
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float32()),
                ),
                # OVERFLOW: beyond float32 max -> Inf
                (
                    pa.array([1e40, -1e40, f64_max], pa.float64()),
                    pa.array([float("inf"), float("-inf"), float("inf")], pa.float32()),
                ),
                # boundary + precision loss
                (
                    pa.array([f32_max, 1.23456789012345], pa.float64()),
                    pa.array([f32_max, 1.2345678806304932], pa.float32()),
                ),
            ],
            "float64": [
                # identity cast: normal + special + boundary
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float64()),
                    pa.array([0.0, 1.0, -1.0, 1.5, -0.0, None], pa.float64()),
                ),
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float64()),
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float64()),
                ),
                # boundary + subnormal values
                (
                    pa.array([f64_max, -f64_max, f64_min_normal, 5e-324], pa.float64()),
                    pa.array([f64_max, -f64_max, f64_min_normal, 5e-324], pa.float64()),
                ),
            ],
            # === Non-numerical types ===
            "bool": [
                (
                    pa.array([0.0, 1.0, -1.0, 0.5, None], pa.float64()),
                    pa.array([False, True, True, True, None], pa.bool_()),
                ),
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float64()),
                    pa.array([True, True, True], pa.bool_()),
                ),
            ],
            "string": [
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, 1e100, None], pa.float64()),
                    pa.array(["0", "1", "-1", "1.5", "1e+100", None], pa.string()),
                ),
                (
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float64()),
                    pa.array(["nan", "inf", "-inf"], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([0.0, 1.0, float("nan"), None], pa.float64()),
                    pa.array(["0", "1", "nan", None], pa.large_string()),
                ),
            ],
            "binary": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [
                (pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)
            ],
            "decimal128": [
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, None], pa.float64()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), Decimal("1.5"), None],
                        pa.decimal128(38, 10),
                    ),
                ),
                (pa.array([float("nan"), float("inf")], pa.float64()), pa.lib.ArrowInvalid),
            ],
            "decimal256": [
                (
                    pa.array([0.0, 1.0, -1.0, 1.5, None], pa.float64()),
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), Decimal("1.5"), None],
                        pa.decimal256(76, 10),
                    ),
                ),
                (pa.array([float("nan")], pa.float64()), pa.lib.ArrowInvalid),
            ],
            "date32": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([0.0], pa.float64()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "float64")

    # ============================================================
    # bool casts
    # ============================================================
    def test_bool_casts(self):
        """Test bool -> all scalar types."""
        import pyarrow as pa

        casts = {
            # bool -> integers (True=1, False=0)
            "int8": [
                (pa.array([True, False, None], pa.bool_()), pa.array([1, 0, None], pa.int8())),
            ],
            "int16": [
                (pa.array([True, False, None], pa.bool_()), pa.array([1, 0, None], pa.int16())),
            ],
            "int32": [
                (pa.array([True, False, None], pa.bool_()), pa.array([1, 0, None], pa.int32())),
            ],
            "int64": [
                (pa.array([True, False, None], pa.bool_()), pa.array([1, 0, None], pa.int64())),
            ],
            "uint8": [
                (pa.array([True, False, None], pa.bool_()), pa.array([1, 0, None], pa.uint8())),
            ],
            "uint16": [
                (pa.array([True, False, None], pa.bool_()), pa.array([1, 0, None], pa.uint16())),
            ],
            "uint32": [
                (pa.array([True, False, None], pa.bool_()), pa.array([1, 0, None], pa.uint32())),
            ],
            "uint64": [
                (pa.array([True, False, None], pa.bool_()), pa.array([1, 0, None], pa.uint64())),
            ],
            # bool -> floats
            "float16": [
                (pa.array([True, False, None], pa.bool_()), pa.lib.ArrowNotImplementedError),
            ],
            "float32": [
                (
                    pa.array([True, False, None], pa.bool_()),
                    pa.array([1.0, 0.0, None], pa.float32()),
                ),
            ],
            "float64": [
                (
                    pa.array([True, False, None], pa.bool_()),
                    pa.array([1.0, 0.0, None], pa.float64()),
                ),
            ],
            # bool -> bool (identity)
            "bool": [
                (
                    pa.array([True, False, None], pa.bool_()),
                    pa.array([True, False, None], pa.bool_()),
                ),
            ],
            # bool -> string
            "string": [
                (
                    pa.array([True, False, None], pa.bool_()),
                    pa.array(["true", "false", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([True, False, None], pa.bool_()),
                    pa.array(["true", "false", None], pa.large_string()),
                ),
            ],
            # bool -> binary (not supported)
            "binary": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            # bool -> decimal (not supported)
            "decimal128": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "decimal256": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            # bool -> temporal (not supported)
            "date32": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms_tz": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([True, False], pa.bool_()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "bool")

    # ============================================================
    # string casts
    # ============================================================
    def test_string_casts(self):
        """Test string -> all scalar types."""
        import pyarrow as pa
        from decimal import Decimal
        import datetime

        casts = {
            # string -> integers (parse string to int)
            "int8": [
                (
                    pa.array(["0", "1", "-1", "127", "-128", None], pa.string()),
                    pa.array([0, 1, -1, 127, -128, None], pa.int8()),
                ),
                (pa.array(["128", "abc"], pa.string()), pa.lib.ArrowInvalid),  # overflow/invalid
            ],
            "int16": [
                (
                    pa.array(["0", "1", "-1", None], pa.string()),
                    pa.array([0, 1, -1, None], pa.int16()),
                ),
                (pa.array(["abc"], pa.string()), pa.lib.ArrowInvalid),
            ],
            "int32": [
                (
                    pa.array(["0", "1", "-1", None], pa.string()),
                    pa.array([0, 1, -1, None], pa.int32()),
                ),
                (pa.array(["abc"], pa.string()), pa.lib.ArrowInvalid),
            ],
            "int64": [
                (
                    pa.array(["0", "1", "-1", None], pa.string()),
                    pa.array([0, 1, -1, None], pa.int64()),
                ),
                (pa.array(["abc"], pa.string()), pa.lib.ArrowInvalid),
            ],
            "uint8": [
                (
                    pa.array(["0", "1", "255", None], pa.string()),
                    pa.array([0, 1, 255, None], pa.uint8()),
                ),
                (pa.array(["-1", "256"], pa.string()), pa.lib.ArrowInvalid),
            ],
            "uint16": [
                (pa.array(["0", "1", None], pa.string()), pa.array([0, 1, None], pa.uint16())),
            ],
            "uint32": [
                (pa.array(["0", "1", None], pa.string()), pa.array([0, 1, None], pa.uint32())),
            ],
            "uint64": [
                (pa.array(["0", "1", None], pa.string()), pa.array([0, 1, None], pa.uint64())),
            ],
            # string -> floats
            "float16": [
                (
                    pa.array(["0", "1.5", "-1.5", None], pa.string()),
                    pa.array([0.0, 1.5, -1.5, None], pa.float16()),
                ),
            ],
            "float32": [
                (
                    pa.array(["0", "1.5", "-1.5", None], pa.string()),
                    pa.array([0.0, 1.5, -1.5, None], pa.float32()),
                ),
                (
                    pa.array(["nan", "inf", "-inf"], pa.string()),
                    pa.array([float("nan"), float("inf"), float("-inf")], pa.float32()),
                ),
            ],
            "float64": [
                (
                    pa.array(["0", "1.5", "-1.5", None], pa.string()),
                    pa.array([0.0, 1.5, -1.5, None], pa.float64()),
                ),
            ],
            # string -> bool
            "bool": [
                (
                    pa.array(["true", "false", None], pa.string()),
                    pa.array([True, False, None], pa.bool_()),
                ),
                (pa.array(["1", "0"], pa.string()), pa.array([True, False], pa.bool_())),
                (pa.array(["yes", "abc"], pa.string()), pa.lib.ArrowInvalid),
            ],
            # string -> string (identity)
            "string": [
                (
                    pa.array(["hello", "world", None], pa.string()),
                    pa.array(["hello", "world", None], pa.string()),
                ),
                # Empty string
                (pa.array(["", None], pa.string()), pa.array(["", None], pa.string())),
                # Unicode: CJK characters (Chinese, Japanese, Korean)
                (
                    pa.array(["你好", "世界", None], pa.string()),
                    pa.array(["你好", "世界", None], pa.string()),
                ),
                (pa.array(["こんにちは", "日本語"], pa.string()), pa.array(["こんにちは", "日本語"], pa.string())),
                (pa.array(["안녕하세요", "한국어"], pa.string()), pa.array(["안녕하세요", "한국어"], pa.string())),
                # Unicode: Emoji (including multi-codepoint emoji)
                (
                    pa.array(["😀", "🎉🎊", "👨‍👩‍👧‍👦"], pa.string()),
                    pa.array(["😀", "🎉🎊", "👨‍👩‍👧‍👦"], pa.string()),
                ),
                # Unicode: RTL languages (Arabic, Hebrew)
                (
                    pa.array(["مرحبا", "שלום"], pa.string()),
                    pa.array(["مرحبا", "שלום"], pa.string()),
                ),
                # Unicode: Mixed scripts
                (
                    pa.array(["Hello世界🌍", "Привет мир"], pa.string()),
                    pa.array(["Hello世界🌍", "Привет мир"], pa.string()),
                ),
                # Special characters (whitespace, control chars)
                (
                    pa.array(["a\tb\nc", "  spaces  "], pa.string()),
                    pa.array(["a\tb\nc", "  spaces  "], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array(["hello", "world", None], pa.string()),
                    pa.array(["hello", "world", None], pa.large_string()),
                ),
                # Unicode preserved in large_string
                (
                    pa.array(["你好", "😀", None], pa.string()),
                    pa.array(["你好", "😀", None], pa.large_string()),
                ),
            ],
            # string -> binary
            "binary": [
                (
                    pa.array(["hello", "world", None], pa.string()),
                    pa.array([b"hello", b"world", None], pa.binary()),
                ),
                # Empty string -> empty binary
                (pa.array(["", None], pa.string()), pa.array([b"", None], pa.binary())),
                # Unicode strings -> UTF-8 encoded binary
                (
                    pa.array(["你好", None], pa.string()),
                    pa.array(["你好".encode("utf-8"), None], pa.binary()),
                ),
                (pa.array(["😀"], pa.string()), pa.array(["😀".encode("utf-8")], pa.binary())),
            ],
            "large_binary": [
                (
                    pa.array(["hello", "world", None], pa.string()),
                    pa.array([b"hello", b"world", None], pa.large_binary()),
                ),
                # Unicode to large_binary
                (
                    pa.array(["世界", None], pa.string()),
                    pa.array(["世界".encode("utf-8"), None], pa.large_binary()),
                ),
            ],
            "fixed_size_binary_16": [
                (pa.array(["hello"], pa.string()), pa.lib.ArrowInvalid),  # wrong size
            ],
            # string -> decimal
            "decimal128": [
                (
                    pa.array(["0", "1.5", "-1.5", None], pa.string()),
                    pa.array(
                        [Decimal("0"), Decimal("1.5"), Decimal("-1.5"), None], pa.decimal128(38, 10)
                    ),
                ),
            ],
            "decimal256": [
                (
                    pa.array(["0", "1.5", None], pa.string()),
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal256(76, 10)),
                ),
            ],
            # string -> temporal
            "date32": [
                (
                    pa.array(["2022-01-01", None], pa.string()),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array(["2022-01-01", None], pa.string()),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            "timestamp_s": [
                (
                    pa.array(["2022-01-01 00:00:00", None], pa.string()),
                    pa.array([1640995200, None], pa.timestamp("s")),
                ),
            ],
            "timestamp_ms": [
                (
                    pa.array(["2022-01-01 00:00:00", None], pa.string()),
                    pa.array([1640995200000, None], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array(["2022-01-01 00:00:00", None], pa.string()),
                    pa.array([1640995200000000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array(["2022-01-01 00:00:00", None], pa.string()),
                    pa.array([1640995200000000000, None], pa.timestamp("ns")),
                ),
            ],
            "timestamp_s_tz": [
                (pa.array(["2022-01-01"], pa.string()), pa.lib.ArrowInvalid)
            ],  # needs zone offset
            "timestamp_ms_tz": [(pa.array(["2022-01-01"], pa.string()), pa.lib.ArrowInvalid)],
            "timestamp_us_tz": [(pa.array(["2022-01-01"], pa.string()), pa.lib.ArrowInvalid)],
            "timestamp_ns_tz": [(pa.array(["2022-01-01"], pa.string()), pa.lib.ArrowInvalid)],
            "timestamp_s_tz_ny": [(pa.array(["2022-01-01"], pa.string()), pa.lib.ArrowInvalid)],
            "timestamp_s_tz_shanghai": [
                (pa.array(["2022-01-01"], pa.string()), pa.lib.ArrowInvalid)
            ],
            "duration_s": [(pa.array(["1"], pa.string()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array(["1"], pa.string()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array(["1"], pa.string()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array(["1"], pa.string()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array(["12:00:00"], pa.string()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array(["12:00:00"], pa.string()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array(["12:00:00"], pa.string()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array(["12:00:00"], pa.string()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "string")

    def test_large_string_casts(self):
        """Test large_string -> all scalar types (similar to string)."""
        import pyarrow as pa
        from decimal import Decimal
        import datetime

        casts = {
            # large_string -> integers
            "int8": [
                (
                    pa.array(["0", "1", "-1", None], pa.large_string()),
                    pa.array([0, 1, -1, None], pa.int8()),
                ),
            ],
            "int16": [
                (
                    pa.array(["0", "1", "-1", None], pa.large_string()),
                    pa.array([0, 1, -1, None], pa.int16()),
                ),
            ],
            "int32": [
                (
                    pa.array(["0", "1", "-1", None], pa.large_string()),
                    pa.array([0, 1, -1, None], pa.int32()),
                ),
            ],
            "int64": [
                (
                    pa.array(["0", "1", "-1", None], pa.large_string()),
                    pa.array([0, 1, -1, None], pa.int64()),
                ),
            ],
            "uint8": [
                (pa.array(["0", "1", None], pa.large_string()), pa.array([0, 1, None], pa.uint8())),
            ],
            "uint16": [
                (
                    pa.array(["0", "1", None], pa.large_string()),
                    pa.array([0, 1, None], pa.uint16()),
                ),
            ],
            "uint32": [
                (
                    pa.array(["0", "1", None], pa.large_string()),
                    pa.array([0, 1, None], pa.uint32()),
                ),
            ],
            "uint64": [
                (
                    pa.array(["0", "1", None], pa.large_string()),
                    pa.array([0, 1, None], pa.uint64()),
                ),
            ],
            # large_string -> floats
            "float16": [
                (
                    pa.array(["0", "1.5", None], pa.large_string()),
                    pa.array([0.0, 1.5, None], pa.float16()),
                ),
            ],
            "float32": [
                (
                    pa.array(["0", "1.5", None], pa.large_string()),
                    pa.array([0.0, 1.5, None], pa.float32()),
                ),
            ],
            "float64": [
                (
                    pa.array(["0", "1.5", None], pa.large_string()),
                    pa.array([0.0, 1.5, None], pa.float64()),
                ),
            ],
            # large_string -> bool
            "bool": [
                (
                    pa.array(["true", "false", None], pa.large_string()),
                    pa.array([True, False, None], pa.bool_()),
                ),
            ],
            # large_string -> string
            "string": [
                (
                    pa.array(["hello", None], pa.large_string()),
                    pa.array(["hello", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array(["hello", None], pa.large_string()),
                    pa.array(["hello", None], pa.large_string()),
                ),
            ],
            # large_string -> binary
            "binary": [
                (
                    pa.array(["hello", None], pa.large_string()),
                    pa.array([b"hello", None], pa.binary()),
                ),
            ],
            "large_binary": [
                (
                    pa.array(["hello", None], pa.large_string()),
                    pa.array([b"hello", None], pa.large_binary()),
                ),
            ],
            "fixed_size_binary_16": [(pa.array(["hello"], pa.large_string()), pa.lib.ArrowInvalid)],
            # large_string -> decimal
            "decimal128": [
                (
                    pa.array(["0", "1.5", None], pa.large_string()),
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal128(38, 10)),
                ),
            ],
            "decimal256": [
                (
                    pa.array(["0", "1.5", None], pa.large_string()),
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal256(76, 10)),
                ),
            ],
            # large_string -> temporal
            "date32": [
                (
                    pa.array(["2022-01-01", None], pa.large_string()),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array(["2022-01-01", None], pa.large_string()),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            "timestamp_s": [
                (
                    pa.array(["2022-01-01 00:00:00", None], pa.large_string()),
                    pa.array([1640995200, None], pa.timestamp("s")),
                ),
            ],
            "timestamp_ms": [
                (
                    pa.array(["2022-01-01 00:00:00", None], pa.large_string()),
                    pa.array([1640995200000, None], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array(["2022-01-01 00:00:00", None], pa.large_string()),
                    pa.array([1640995200000000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array(["2022-01-01 00:00:00", None], pa.large_string()),
                    pa.array([1640995200000000000, None], pa.timestamp("ns")),
                ),
            ],
            "timestamp_s_tz": [(pa.array(["2022-01-01"], pa.large_string()), pa.lib.ArrowInvalid)],
            "timestamp_ms_tz": [(pa.array(["2022-01-01"], pa.large_string()), pa.lib.ArrowInvalid)],
            "timestamp_us_tz": [(pa.array(["2022-01-01"], pa.large_string()), pa.lib.ArrowInvalid)],
            "timestamp_ns_tz": [(pa.array(["2022-01-01"], pa.large_string()), pa.lib.ArrowInvalid)],
            "timestamp_s_tz_ny": [
                (pa.array(["2022-01-01"], pa.large_string()), pa.lib.ArrowInvalid)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array(["2022-01-01"], pa.large_string()), pa.lib.ArrowInvalid)
            ],
            "duration_s": [(pa.array(["1"], pa.large_string()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array(["1"], pa.large_string()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array(["1"], pa.large_string()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array(["1"], pa.large_string()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [
                (pa.array(["12:00:00"], pa.large_string()), pa.lib.ArrowNotImplementedError)
            ],
            "time32_ms": [
                (pa.array(["12:00:00"], pa.large_string()), pa.lib.ArrowNotImplementedError)
            ],
            "time64_us": [
                (pa.array(["12:00:00"], pa.large_string()), pa.lib.ArrowNotImplementedError)
            ],
            "time64_ns": [
                (pa.array(["12:00:00"], pa.large_string()), pa.lib.ArrowNotImplementedError)
            ],
        }
        self._run_cast_tests(casts, "large_string")

    # ============================================================
    # binary casts
    # ============================================================
    def test_binary_casts(self):
        """Test binary -> all scalar types."""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            # binary -> integers (not supported)
            "int8": [(pa.array([b"\x00", b"\x01"], pa.binary()), pa.lib.ArrowInvalid)],
            "int16": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            "int32": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            "int64": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            "uint8": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            "uint16": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            "uint32": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            "uint64": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            # binary -> floats (parses as string, fails for non-numeric)
            "float16": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            "float32": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            "float64": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            # binary -> bool (parses as string, fails)
            "bool": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowInvalid)],
            # binary -> string
            "string": [
                (
                    pa.array([b"hello", b"world", None], pa.binary()),
                    pa.array(["hello", "world", None], pa.string()),
                ),
                # Empty binary -> empty string
                (pa.array([b"", None], pa.binary()), pa.array(["", None], pa.string())),
                # UTF-8 encoded Unicode in binary -> string (valid UTF-8)
                (
                    pa.array(["你好".encode("utf-8"), None], pa.binary()),
                    pa.array(["你好", None], pa.string()),
                ),
                (pa.array(["😀".encode("utf-8")], pa.binary()), pa.array(["😀"], pa.string())),
                # Invalid UTF-8 sequences -> ArrowInvalid
                (pa.array([b"\xff\xfe"], pa.binary()), pa.lib.ArrowInvalid),
                (pa.array([b"\x80\x81\x82"], pa.binary()), pa.lib.ArrowInvalid),
            ],
            "large_string": [
                (
                    pa.array([b"hello", None], pa.binary()),
                    pa.array(["hello", None], pa.large_string()),
                ),
                # UTF-8 encoded Unicode
                (
                    pa.array(["世界".encode("utf-8"), None], pa.binary()),
                    pa.array(["世界", None], pa.large_string()),
                ),
            ],
            # binary -> binary
            "binary": [
                (pa.array([b"hello", None], pa.binary()), pa.array([b"hello", None], pa.binary())),
                # Empty binary
                (pa.array([b"", None], pa.binary()), pa.array([b"", None], pa.binary())),
                # Binary with null bytes (preserved)
                (
                    pa.array([b"\x00\x01\x02", None], pa.binary()),
                    pa.array([b"\x00\x01\x02", None], pa.binary()),
                ),
            ],
            "large_binary": [
                (
                    pa.array([b"hello", None], pa.binary()),
                    pa.array([b"hello", None], pa.large_binary()),
                ),
            ],
            "fixed_size_binary_16": [
                (pa.array([b"hello"], pa.binary()), pa.lib.ArrowInvalid),  # wrong size
            ],
            # binary -> decimal (parses as string)
            "decimal128": [
                (
                    pa.array([b"0", b"1.5", None], pa.binary()),
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal128(38, 10)),
                ),
            ],
            "decimal256": [
                (
                    pa.array([b"0", None], pa.binary()),
                    pa.array([Decimal("0"), None], pa.decimal256(76, 10)),
                ),
            ],
            # binary -> temporal (not supported)
            "date32": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [
                (pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([b"\x00"], pa.binary()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "binary")

    def test_large_binary_casts(self):
        """Test large_binary -> all scalar types."""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            # large_binary -> integers (not supported)
            "int8": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            "int16": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            "int32": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            "int64": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            "uint8": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            "uint16": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            "uint32": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            "uint64": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            # large_binary -> floats (parses as string, fails)
            "float16": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            "float32": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            "float64": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            # large_binary -> bool (parses as string, fails)
            "bool": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowInvalid)],
            # large_binary -> string
            "string": [
                (
                    pa.array([b"hello", None], pa.large_binary()),
                    pa.array(["hello", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([b"hello", None], pa.large_binary()),
                    pa.array(["hello", None], pa.large_string()),
                ),
            ],
            # large_binary -> binary
            "binary": [
                (
                    pa.array([b"hello", None], pa.large_binary()),
                    pa.array([b"hello", None], pa.binary()),
                ),
            ],
            "large_binary": [
                (
                    pa.array([b"hello", None], pa.large_binary()),
                    pa.array([b"hello", None], pa.large_binary()),
                ),
            ],
            "fixed_size_binary_16": [
                (pa.array([b"hello"], pa.large_binary()), pa.lib.ArrowInvalid)
            ],
            # large_binary -> decimal (parses as string)
            "decimal128": [
                (
                    pa.array([b"0", None], pa.large_binary()),
                    pa.array([Decimal("0"), None], pa.decimal128(38, 10)),
                ),
            ],
            "decimal256": [
                (
                    pa.array([b"0", None], pa.large_binary()),
                    pa.array([Decimal("0"), None], pa.decimal256(76, 10)),
                ),
            ],
            # large_binary -> temporal (not supported)
            "date32": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms_tz": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ms": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_us": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ns": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "time32_s": [(pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "time64_us": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
            "time64_ns": [
                (pa.array([b"\x00"], pa.large_binary()), pa.lib.ArrowNotImplementedError)
            ],
        }
        self._run_cast_tests(casts, "large_binary")

    def test_fixed_size_binary_16_casts(self):
        """Test fixed_size_binary(16) -> all scalar types."""
        import pyarrow as pa

        # 16-byte binary values
        val16 = b"0123456789abcdef"

        casts = {
            # fixed_size_binary -> integers (not supported)
            "int8": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "int64": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "uint8": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            # fixed_size_binary -> floats (not supported)
            "float16": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            # fixed_size_binary -> bool (not supported)
            "bool": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            # fixed_size_binary -> string
            "string": [
                (
                    pa.array([val16, None], pa.binary(16)),
                    pa.array([val16.decode(), None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([val16, None], pa.binary(16)),
                    pa.array([val16.decode(), None], pa.large_string()),
                ),
            ],
            # fixed_size_binary -> binary
            "binary": [
                (pa.array([val16, None], pa.binary(16)), pa.array([val16, None], pa.binary())),
            ],
            "large_binary": [
                (
                    pa.array([val16, None], pa.binary(16)),
                    pa.array([val16, None], pa.large_binary()),
                ),
            ],
            "fixed_size_binary_16": [
                (pa.array([val16, None], pa.binary(16)), pa.array([val16, None], pa.binary(16))),
            ],
            # fixed_size_binary -> decimal (not supported)
            "decimal128": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "decimal256": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            # fixed_size_binary -> temporal (not supported)
            "date32": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "timestamp_s": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [
                (pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "time32_s": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([val16], pa.binary(16)), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "fixed_size_binary_16")

    # ============================================================
    # decimal casts
    # ============================================================
    def test_decimal128_casts(self):
        """Test decimal128 -> all scalar types."""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            # decimal128 -> integers
            "int8": [
                (
                    pa.array(
                        [Decimal("0"), Decimal("1"), Decimal("-1"), None], pa.decimal128(38, 10)
                    ),
                    pa.array([0, 1, -1, None], pa.int8()),
                ),
                (
                    pa.array([Decimal("1.5")], pa.decimal128(38, 10)),
                    pa.lib.ArrowInvalid,
                ),  # fractional
                # Boundary: int8 max/min
                (
                    pa.array([Decimal("127"), Decimal("-128")], pa.decimal128(38, 10)),
                    pa.array([127, -128], pa.int8()),
                ),
                # Overflow: beyond int8 range
                (pa.array([Decimal("128")], pa.decimal128(38, 10)), pa.lib.ArrowInvalid),
            ],
            "int16": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal128(38, 10)),
                    pa.array([0, 1, None], pa.int16()),
                ),
            ],
            "int32": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal128(38, 10)),
                    pa.array([0, 1, None], pa.int32()),
                ),
            ],
            "int64": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal128(38, 10)),
                    pa.array([0, 1, None], pa.int64()),
                ),
                # Large decimal values
                (
                    pa.array([Decimal("9223372036854775807")], pa.decimal128(38, 0)),
                    pa.array([9223372036854775807], pa.int64()),
                ),  # int64 max
                # Overflow: int64 max + 1
                (
                    pa.array([Decimal("9223372036854775808")], pa.decimal128(38, 0)),
                    pa.lib.ArrowInvalid,
                ),
            ],
            "uint8": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal128(38, 10)),
                    pa.array([0, 1, None], pa.uint8()),
                ),
            ],
            "uint16": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal128(38, 10)),
                    pa.array([0, 1, None], pa.uint16()),
                ),
            ],
            "uint32": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal128(38, 10)),
                    pa.array([0, 1, None], pa.uint32()),
                ),
            ],
            "uint64": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal128(38, 10)),
                    pa.array([0, 1, None], pa.uint64()),
                ),
            ],
            # decimal128 -> floats
            "float16": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "float32": [
                (
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal128(38, 10)),
                    pa.array([0.0, 1.5, None], pa.float32()),
                ),
            ],
            "float64": [
                (
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal128(38, 10)),
                    pa.array([0.0, 1.5, None], pa.float64()),
                ),
            ],
            # decimal128 -> bool (not supported)
            "bool": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            # decimal128 -> string
            "string": [
                (
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal128(38, 10)),
                    pa.array(["0E-10", "1.5000000000", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([Decimal("0"), None], pa.decimal128(38, 10)),
                    pa.array(["0E-10", None], pa.large_string()),
                ),
            ],
            # decimal128 -> binary (not supported)
            "binary": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "large_binary": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            # decimal128 -> decimal
            "decimal128": [
                (
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal128(38, 10)),
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal128(38, 10)),
                ),
            ],
            "decimal256": [
                (
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal128(38, 10)),
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal256(76, 10)),
                ),
            ],
            # decimal128 -> temporal (not supported)
            "date32": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "date64": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms_tz": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ms": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "duration_us": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ns": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "time32_s": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "time32_ms": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "time64_us": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "time64_ns": [
                (pa.array([Decimal("0")], pa.decimal128(38, 10)), pa.lib.ArrowNotImplementedError)
            ],
        }
        self._run_cast_tests(casts, "decimal128")

    def test_decimal256_casts(self):
        """Test decimal256 -> all scalar types."""
        import pyarrow as pa
        from decimal import Decimal

        casts = {
            # decimal256 -> integers
            "int8": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal256(76, 10)),
                    pa.array([0, 1, None], pa.int8()),
                ),
            ],
            "int16": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal256(76, 10)),
                    pa.array([0, 1, None], pa.int16()),
                ),
            ],
            "int32": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal256(76, 10)),
                    pa.array([0, 1, None], pa.int32()),
                ),
            ],
            "int64": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal256(76, 10)),
                    pa.array([0, 1, None], pa.int64()),
                ),
            ],
            "uint8": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal256(76, 10)),
                    pa.array([0, 1, None], pa.uint8()),
                ),
            ],
            "uint16": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal256(76, 10)),
                    pa.array([0, 1, None], pa.uint16()),
                ),
            ],
            "uint32": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal256(76, 10)),
                    pa.array([0, 1, None], pa.uint32()),
                ),
            ],
            "uint64": [
                (
                    pa.array([Decimal("0"), Decimal("1"), None], pa.decimal256(76, 10)),
                    pa.array([0, 1, None], pa.uint64()),
                ),
            ],
            # decimal256 -> floats
            "float16": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "float32": [
                (
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal256(76, 10)),
                    pa.array([0.0, 1.5, None], pa.float32()),
                ),
            ],
            "float64": [
                (
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal256(76, 10)),
                    pa.array([0.0, 1.5, None], pa.float64()),
                ),
            ],
            # decimal256 -> bool (not supported)
            "bool": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            # decimal256 -> string
            "string": [
                (
                    pa.array([Decimal("0"), None], pa.decimal256(76, 10)),
                    pa.array(["0E-10", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([Decimal("0"), None], pa.decimal256(76, 10)),
                    pa.array(["0E-10", None], pa.large_string()),
                ),
            ],
            # decimal256 -> binary (not supported)
            "binary": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "large_binary": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            # decimal256 -> decimal
            "decimal128": [
                (
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal256(76, 10)),
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal128(38, 10)),
                ),
            ],
            "decimal256": [
                (
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal256(76, 10)),
                    pa.array([Decimal("0"), Decimal("1.5"), None], pa.decimal256(76, 10)),
                ),
            ],
            # decimal256 -> temporal (not supported)
            "date32": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "date64": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms_tz": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "duration_s": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ms": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "duration_us": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ns": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "time32_s": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "time32_ms": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "time64_us": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
            "time64_ns": [
                (pa.array([Decimal("0")], pa.decimal256(76, 10)), pa.lib.ArrowNotImplementedError)
            ],
        }
        self._run_cast_tests(casts, "decimal256")

    # ============================================================
    # date casts
    # ============================================================
    def test_date32_casts(self):
        """Test date32 -> all scalar types."""
        import pyarrow as pa
        import datetime

        d = datetime.date(2022, 1, 1)

        # 2022-01-01 = 18993 days since 1970-01-01 (not 19358)
        days_since_epoch = 18993

        casts = {
            # date32 -> integers (only int32 supported - same storage type)
            "int8": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "int32": [
                (pa.array([d, None], pa.date32()), pa.array([days_since_epoch, None], pa.int32())),
            ],
            "int64": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "uint8": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            # date32 -> floats (not supported)
            "float16": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            # date32 -> bool (not supported)
            "bool": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            # date32 -> string
            "string": [
                (pa.array([d, None], pa.date32()), pa.array(["2022-01-01", None], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([d, None], pa.date32()),
                    pa.array(["2022-01-01", None], pa.large_string()),
                ),
            ],
            # date32 -> binary (not supported)
            "binary": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            # date32 -> decimal (not supported)
            "decimal128": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "decimal256": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            # date32 -> date
            "date32": [
                (pa.array([d, None], pa.date32()), pa.array([d, None], pa.date32())),
            ],
            "date64": [
                (pa.array([d, None], pa.date32()), pa.array([d, None], pa.date64())),
            ],
            # date32 -> timestamp
            "timestamp_s": [
                (pa.array([d, None], pa.date32()), pa.array([1640995200, None], pa.timestamp("s"))),
            ],
            "timestamp_ms": [
                (
                    pa.array([d, None], pa.date32()),
                    pa.array([1640995200000, None], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array([d, None], pa.date32()),
                    pa.array([1640995200000000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([d, None], pa.date32()),
                    pa.array([1640995200000000000, None], pa.timestamp("ns")),
                ),
            ],
            "timestamp_s_tz": [
                (
                    pa.array([d, None], pa.date32()),
                    pa.array([1640995200, None], pa.timestamp("s", tz="UTC")),
                ),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([d, None], pa.date32()),
                    pa.array([1640995200000, None], pa.timestamp("ms", tz="UTC")),
                ),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([d, None], pa.date32()),
                    pa.array([1640995200000000, None], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([d, None], pa.date32()),
                    pa.array([1640995200000000000, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([d, None], pa.date32()),
                    pa.array([1640995200, None], pa.timestamp("s", tz="America/New_York")),
                ),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([d, None], pa.date32()),
                    pa.array([1640995200, None], pa.timestamp("s", tz="Asia/Shanghai")),
                ),
            ],
            # date32 -> duration (not supported)
            "duration_s": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            # date32 -> time (not supported)
            "time32_s": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([d], pa.date32()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "date32")

    def test_date64_casts(self):
        """Test date64 -> all scalar types."""
        import pyarrow as pa
        import datetime

        d = datetime.date(2022, 1, 1)

        # 2022-01-01 00:00:00 UTC = 1640995200000 ms since epoch
        ms_since_epoch = 1640995200000

        casts = {
            # date64 -> integers (only int64 supported - same storage type)
            "int8": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "int64": [
                (pa.array([d, None], pa.date64()), pa.array([ms_since_epoch, None], pa.int64())),
            ],
            "uint8": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            # date64 -> floats (not supported)
            "float16": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            # date64 -> bool (not supported)
            "bool": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            # date64 -> string
            "string": [
                (pa.array([d, None], pa.date64()), pa.array(["2022-01-01", None], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([d, None], pa.date64()),
                    pa.array(["2022-01-01", None], pa.large_string()),
                ),
            ],
            # date64 -> binary (not supported)
            "binary": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            # date64 -> decimal (not supported)
            "decimal128": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "decimal256": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            # date64 -> date
            "date32": [
                (pa.array([d, None], pa.date64()), pa.array([d, None], pa.date32())),
            ],
            "date64": [
                (pa.array([d, None], pa.date64()), pa.array([d, None], pa.date64())),
            ],
            # date64 -> timestamp
            "timestamp_s": [
                (pa.array([d, None], pa.date64()), pa.array([1640995200, None], pa.timestamp("s"))),
            ],
            "timestamp_ms": [
                (
                    pa.array([d, None], pa.date64()),
                    pa.array([1640995200000, None], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array([d, None], pa.date64()),
                    pa.array([1640995200000000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([d, None], pa.date64()),
                    pa.array([1640995200000000000, None], pa.timestamp("ns")),
                ),
            ],
            "timestamp_s_tz": [
                (
                    pa.array([d, None], pa.date64()),
                    pa.array([1640995200, None], pa.timestamp("s", tz="UTC")),
                ),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([d, None], pa.date64()),
                    pa.array([1640995200000, None], pa.timestamp("ms", tz="UTC")),
                ),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([d, None], pa.date64()),
                    pa.array([1640995200000000, None], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([d, None], pa.date64()),
                    pa.array([1640995200000000000, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([d, None], pa.date64()),
                    pa.array([1640995200, None], pa.timestamp("s", tz="America/New_York")),
                ),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([d, None], pa.date64()),
                    pa.array([1640995200, None], pa.timestamp("s", tz="Asia/Shanghai")),
                ),
            ],
            # date64 -> duration (not supported)
            "duration_s": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            # date64 -> time (not supported)
            "time32_s": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([d], pa.date64()), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "date64")

    def test_timestamp_s_casts(self):
        """Test timestamp[s] -> all scalar types."""
        import pyarrow as pa
        import datetime

        ts = datetime.datetime(2022, 1, 1, 12, 30, 45)
        epoch_s = 1641040245  # seconds since epoch

        # Boundary timestamps for additional coverage
        unix_epoch = datetime.datetime(1970, 1, 1, 0, 0, 0)  # Unix epoch
        before_epoch = datetime.datetime(1969, 12, 31, 23, 59, 59)  # 1 second before epoch
        before_epoch_s = -1  # negative timestamp

        casts = {
            # timestamp[s] -> integers (only int64 supported)
            "int8": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "int64": [
                (pa.array([ts, None], pa.timestamp("s")), pa.array([epoch_s, None], pa.int64())),
                # Unix epoch boundary
                (pa.array([unix_epoch], pa.timestamp("s")), pa.array([0], pa.int64())),
                # Negative timestamp (before 1970)
                (
                    pa.array([before_epoch], pa.timestamp("s")),
                    pa.array([before_epoch_s], pa.int64()),
                ),
            ],
            "uint8": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            # timestamp[s] -> floats (not supported)
            "float16": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            # timestamp[s] -> bool (not supported)
            "bool": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            # timestamp[s] -> string
            "string": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array(["2022-01-01 12:30:45", None], pa.string()),
                ),
                # Unix epoch boundary
                (
                    pa.array([unix_epoch], pa.timestamp("s")),
                    pa.array(["1970-01-01 00:00:00"], pa.string()),
                ),
                # Negative timestamp (before 1970)
                (
                    pa.array([before_epoch], pa.timestamp("s")),
                    pa.array(["1969-12-31 23:59:59"], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array(["2022-01-01 12:30:45", None], pa.large_string()),
                ),
            ],
            # timestamp[s] -> binary (not supported)
            "binary": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [
                (pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[s] -> decimal (not supported)
            "decimal128": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "decimal256": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            # timestamp[s] -> date
            "date32": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            # timestamp[s] -> timestamp (conversions between units)
            "timestamp_s": [
                (pa.array([ts, None], pa.timestamp("s")), pa.array([ts, None], pa.timestamp("s"))),
            ],
            "timestamp_ms": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([epoch_s * 1000, None], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([epoch_s * 1000000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([epoch_s * 1000000000, None], pa.timestamp("ns")),
                ),
            ],
            # timestamp[s] -> timestamp with tz (adds timezone)
            "timestamp_s_tz": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                ),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([epoch_s * 1000, None], pa.timestamp("ms", tz="UTC")),
                ),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([epoch_s * 1000000, None], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([epoch_s * 1000000000, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                ),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                ),
            ],
            # timestamp[s] -> duration (not supported)
            "duration_s": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([ts], pa.timestamp("s")), pa.lib.ArrowNotImplementedError)],
            # timestamp[s] -> time
            "time32_s": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("s")),
                ),
            ],
            "time32_ms": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("ms")),
                ),
            ],
            "time64_us": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time64("us")),
                ),
            ],
            "time64_ns": [
                (
                    pa.array([ts, None], pa.timestamp("s")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time64("ns")),
                ),
            ],
        }
        self._run_cast_tests(casts, "timestamp_s")

    def test_timestamp_ms_casts(self):
        """Test timestamp[ms] -> all scalar types."""
        import pyarrow as pa
        import datetime

        ts_lossy = datetime.datetime(2022, 1, 1, 12, 30, 45, 123000)  # 123ms
        ts_lossless = datetime.datetime(2022, 1, 1, 12, 30, 45)  # no sub-second
        epoch_ms_lossy = 1641040245123
        epoch_ms_lossless = 1641040245000

        casts = {
            # timestamp[ms] -> integers (only int64 supported)
            "int8": [(pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)],
            "int64": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([epoch_ms_lossy, None], pa.int64()),
                ),
            ],
            "uint8": [(pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)],
            # timestamp[ms] -> floats (not supported)
            "float16": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "float32": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "float64": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[ms] -> bool (not supported)
            "bool": [(pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)],
            # timestamp[ms] -> string
            "string": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array(["2022-01-01 12:30:45.123", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array(["2022-01-01 12:30:45.123", None], pa.large_string()),
                ),
            ],
            # timestamp[ms] -> binary (not supported)
            "binary": [(pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[ms] -> decimal (not supported)
            "decimal128": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "decimal256": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[ms] -> date
            "date32": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            # timestamp[ms] -> timestamp (lossy to lower precision)
            "timestamp_s": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("ms")),
                    pa.array([epoch_ms_lossless // 1000, None], pa.timestamp("s")),
                ),
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([epoch_ms_lossy * 1000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([epoch_ms_lossy * 1000000, None], pa.timestamp("ns")),
                ),
            ],
            # timestamp[ms] -> timestamp with tz
            "timestamp_s_tz": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("ms")),
                    pa.array([epoch_ms_lossless // 1000, None], pa.timestamp("s", tz="UTC")),
                ),
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                ),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([epoch_ms_lossy * 1000, None], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([epoch_ms_lossy * 1000000, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("ms")),
                    pa.array(
                        [epoch_ms_lossless // 1000, None], pa.timestamp("s", tz="America/New_York")
                    ),
                ),
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowInvalid),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("ms")),
                    pa.array(
                        [epoch_ms_lossless // 1000, None], pa.timestamp("s", tz="Asia/Shanghai")
                    ),
                ),
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowInvalid),
            ],
            # timestamp[ms] -> duration (not supported)
            "duration_s": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ms": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_us": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ns": [
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[ms] -> time
            "time32_s": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("ms")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("s")),
                ),
                (pa.array([ts_lossy], pa.timestamp("ms")), pa.lib.ArrowInvalid),
            ],
            "time32_ms": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([datetime.time(12, 30, 45, 123000), None], pa.time32("ms")),
                ),
            ],
            "time64_us": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([datetime.time(12, 30, 45, 123000), None], pa.time64("us")),
                ),
            ],
            "time64_ns": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("ms")),
                    pa.array([datetime.time(12, 30, 45, 123000), None], pa.time64("ns")),
                ),
            ],
        }
        self._run_cast_tests(casts, "timestamp_ms")

    def test_timestamp_us_casts(self):
        """Test timestamp[us] -> all scalar types."""
        import pyarrow as pa
        import datetime

        ts_lossy = datetime.datetime(2022, 1, 1, 12, 30, 45, 123456)  # has us precision
        ts_lossless = datetime.datetime(2022, 1, 1, 12, 30, 45)  # no sub-second
        epoch_us_lossy = 1641040245123456
        epoch_us_lossless = 1641040245000000

        casts = {
            # timestamp[us] -> integers (only int64 supported)
            "int8": [(pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)],
            "int64": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array([epoch_us_lossy, None], pa.int64()),
                ),
            ],
            "uint8": [(pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)],
            # timestamp[us] -> floats (not supported)
            "float16": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            "float32": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            "float64": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[us] -> bool (not supported)
            "bool": [(pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)],
            # timestamp[us] -> string
            "string": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array(["2022-01-01 12:30:45.123456", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array(["2022-01-01 12:30:45.123456", None], pa.large_string()),
                ),
            ],
            # timestamp[us] -> binary (not supported)
            "binary": [(pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[us] -> decimal (not supported)
            "decimal128": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            "decimal256": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[us] -> date
            "date32": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            # timestamp[us] -> timestamp (lossy to lower precision)
            "timestamp_s": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("us")),
                    pa.array([epoch_us_lossless // 1000000, None], pa.timestamp("s")),
                ),
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("us")),
                    pa.array([epoch_us_lossless // 1000, None], pa.timestamp("ms")),
                ),
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowInvalid),
            ],
            "timestamp_us": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array([epoch_us_lossy * 1000, None], pa.timestamp("ns")),
                ),
            ],
            # timestamp[us] -> timestamp with tz
            "timestamp_s_tz": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("us")),
                    pa.array([epoch_us_lossless // 1000000, None], pa.timestamp("s", tz="UTC")),
                ),
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("us")),
                    pa.array([epoch_us_lossless // 1000, None], pa.timestamp("ms", tz="UTC")),
                ),
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowInvalid),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array([epoch_us_lossy * 1000, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("us")),
                    pa.array(
                        [epoch_us_lossless // 1000000, None],
                        pa.timestamp("s", tz="America/New_York"),
                    ),
                ),
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowInvalid),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("us")),
                    pa.array(
                        [epoch_us_lossless // 1000000, None], pa.timestamp("s", tz="Asia/Shanghai")
                    ),
                ),
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowInvalid),
            ],
            # timestamp[us] -> duration (not supported)
            "duration_s": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ms": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_us": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ns": [
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[us] -> time
            "time32_s": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("us")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("s")),
                ),
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowInvalid),
            ],
            "time32_ms": [
                (
                    pa.array([ts_lossless, None], pa.timestamp("us")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("ms")),
                ),
                (pa.array([ts_lossy], pa.timestamp("us")), pa.lib.ArrowInvalid),
            ],
            "time64_us": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array([datetime.time(12, 30, 45, 123456), None], pa.time64("us")),
                ),
            ],
            "time64_ns": [
                (
                    pa.array([ts_lossy, None], pa.timestamp("us")),
                    pa.array([datetime.time(12, 30, 45, 123456), None], pa.time64("ns")),
                ),
            ],
        }
        self._run_cast_tests(casts, "timestamp_us")

    def test_timestamp_ns_casts(self):
        """Test timestamp[ns] -> all scalar types."""
        import pyarrow as pa
        import datetime

        # Use a value with no sub-second precision for lossless casts
        epoch_ns_lossless = 1641040245000000000  # ns since epoch (no sub-ns)
        # Use value with ns precision for lossy cast tests
        epoch_ns_lossy = 1641040245123456789  # ns since epoch (with ns precision)

        casts = {
            # timestamp[ns] -> integers (only int64 supported)
            "int8": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "int16": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "int32": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "int64": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns")),
                    pa.array([epoch_ns_lossy, None], pa.int64()),
                ),
            ],
            "uint8": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "uint16": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "uint32": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "uint64": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[ns] -> floats (not supported)
            "float16": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "float32": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "float64": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[ns] -> bool (not supported)
            "bool": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[ns] -> string
            "string": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns")),
                    pa.array(["2022-01-01 12:30:45.123456789", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns")),
                    pa.array(["2022-01-01 12:30:45.123456789", None], pa.large_string()),
                ),
            ],
            # timestamp[ns] -> binary (not supported)
            "binary": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "large_binary": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[ns] -> decimal (not supported)
            "decimal128": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "decimal256": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[ns] -> date
            "date32": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            # timestamp[ns] -> timestamp (loses precision with safe=True)
            "timestamp_s": [
                # Lossless conversion when no sub-second data
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array([epoch_ns_lossless // 1000000000, None], pa.timestamp("s")),
                ),
                # Lossy conversion fails with safe=True
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array([epoch_ns_lossless // 1000000, None], pa.timestamp("ms")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            "timestamp_us": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array([epoch_ns_lossless // 1000, None], pa.timestamp("us")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ns": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns")),
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns")),
                ),
            ],
            # timestamp[ns] -> timestamp with tz (loses precision with safe=True)
            "timestamp_s_tz": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array([epoch_ns_lossless // 1000000000, None], pa.timestamp("s", tz="UTC")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array([epoch_ns_lossless // 1000000, None], pa.timestamp("ms", tz="UTC")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array([epoch_ns_lossless // 1000, None], pa.timestamp("us", tz="UTC")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns")),
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array(
                        [epoch_ns_lossless // 1000000000, None],
                        pa.timestamp("s", tz="America/New_York"),
                    ),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array(
                        [epoch_ns_lossless // 1000000000, None],
                        pa.timestamp("s", tz="Asia/Shanghai"),
                    ),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            # timestamp[ns] -> duration (not supported)
            "duration_s": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ms": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_us": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ns": [
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[ns] -> time (loses precision with safe=True)
            "time32_s": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("s")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            "time32_ms": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("ms")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            "time64_us": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time64("us")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns")), pa.lib.ArrowInvalid),
            ],
            "time64_ns": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns")),
                    pa.array([datetime.time(12, 30, 45, 123456), None], pa.time64("ns")),
                ),
            ],
        }
        self._run_cast_tests(casts, "timestamp_ns")

    def test_timestamp_s_tz_casts(self):
        """Test timestamp[s, tz=UTC] -> all scalar types."""
        import pyarrow as pa
        import datetime

        epoch_s = 1641040245

        casts = {
            # timestamp[s,tz] -> integers (only int64 supported)
            "int8": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "int16": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "int32": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "int64": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s, None], pa.int64()),
                ),
            ],
            "uint8": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "uint16": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "uint32": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "uint64": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[s,tz] -> floats (not supported)
            "float16": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "float32": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "float64": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[s,tz] -> bool (not supported)
            "bool": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[s,tz] -> string
            "string": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array(["2022-01-01 12:30:45Z", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array(["2022-01-01 12:30:45Z", None], pa.large_string()),
                ),
            ],
            # timestamp[s,tz] -> binary (not supported)
            "binary": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "large_binary": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[s,tz] -> decimal (not supported)
            "decimal128": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "decimal256": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[s,tz] -> date
            "date32": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            # timestamp[s,tz] -> timestamp (strips tz)
            "timestamp_s": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s, None], pa.timestamp("s")),
                ),
            ],
            "timestamp_ms": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s * 1000, None], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s * 1000000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s * 1000000000, None], pa.timestamp("ns")),
                ),
            ],
            # timestamp[s,tz] -> timestamp with tz (same or different tz)
            "timestamp_s_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                ),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s * 1000, None], pa.timestamp("ms", tz="UTC")),
                ),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s * 1000000, None], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s * 1000000000, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                ),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                ),
            ],
            # timestamp[s,tz] -> duration (not supported)
            "duration_s": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ms": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_us": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            "duration_ns": [
                (pa.array([epoch_s], pa.timestamp("s", tz="UTC")), pa.lib.ArrowNotImplementedError)
            ],
            # timestamp[s,tz] -> time
            "time32_s": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("s")),
                ),
            ],
            "time32_ms": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("ms")),
                ),
            ],
            "time64_us": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time64("us")),
                ),
            ],
            "time64_ns": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time64("ns")),
                ),
            ],
        }
        self._run_cast_tests(casts, "timestamp_s_tz")

    def test_timestamp_ms_tz_casts(self):
        """Test timestamp[ms, tz=UTC] -> all scalar types."""
        import pyarrow as pa
        import datetime

        epoch_ms_lossy = 1641040245123
        epoch_ms_lossless = 1641040245000

        casts = {
            # timestamp[ms,tz] -> integers (only int64 supported)
            "int8": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int16": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int32": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int64": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([epoch_ms_lossy, None], pa.int64()),
                ),
            ],
            "uint8": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint16": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint32": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint64": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ms,tz] -> floats (not supported)
            "float16": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "float32": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "float64": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ms,tz] -> bool (not supported)
            "bool": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ms,tz] -> string
            "string": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array(["2022-01-01 12:30:45.123Z", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array(["2022-01-01 12:30:45.123Z", None], pa.large_string()),
                ),
            ],
            # timestamp[ms,tz] -> binary (not supported)
            "binary": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "large_binary": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "fixed_size_binary_16": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ms,tz] -> decimal (not supported)
            "decimal128": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "decimal256": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ms,tz] -> date
            "date32": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            # timestamp[ms,tz] -> timestamp (lossy to lower precision)
            "timestamp_s": [
                (
                    pa.array([epoch_ms_lossless, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([epoch_ms_lossless // 1000, None], pa.timestamp("s")),
                ),
                (pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([epoch_ms_lossy * 1000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([epoch_ms_lossy * 1000000, None], pa.timestamp("ns")),
                ),
            ],
            # timestamp[ms,tz] -> timestamp with tz
            "timestamp_s_tz": [
                (
                    pa.array([epoch_ms_lossless, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([epoch_ms_lossless // 1000, None], pa.timestamp("s", tz="UTC")),
                ),
                (pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                ),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([epoch_ms_lossy * 1000, None], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([epoch_ms_lossy * 1000000, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([epoch_ms_lossless, None], pa.timestamp("ms", tz="UTC")),
                    pa.array(
                        [epoch_ms_lossless // 1000, None], pa.timestamp("s", tz="America/New_York")
                    ),
                ),
                (pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([epoch_ms_lossless, None], pa.timestamp("ms", tz="UTC")),
                    pa.array(
                        [epoch_ms_lossless // 1000, None], pa.timestamp("s", tz="Asia/Shanghai")
                    ),
                ),
                (pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            # timestamp[ms,tz] -> duration (not supported)
            "duration_s": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_ms": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_us": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_ns": [
                (
                    pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ms,tz] -> time
            "time32_s": [
                (
                    pa.array([epoch_ms_lossless, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("s")),
                ),
                (pa.array([epoch_ms_lossy], pa.timestamp("ms", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "time32_ms": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45, 123000), None], pa.time32("ms")),
                ),
            ],
            "time64_us": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45, 123000), None], pa.time64("us")),
                ),
            ],
            "time64_ns": [
                (
                    pa.array([epoch_ms_lossy, None], pa.timestamp("ms", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45, 123000), None], pa.time64("ns")),
                ),
            ],
        }
        self._run_cast_tests(casts, "timestamp_ms_tz")

    def test_timestamp_us_tz_casts(self):
        """Test timestamp[us, tz=UTC] -> all scalar types."""
        import pyarrow as pa
        import datetime

        epoch_us_lossy = 1641040245123456
        epoch_us_lossless = 1641040245000000

        casts = {
            # timestamp[us,tz] -> integers (only int64 supported)
            "int8": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int16": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int32": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int64": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array([epoch_us_lossy, None], pa.int64()),
                ),
            ],
            "uint8": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint16": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint32": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint64": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[us,tz] -> floats (not supported)
            "float16": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "float32": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "float64": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[us,tz] -> bool (not supported)
            "bool": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[us,tz] -> string
            "string": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array(["2022-01-01 12:30:45.123456Z", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array(["2022-01-01 12:30:45.123456Z", None], pa.large_string()),
                ),
            ],
            # timestamp[us,tz] -> binary (not supported)
            "binary": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "large_binary": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "fixed_size_binary_16": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[us,tz] -> decimal (not supported)
            "decimal128": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "decimal256": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[us,tz] -> date
            "date32": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            # timestamp[us,tz] -> timestamp (lossy to lower precision)
            "timestamp_s": [
                (
                    pa.array([epoch_us_lossless, None], pa.timestamp("us", tz="UTC")),
                    pa.array([epoch_us_lossless // 1000000, None], pa.timestamp("s")),
                ),
                (pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms": [
                (
                    pa.array([epoch_us_lossless, None], pa.timestamp("us", tz="UTC")),
                    pa.array([epoch_us_lossless // 1000, None], pa.timestamp("ms")),
                ),
                (pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_us": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array([epoch_us_lossy, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array([epoch_us_lossy * 1000, None], pa.timestamp("ns")),
                ),
            ],
            # timestamp[us,tz] -> timestamp with tz
            "timestamp_s_tz": [
                (
                    pa.array([epoch_us_lossless, None], pa.timestamp("us", tz="UTC")),
                    pa.array([epoch_us_lossless // 1000000, None], pa.timestamp("s", tz="UTC")),
                ),
                (pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([epoch_us_lossless, None], pa.timestamp("us", tz="UTC")),
                    pa.array([epoch_us_lossless // 1000, None], pa.timestamp("ms", tz="UTC")),
                ),
                (pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array([epoch_us_lossy * 1000, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([epoch_us_lossless, None], pa.timestamp("us", tz="UTC")),
                    pa.array(
                        [epoch_us_lossless // 1000000, None],
                        pa.timestamp("s", tz="America/New_York"),
                    ),
                ),
                (pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([epoch_us_lossless, None], pa.timestamp("us", tz="UTC")),
                    pa.array(
                        [epoch_us_lossless // 1000000, None], pa.timestamp("s", tz="Asia/Shanghai")
                    ),
                ),
                (pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            # timestamp[us,tz] -> duration (not supported)
            "duration_s": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_ms": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_us": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_ns": [
                (
                    pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[us,tz] -> time
            "time32_s": [
                (
                    pa.array([epoch_us_lossless, None], pa.timestamp("us", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("s")),
                ),
                (pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "time32_ms": [
                (
                    pa.array([epoch_us_lossless, None], pa.timestamp("us", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("ms")),
                ),
                (pa.array([epoch_us_lossy], pa.timestamp("us", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "time64_us": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45, 123456), None], pa.time64("us")),
                ),
            ],
            "time64_ns": [
                (
                    pa.array([epoch_us_lossy, None], pa.timestamp("us", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45, 123456), None], pa.time64("ns")),
                ),
            ],
        }
        self._run_cast_tests(casts, "timestamp_us_tz")

    def test_timestamp_ns_tz_casts(self):
        """Test timestamp[ns, tz=UTC] -> all scalar types."""
        import pyarrow as pa
        import datetime

        epoch_ns_lossy = 1641040245123456789
        epoch_ns_lossless = 1641040245000000000

        casts = {
            # timestamp[ns,tz] -> integers (only int64 supported)
            "int8": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int16": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int32": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int64": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([epoch_ns_lossy, None], pa.int64()),
                ),
            ],
            "uint8": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint16": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint32": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint64": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ns,tz] -> floats (not supported)
            "float16": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "float32": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "float64": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ns,tz] -> bool (not supported)
            "bool": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ns,tz] -> string
            "string": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns", tz="UTC")),
                    pa.array(["2022-01-01 12:30:45.123456789Z", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns", tz="UTC")),
                    pa.array(["2022-01-01 12:30:45.123456789Z", None], pa.large_string()),
                ),
            ],
            # timestamp[ns,tz] -> binary (not supported)
            "binary": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "large_binary": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "fixed_size_binary_16": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ns,tz] -> decimal (not supported)
            "decimal128": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "decimal256": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ns,tz] -> date
            "date32": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            # timestamp[ns,tz] -> timestamp (lossy to lower precision)
            "timestamp_s": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([epoch_ns_lossless // 1000000000, None], pa.timestamp("s")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([epoch_ns_lossless // 1000000, None], pa.timestamp("ms")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_us": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([epoch_ns_lossless // 1000, None], pa.timestamp("us")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ns": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns")),
                ),
            ],
            # timestamp[ns,tz] -> timestamp with tz
            "timestamp_s_tz": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([epoch_ns_lossless // 1000000000, None], pa.timestamp("s", tz="UTC")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([epoch_ns_lossless // 1000000, None], pa.timestamp("ms", tz="UTC")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([epoch_ns_lossless // 1000, None], pa.timestamp("us", tz="UTC")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array(
                        [epoch_ns_lossless // 1000000000, None],
                        pa.timestamp("s", tz="America/New_York"),
                    ),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array(
                        [epoch_ns_lossless // 1000000000, None],
                        pa.timestamp("s", tz="Asia/Shanghai"),
                    ),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            # timestamp[ns,tz] -> duration (not supported)
            "duration_s": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_ms": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_us": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_ns": [
                (
                    pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[ns,tz] -> time (loses ns precision)
            "time32_s": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("s")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "time32_ms": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time32("ms")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "time64_us": [
                (
                    pa.array([epoch_ns_lossless, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45), None], pa.time64("us")),
                ),
                (pa.array([epoch_ns_lossy], pa.timestamp("ns", tz="UTC")), pa.lib.ArrowInvalid),
            ],
            "time64_ns": [
                (
                    pa.array([epoch_ns_lossy, None], pa.timestamp("ns", tz="UTC")),
                    pa.array([datetime.time(12, 30, 45, 123456), None], pa.time64("ns")),
                ),
            ],
        }
        self._run_cast_tests(casts, "timestamp_ns_tz")

    def test_timestamp_s_tz_ny_casts(self):
        """Test timestamp[s, tz=America/New_York] -> all scalar types."""
        import pyarrow as pa
        import datetime

        epoch_s = 1641040245  # UTC epoch

        casts = {
            # timestamp[s,tz_ny] -> integers (only int64 supported)
            "int8": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int16": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int32": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int64": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s, None], pa.int64()),
                ),
            ],
            "uint8": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint16": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint32": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint64": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_ny] -> floats (not supported)
            "float16": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "float32": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "float64": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_ny] -> bool (not supported)
            "bool": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_ny] -> string (shows local time)
            "string": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array(["2022-01-01 07:30:45-0500", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array(["2022-01-01 07:30:45-0500", None], pa.large_string()),
                ),
            ],
            # timestamp[s,tz_ny] -> binary (not supported)
            "binary": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "large_binary": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "fixed_size_binary_16": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_ny] -> decimal (not supported)
            "decimal128": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "decimal256": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_ny] -> date (local date)
            "date32": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            # timestamp[s,tz_ny] -> timestamp NTZ (strips tz)
            "timestamp_s": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s, None], pa.timestamp("s")),
                ),
            ],
            "timestamp_ms": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s * 1000, None], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s * 1000000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s * 1000000000, None], pa.timestamp("ns")),
                ),
            ],
            # timestamp[s,tz_ny] -> timestamp with tz (converts)
            "timestamp_s_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                ),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s * 1000, None], pa.timestamp("ms", tz="UTC")),
                ),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s * 1000000, None], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s * 1000000000, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                ),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                ),
            ],
            # timestamp[s,tz_ny] -> duration (not supported)
            "duration_s": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_ms": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_us": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_ns": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="America/New_York")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_ny] -> time (local time)
            "time32_s": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([datetime.time(7, 30, 45), None], pa.time32("s")),
                ),
            ],
            "time32_ms": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([datetime.time(7, 30, 45), None], pa.time32("ms")),
                ),
            ],
            "time64_us": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([datetime.time(7, 30, 45), None], pa.time64("us")),
                ),
            ],
            "time64_ns": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                    pa.array([datetime.time(7, 30, 45), None], pa.time64("ns")),
                ),
            ],
        }
        self._run_cast_tests(casts, "timestamp_s_tz_ny")

    def test_timestamp_s_tz_shanghai_casts(self):
        """Test timestamp[s, tz=Asia/Shanghai] -> all scalar types."""
        import pyarrow as pa
        import datetime

        epoch_s = 1641040245  # UTC epoch

        casts = {
            # timestamp[s,tz_shanghai] -> integers (only int64 supported)
            "int8": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int16": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int32": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "int64": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s, None], pa.int64()),
                ),
            ],
            "uint8": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint16": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint32": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "uint64": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_shanghai] -> floats (not supported)
            "float16": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "float32": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "float64": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_shanghai] -> bool (not supported)
            "bool": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_shanghai] -> string (shows local time)
            "string": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array(["2022-01-01 20:30:45+0800", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array(["2022-01-01 20:30:45+0800", None], pa.large_string()),
                ),
            ],
            # timestamp[s,tz_shanghai] -> binary (not supported)
            "binary": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "large_binary": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "fixed_size_binary_16": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_shanghai] -> decimal (not supported)
            "decimal128": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "decimal256": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_shanghai] -> date (local date)
            "date32": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date32()),
                ),
            ],
            "date64": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([datetime.date(2022, 1, 1), None], pa.date64()),
                ),
            ],
            # timestamp[s,tz_shanghai] -> timestamp NTZ (strips tz)
            "timestamp_s": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s, None], pa.timestamp("s")),
                ),
            ],
            "timestamp_ms": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s * 1000, None], pa.timestamp("ms")),
                ),
            ],
            "timestamp_us": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s * 1000000, None], pa.timestamp("us")),
                ),
            ],
            "timestamp_ns": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s * 1000000000, None], pa.timestamp("ns")),
                ),
            ],
            # timestamp[s,tz_shanghai] -> timestamp with tz (converts)
            "timestamp_s_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="UTC")),
                ),
            ],
            "timestamp_ms_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s * 1000, None], pa.timestamp("ms", tz="UTC")),
                ),
            ],
            "timestamp_us_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s * 1000000, None], pa.timestamp("us", tz="UTC")),
                ),
            ],
            "timestamp_ns_tz": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s * 1000000000, None], pa.timestamp("ns", tz="UTC")),
                ),
            ],
            "timestamp_s_tz_ny": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="America/New_York")),
                ),
            ],
            "timestamp_s_tz_shanghai": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                ),
            ],
            # timestamp[s,tz_shanghai] -> duration (not supported)
            "duration_s": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_ms": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_us": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            "duration_ns": [
                (
                    pa.array([epoch_s], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.lib.ArrowNotImplementedError,
                )
            ],
            # timestamp[s,tz_shanghai] -> time (local time)
            "time32_s": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([datetime.time(20, 30, 45), None], pa.time32("s")),
                ),
            ],
            "time32_ms": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([datetime.time(20, 30, 45), None], pa.time32("ms")),
                ),
            ],
            "time64_us": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([datetime.time(20, 30, 45), None], pa.time64("us")),
                ),
            ],
            "time64_ns": [
                (
                    pa.array([epoch_s, None], pa.timestamp("s", tz="Asia/Shanghai")),
                    pa.array([datetime.time(20, 30, 45), None], pa.time64("ns")),
                ),
            ],
        }
        self._run_cast_tests(casts, "timestamp_s_tz_shanghai")

    def test_duration_s_casts(self):
        """Test duration[s] -> all scalar types."""
        import pyarrow as pa

        dur_s = 3600  # 1 hour in seconds

        casts = {
            # duration[s] -> integers (only int64 supported)
            "int8": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "int64": [
                (pa.array([dur_s, None], pa.duration("s")), pa.array([dur_s, None], pa.int64())),
            ],
            "uint8": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            # duration[s] -> floats (not supported)
            "float16": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            # duration[s] -> bool (not supported)
            "bool": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            # duration[s] -> string
            "string": [
                (pa.array([dur_s, None], pa.duration("s")), pa.array(["3600", None], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([dur_s, None], pa.duration("s")),
                    pa.array(["3600", None], pa.large_string()),
                ),
            ],
            # duration[s] -> binary (not supported)
            "binary": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[s] -> decimal (not supported)
            "decimal128": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "decimal256": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            # duration[s] -> date (not supported)
            "date32": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            # duration[s] -> timestamp (not supported)
            "timestamp_s": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms_tz": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[s] -> duration (conversions between units)
            "duration_s": [
                (
                    pa.array([dur_s, None], pa.duration("s")),
                    pa.array([dur_s, None], pa.duration("s")),
                ),
            ],
            "duration_ms": [
                (
                    pa.array([dur_s, None], pa.duration("s")),
                    pa.array([dur_s * 1000, None], pa.duration("ms")),
                ),
            ],
            "duration_us": [
                (
                    pa.array([dur_s, None], pa.duration("s")),
                    pa.array([dur_s * 1000000, None], pa.duration("us")),
                ),
            ],
            "duration_ns": [
                (
                    pa.array([dur_s, None], pa.duration("s")),
                    pa.array([dur_s * 1000000000, None], pa.duration("ns")),
                ),
            ],
            # duration[s] -> time (not supported)
            "time32_s": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([dur_s], pa.duration("s")), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "duration_s")

    def test_duration_ms_casts(self):
        """Test duration[ms] -> all scalar types."""
        import pyarrow as pa

        dur_ms = 3600000  # 1 hour in milliseconds
        dur_ms_lossy = 3600123  # 1 hour + 123ms

        casts = {
            # duration[ms] -> integers (only int64 supported)
            "int8": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "int64": [
                (pa.array([dur_ms, None], pa.duration("ms")), pa.array([dur_ms, None], pa.int64())),
            ],
            "uint8": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            # duration[ms] -> floats (not supported)
            "float16": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            # duration[ms] -> bool (not supported)
            "bool": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            # duration[ms] -> string
            "string": [
                (
                    pa.array([dur_ms, None], pa.duration("ms")),
                    pa.array(["3600000", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([dur_ms, None], pa.duration("ms")),
                    pa.array(["3600000", None], pa.large_string()),
                ),
            ],
            # duration[ms] -> binary (not supported)
            "binary": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[ms] -> decimal (not supported)
            "decimal128": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "decimal256": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[ms] -> date (not supported)
            "date32": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            # duration[ms] -> timestamp (not supported)
            "timestamp_s": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms_tz": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[ms] -> duration (lossy to lower precision)
            "duration_s": [
                (
                    pa.array([dur_ms, None], pa.duration("ms")),
                    pa.array([dur_ms // 1000, None], pa.duration("s")),
                ),
                (pa.array([dur_ms_lossy], pa.duration("ms")), pa.lib.ArrowInvalid),
            ],
            "duration_ms": [
                (
                    pa.array([dur_ms, None], pa.duration("ms")),
                    pa.array([dur_ms, None], pa.duration("ms")),
                ),
            ],
            "duration_us": [
                (
                    pa.array([dur_ms, None], pa.duration("ms")),
                    pa.array([dur_ms * 1000, None], pa.duration("us")),
                ),
            ],
            "duration_ns": [
                (
                    pa.array([dur_ms, None], pa.duration("ms")),
                    pa.array([dur_ms * 1000000, None], pa.duration("ns")),
                ),
            ],
            # duration[ms] -> time (not supported)
            "time32_s": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([dur_ms], pa.duration("ms")), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "duration_ms")

    def test_duration_us_casts(self):
        """Test duration[us] -> all scalar types."""
        import pyarrow as pa

        dur_us = 3600000000  # 1 hour in microseconds
        dur_us_lossy = 3600000123  # 1 hour + 123us

        casts = {
            # duration[us] -> integers (only int64 supported)
            "int8": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "int64": [
                (pa.array([dur_us, None], pa.duration("us")), pa.array([dur_us, None], pa.int64())),
            ],
            "uint8": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            # duration[us] -> floats (not supported)
            "float16": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            # duration[us] -> bool (not supported)
            "bool": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            # duration[us] -> string
            "string": [
                (
                    pa.array([dur_us, None], pa.duration("us")),
                    pa.array(["3600000000", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([dur_us, None], pa.duration("us")),
                    pa.array(["3600000000", None], pa.large_string()),
                ),
            ],
            # duration[us] -> binary (not supported)
            "binary": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[us] -> decimal (not supported)
            "decimal128": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "decimal256": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[us] -> date (not supported)
            "date32": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            # duration[us] -> timestamp (not supported)
            "timestamp_s": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms_tz": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[us] -> duration (lossy to lower precision)
            "duration_s": [
                (
                    pa.array([dur_us, None], pa.duration("us")),
                    pa.array([dur_us // 1000000, None], pa.duration("s")),
                ),
                (pa.array([dur_us_lossy], pa.duration("us")), pa.lib.ArrowInvalid),
            ],
            "duration_ms": [
                (
                    pa.array([dur_us, None], pa.duration("us")),
                    pa.array([dur_us // 1000, None], pa.duration("ms")),
                ),
                (pa.array([dur_us_lossy], pa.duration("us")), pa.lib.ArrowInvalid),
            ],
            "duration_us": [
                (
                    pa.array([dur_us, None], pa.duration("us")),
                    pa.array([dur_us, None], pa.duration("us")),
                ),
            ],
            "duration_ns": [
                (
                    pa.array([dur_us, None], pa.duration("us")),
                    pa.array([dur_us * 1000, None], pa.duration("ns")),
                ),
            ],
            # duration[us] -> time (not supported)
            "time32_s": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([dur_us], pa.duration("us")), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "duration_us")

    def test_duration_ns_casts(self):
        """Test duration[ns] -> all scalar types."""
        import pyarrow as pa

        dur_ns = 3600000000000  # 1 hour in nanoseconds
        dur_ns_lossy = 3600000000123  # 1 hour + 123ns

        casts = {
            # duration[ns] -> integers (only int64 supported)
            "int8": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "int64": [
                (pa.array([dur_ns, None], pa.duration("ns")), pa.array([dur_ns, None], pa.int64())),
            ],
            "uint8": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            # duration[ns] -> floats (not supported)
            "float16": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            # duration[ns] -> bool (not supported)
            "bool": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            # duration[ns] -> string
            "string": [
                (
                    pa.array([dur_ns, None], pa.duration("ns")),
                    pa.array(["3600000000000", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([dur_ns, None], pa.duration("ns")),
                    pa.array(["3600000000000", None], pa.large_string()),
                ),
            ],
            # duration[ns] -> binary (not supported)
            "binary": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "fixed_size_binary_16": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[ns] -> decimal (not supported)
            "decimal128": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "decimal256": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[ns] -> date (not supported)
            "date32": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            # duration[ns] -> timestamp (not supported)
            "timestamp_s": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms_tz": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # duration[ns] -> duration (lossy to lower precision)
            "duration_s": [
                (
                    pa.array([dur_ns, None], pa.duration("ns")),
                    pa.array([dur_ns // 1000000000, None], pa.duration("s")),
                ),
                (pa.array([dur_ns_lossy], pa.duration("ns")), pa.lib.ArrowInvalid),
            ],
            "duration_ms": [
                (
                    pa.array([dur_ns, None], pa.duration("ns")),
                    pa.array([dur_ns // 1000000, None], pa.duration("ms")),
                ),
                (pa.array([dur_ns_lossy], pa.duration("ns")), pa.lib.ArrowInvalid),
            ],
            "duration_us": [
                (
                    pa.array([dur_ns, None], pa.duration("ns")),
                    pa.array([dur_ns // 1000, None], pa.duration("us")),
                ),
                (pa.array([dur_ns_lossy], pa.duration("ns")), pa.lib.ArrowInvalid),
            ],
            "duration_ns": [
                (
                    pa.array([dur_ns, None], pa.duration("ns")),
                    pa.array([dur_ns, None], pa.duration("ns")),
                ),
            ],
            # duration[ns] -> time (not supported)
            "time32_s": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "time32_ms": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "time64_us": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
            "time64_ns": [(pa.array([dur_ns], pa.duration("ns")), pa.lib.ArrowNotImplementedError)],
        }
        self._run_cast_tests(casts, "duration_ns")

    def test_time32_s_casts(self):
        """Test time32[s] -> all scalar types."""
        import pyarrow as pa
        import datetime

        t = datetime.time(12, 30, 45)
        t_us = 45045  # seconds since midnight

        casts = {
            # time32[s] -> integers (only int32 supported)
            "int8": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "int32": [
                (pa.array([t, None], pa.time32("s")), pa.array([t_us, None], pa.int32())),
            ],
            "int64": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "uint8": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            # time32[s] -> floats (not supported)
            "float16": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            # time32[s] -> bool (not supported)
            "bool": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            # time32[s] -> string
            "string": [
                (pa.array([t, None], pa.time32("s")), pa.array(["12:30:45", None], pa.string())),
            ],
            "large_string": [
                (
                    pa.array([t, None], pa.time32("s")),
                    pa.array(["12:30:45", None], pa.large_string()),
                ),
            ],
            # time32[s] -> binary (not supported)
            "binary": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [
                (pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)
            ],
            # time32[s] -> decimal (not supported)
            "decimal128": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "decimal256": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            # time32[s] -> date (not supported)
            "date32": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            # time32[s] -> timestamp (not supported)
            "timestamp_s": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_shanghai": [
                (pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)
            ],
            # time32[s] -> duration (not supported)
            "duration_s": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([t], pa.time32("s")), pa.lib.ArrowNotImplementedError)],
            # time32[s] -> time (conversions between units)
            "time32_s": [
                (pa.array([t, None], pa.time32("s")), pa.array([t, None], pa.time32("s"))),
            ],
            "time32_ms": [
                (pa.array([t, None], pa.time32("s")), pa.array([t, None], pa.time32("ms"))),
            ],
            "time64_us": [
                (pa.array([t, None], pa.time32("s")), pa.array([t, None], pa.time64("us"))),
            ],
            "time64_ns": [
                (pa.array([t, None], pa.time32("s")), pa.array([t, None], pa.time64("ns"))),
            ],
        }
        self._run_cast_tests(casts, "time32_s")

    def test_time32_ms_casts(self):
        """Test time32[ms] -> all scalar types."""
        import pyarrow as pa
        import datetime

        t = datetime.time(12, 30, 45, 123000)  # with ms
        t_lossless = datetime.time(12, 30, 45)  # no sub-second
        t_ms = 45045123  # milliseconds since midnight

        casts = {
            # time32[ms] -> integers (only int32 supported)
            "int8": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "int32": [
                (pa.array([t, None], pa.time32("ms")), pa.array([t_ms, None], pa.int32())),
            ],
            "int64": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "uint8": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            # time32[ms] -> floats (not supported)
            "float16": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            # time32[ms] -> bool (not supported)
            "bool": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            # time32[ms] -> string
            "string": [
                (
                    pa.array([t, None], pa.time32("ms")),
                    pa.array(["12:30:45.123", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([t, None], pa.time32("ms")),
                    pa.array(["12:30:45.123", None], pa.large_string()),
                ),
            ],
            # time32[ms] -> binary (not supported)
            "binary": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [
                (pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)
            ],
            # time32[ms] -> decimal (not supported)
            "decimal128": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "decimal256": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            # time32[ms] -> date (not supported)
            "date32": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            # time32[ms] -> timestamp (not supported)
            "timestamp_s": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [
                (pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)
            ],
            # time32[ms] -> duration (not supported)
            "duration_s": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([t], pa.time32("ms")), pa.lib.ArrowNotImplementedError)],
            # time32[ms] -> time (lossy to lower precision)
            "time32_s": [
                (
                    pa.array([t_lossless, None], pa.time32("ms")),
                    pa.array([t_lossless, None], pa.time32("s")),
                ),
                (pa.array([t], pa.time32("ms")), pa.lib.ArrowInvalid),
            ],
            "time32_ms": [
                (pa.array([t, None], pa.time32("ms")), pa.array([t, None], pa.time32("ms"))),
            ],
            "time64_us": [
                (pa.array([t, None], pa.time32("ms")), pa.array([t, None], pa.time64("us"))),
            ],
            "time64_ns": [
                (pa.array([t, None], pa.time32("ms")), pa.array([t, None], pa.time64("ns"))),
            ],
        }
        self._run_cast_tests(casts, "time32_ms")

    def test_time64_us_casts(self):
        """Test time64[us] -> all scalar types."""
        import pyarrow as pa
        import datetime

        t = datetime.time(12, 30, 45, 123456)  # with us
        t_lossless = datetime.time(12, 30, 45)  # no sub-second
        t_us = 45045123456  # microseconds since midnight

        casts = {
            # time64[us] -> integers (only int64 supported)
            "int8": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "int64": [
                (pa.array([t, None], pa.time64("us")), pa.array([t_us, None], pa.int64())),
            ],
            "uint8": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            # time64[us] -> floats (not supported)
            "float16": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            # time64[us] -> bool (not supported)
            "bool": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            # time64[us] -> string
            "string": [
                (
                    pa.array([t, None], pa.time64("us")),
                    pa.array(["12:30:45.123456", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([t, None], pa.time64("us")),
                    pa.array(["12:30:45.123456", None], pa.large_string()),
                ),
            ],
            # time64[us] -> binary (not supported)
            "binary": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [
                (pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)
            ],
            # time64[us] -> decimal (not supported)
            "decimal128": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "decimal256": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            # time64[us] -> date (not supported)
            "date32": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            # time64[us] -> timestamp (not supported)
            "timestamp_s": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms_tz": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "timestamp_us_tz": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns_tz": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz_ny": [
                (pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)
            ],
            # time64[us] -> duration (not supported)
            "duration_s": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([t], pa.time64("us")), pa.lib.ArrowNotImplementedError)],
            # time64[us] -> time (lossy to lower precision)
            "time32_s": [
                (
                    pa.array([t_lossless, None], pa.time64("us")),
                    pa.array([t_lossless, None], pa.time32("s")),
                ),
                (pa.array([t], pa.time64("us")), pa.lib.ArrowInvalid),
            ],
            "time32_ms": [
                (
                    pa.array([t_lossless, None], pa.time64("us")),
                    pa.array([t_lossless, None], pa.time32("ms")),
                ),
                (pa.array([t], pa.time64("us")), pa.lib.ArrowInvalid),
            ],
            "time64_us": [
                (pa.array([t, None], pa.time64("us")), pa.array([t, None], pa.time64("us"))),
            ],
            "time64_ns": [
                (pa.array([t, None], pa.time64("us")), pa.array([t, None], pa.time64("ns"))),
            ],
        }
        self._run_cast_tests(casts, "time64_us")

    def test_time64_ns_casts(self):
        """Test time64[ns] -> all scalar types."""
        import pyarrow as pa
        import datetime

        # datetime.time only supports microsecond precision
        t = datetime.time(12, 30, 45, 123456)
        t_lossless = datetime.time(12, 30, 45)  # no sub-second
        t_ns = 45045123456789  # nanoseconds since midnight (with ns precision)
        t_ns_lossless = 45045000000000  # no sub-second

        casts = {
            # time64[ns] -> integers (only int64 supported)
            "int8": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "int16": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "int32": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "int64": [
                (pa.array([t_ns, None], pa.time64("ns")), pa.array([t_ns, None], pa.int64())),
            ],
            "uint8": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "uint16": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "uint32": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "uint64": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            # time64[ns] -> floats (not supported)
            "float16": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "float32": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "float64": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            # time64[ns] -> bool (not supported)
            "bool": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            # time64[ns] -> string
            "string": [
                (
                    pa.array([t_ns, None], pa.time64("ns")),
                    pa.array(["12:30:45.123456789", None], pa.string()),
                ),
            ],
            "large_string": [
                (
                    pa.array([t_ns, None], pa.time64("ns")),
                    pa.array(["12:30:45.123456789", None], pa.large_string()),
                ),
            ],
            # time64[ns] -> binary (not supported)
            "binary": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "large_binary": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "fixed_size_binary_16": [
                (pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # time64[ns] -> decimal (not supported)
            "decimal128": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "decimal256": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            # time64[ns] -> date (not supported)
            "date32": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "date64": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            # time64[ns] -> timestamp (not supported)
            "timestamp_s": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ms": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "timestamp_us": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "timestamp_ns": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "timestamp_s_tz": [
                (pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ms_tz": [
                (pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_us_tz": [
                (pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_ns_tz": [
                (pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_ny": [
                (pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)
            ],
            "timestamp_s_tz_shanghai": [
                (pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)
            ],
            # time64[ns] -> duration (not supported)
            "duration_s": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "duration_ms": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "duration_us": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            "duration_ns": [(pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowNotImplementedError)],
            # time64[ns] -> time (lossy to lower precision)
            "time32_s": [
                (
                    pa.array([t_ns_lossless, None], pa.time64("ns")),
                    pa.array([t_lossless, None], pa.time32("s")),
                ),
                (pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowInvalid),
            ],
            "time32_ms": [
                (
                    pa.array([t_ns_lossless, None], pa.time64("ns")),
                    pa.array([t_lossless, None], pa.time32("ms")),
                ),
                (pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowInvalid),
            ],
            "time64_us": [
                (
                    pa.array([t_ns_lossless, None], pa.time64("ns")),
                    pa.array([t_lossless, None], pa.time64("us")),
                ),
                (pa.array([t_ns], pa.time64("ns")), pa.lib.ArrowInvalid),
            ],
            "time64_ns": [
                (pa.array([t_ns, None], pa.time64("ns")), pa.array([t, None], pa.time64("ns"))),
            ],
        }
        self._run_cast_tests(casts, "time64_ns")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
