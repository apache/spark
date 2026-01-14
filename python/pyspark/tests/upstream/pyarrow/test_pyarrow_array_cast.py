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

This test suite is part of SPARK-54936 to monitor upstream PyArrow behavior.

**Scope**: Only tests pa.Array.cast(target_type) with default parameters (safe=True).
Does NOT test with safe=False or other options.

Tests all combinations of source type -> target type to ensure PySpark's
assumptions about PyArrow's casting behavior remain valid across versions.

## Type Conversion Matrix (pa.Array.cast with default safe=True)

### Comprehensive Type Coverage:
- **Integers**: int8, int16, int32, int64, uint8, uint16, uint32, uint64
- **Floats**: float16, float32, float64
- **Strings**: string, large_string
- **Binary**: binary, large_binary
- **Decimals**: decimal128, decimal256
- **Dates**: date32, date64
- **Timestamps**: timestamp[s/ms/us/ns]
- **Durations**: duration[s/ms/us/ns]
- **Times**: time32[s/ms], time64[us/ns]
- **Lists**: list, large_list, fixed_size_list
- **Complex**: struct, map

### Conversion Matrix:

| From \\ To         | int8-64 | uint8-64 | float16-64 | bool | string/large | binary/large | decimal128/256 | date32/64 | timestamp | duration | time | list/large | struct | map |
|-------------------|---------|----------|------------|------|--------------|--------------|----------------|-----------|-----------|----------|------|------------|--------|-----|
| **int8-64**       | ✓       | ✓        | ✓          | ✓    | ✓            | ✗            | ✓              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **uint8-64**      | ✓       | ✓        | ✓          | ✓    | ✓            | ✗            | ✓              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **float16-64**    | ✓ᴺᵀ     | ✓ᴺᵀ      | ✓          | ✓    | ✓            | ✗            | ✓              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **bool**          | ✓       | ✓        | ✓⁽ᶠ³²⁺⁾    | ✓    | ✓            | ✗            | ✗              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **string/large**  | ✓       | ✓        | ✓          | ✓    | ✓            | ✓            | ✓              | ✓         | ✓         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **binary/large**  | ✗       | ✗        | ✗          | ✗    | ✓            | ✓            | ✗              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **decimal128/256**| ✓       | ✓        | ✓          | ✗    | ✓            | ✗            | ✓              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **date32/64**     | ✗       | ✗        | ✗          | ✗    | ✓            | ✗            | ✗              | ✓         | ✓         | ✗        | ✗    | ✗          | ✗      | ✗   |
| **timestamp**     | ✗       | ✗        | ✗          | ✗    | ✓            | ✗            | ✗              | ✓         | ✓ᵁᴾ       | ✗        | ✗    | ✗          | ✗      | ✗   |
| **duration**      | ✗       | ✗        | ✗          | ✗    | ✓            | ✗            | ✗              | ✗         | ✗         | ✓ᵁᴾ      | ✗    | ✗          | ✗      | ✗   |
| **time32/64**     | ✗       | ✗        | ✗          | ✗    | ✓            | ✗            | ✗              | ✗         | ✗         | ✗        | ✓ᵁᴾ  | ✗          | ✗      | ✗   |
| **list/large**    | ✗       | ✗        | ✗          | ✗    | ✗            | ✗            | ✗              | ✗         | ✗         | ✗        | ✗    | ✓ᴱᴸ        | ✗      | ✗   |
| **struct**        | ✗       | ✗        | ✗          | ✗    | ✗            | ✗            | ✗              | ✗         | ✗         | ✗        | ✗    | ✗          | ✓ᴱᴸ    | ✗   |
| **map**           | ✗       | ✗        | ✗          | ✗    | ✗            | ✗            | ✗              | ✗         | ✗         | ✗        | ✗    | ✗          | ✗      | ✓ᴱᴸ |

Legend:
- ✓      = Allowed (no precision loss with safe=True)
- ✓ᴺᵀ    = No Truncation: only if no truncation (e.g., 1.0→1 ok, 1.5→1 fails)
- ✓ᵁᴾ    = Upcast only: converting to higher precision unit (s→ms ok, ms→s may fail)
- ✓ᴱᴸ    = Element-wise: element/field types must be castable
- ✓⁽ᶠ³²⁺⁾ = float32 and above (bool->float16 not supported)
- ✗      = Not allowed / raises ArrowInvalid

Notes:
1. With default safe=True, PyArrow prevents precision loss
2. Float→Int requires whole numbers (1.0 ok, 1.5 fails)
3. Timestamp/Duration/Time conversions to lower precision may lose data
4. Large int64 values may exceed float64 safe range (±2^53)
5. Nested type casts recursively cast element types
6. string/large_string and binary/large_binary are interchangeable
7. decimal128 and decimal256 require sufficient precision for int64 (≥21 digits)
"""

import unittest
import math
from datetime import datetime, date

from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message


from decimal import Decimal


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PyArrowCompleteCastMatrixTests(unittest.TestCase):
    """
    Complete N×N cast matrix test for PyArrow pa.Array.cast() with safe=True.

    Structure: 2D nested dict with list of (source, target) tuples
    ==============================================================
    CAST_MATRIX[source_type][target_type] = [
        (source_arr1, expected_target1),  # tuple: (source, expected)
        (source_arr2, expected_target2),  # expected can be pa.Array or ExceptionClass
        ...
    ]

    Each tuple is an independent test case covering:
    - Normal values and boundary values
    - Edge cases (special floats, Unicode strings, temporal boundaries)
    - Empty arrays and all-null arrays
    - Values that should fail (target is ExceptionClass)

    Type Categories:
    - Integers: int8, int16, int32, int64, uint8, uint16, uint32, uint64
    - Floats: float16, float32, float64
    - Boolean: bool
    - Strings: string, large_string
    - Binary: binary, large_binary
    - Decimals: decimal128, decimal256
    - Temporal: date32, date64, timestamp[s/ms/us/ns], duration[s/ms/us/ns], time32/time64
    - Complex: list, large_list, fixed_size_list, struct, map
    """

    @classmethod
    def setUpClass(cls):
        """Initialize the complete cast matrix."""
        import pyarrow as pa

        cls.CAST_MATRIX = cls._build_cast_matrix(pa)

    @classmethod
    def _build_cast_matrix(cls, pa):
        """
        Build complete 2D cast matrix with all type conversions.

        Structure:
            matrix[src_type][tgt_type] = [
                (source_arr, expected_arr_or_exception),
                ...
            ]
        """
        from collections import defaultdict
        from datetime import datetime, date

        matrix = defaultdict(dict)

        int_types = ["int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"]

        # ============================================================
        # INTEGER -> INTEGER conversions (with boundary values)
        # ============================================================

        # ============================================================
        # int8
        # ============================================================
        # Boundaries: -128 to 127

        # int8 -> int8 (identity)
        matrix["int8"]["int8"] = [
            (
                pa.array([0, 1, -1, None], pa.int8()),
                pa.array([0, 1, -1, None], pa.int8()),
            ),  # normal
            (pa.array([127, -128], pa.int8()), pa.array([127, -128], pa.int8())),  # boundary
            (pa.array([], pa.int8()), pa.array([], pa.int8())),  # empty
        ]
        # int8 -> int16 (widening, always safe)
        matrix["int8"]["int16"] = [
            (
                pa.array([0, 1, -1, None], pa.int8()),
                pa.array([0, 1, -1, None], pa.int16()),
            ),  # normal
            (pa.array([127, -128], pa.int8()), pa.array([127, -128], pa.int16())),  # boundary
            (pa.array([None, None], pa.int8()), pa.array([None, None], pa.int16())),  # all nulls
        ]
        # int8 -> int32 (widening, always safe)
        matrix["int8"]["int32"] = [
            (pa.array([0, 1, -1, None], pa.int8()), pa.array([0, 1, -1, None], pa.int32())),
            (pa.array([127, -128], pa.int8()), pa.array([127, -128], pa.int32())),
            (pa.array([None, None], pa.int8()), pa.array([None, None], pa.int32())),
        ]
        # int8 -> int64 (widening, always safe)
        matrix["int8"]["int64"] = [
            (pa.array([0, 1, -1, None], pa.int8()), pa.array([0, 1, -1, None], pa.int64())),
            (pa.array([127, -128], pa.int8()), pa.array([127, -128], pa.int64())),
            (pa.array([None, None], pa.int8()), pa.array([None, None], pa.int64())),
        ]
        # int8 -> uint8 (negative values overflow)
        matrix["int8"]["uint8"] = [
            (
                pa.array([0, 1, 127, None], pa.int8()),
                pa.array([0, 1, 127, None], pa.uint8()),
            ),  # valid range
            (pa.array([-1], pa.int8()), pa.lib.ArrowInvalid),  # negative overflow
            (pa.array([-128], pa.int8()), pa.lib.ArrowInvalid),  # min boundary overflow
        ]
        # int8 -> uint16 (negative values overflow)
        matrix["int8"]["uint16"] = [
            (pa.array([0, 1, 127, None], pa.int8()), pa.array([0, 1, 127, None], pa.uint16())),
            (pa.array([-1], pa.int8()), pa.lib.ArrowInvalid),
            (pa.array([-128], pa.int8()), pa.lib.ArrowInvalid),
        ]
        # int8 -> uint32 (negative values overflow)
        matrix["int8"]["uint32"] = [
            (pa.array([0, 1, 127, None], pa.int8()), pa.array([0, 1, 127, None], pa.uint32())),
            (pa.array([-1], pa.int8()), pa.lib.ArrowInvalid),
            (pa.array([-128], pa.int8()), pa.lib.ArrowInvalid),
        ]
        # int8 -> uint64 (negative values overflow)
        matrix["int8"]["uint64"] = [
            (pa.array([0, 1, 127, None], pa.int8()), pa.array([0, 1, 127, None], pa.uint64())),
            (pa.array([-1], pa.int8()), pa.lib.ArrowInvalid),
            (pa.array([-128], pa.int8()), pa.lib.ArrowInvalid),
        ]

        # int8 -> float16 (always safe, no precision loss for int8 range)
        matrix["int8"]["float16"] = [
            (pa.array([0, 1, -1, None], pa.int8()), pa.array([0.0, 1.0, -1.0, None], pa.float16())),
            (pa.array([127, -128], pa.int8()), pa.array([127.0, -128.0], pa.float16())),
            (pa.array([None, None], pa.int8()), pa.array([None, None], pa.float16())),
        ]
        # int8 -> float32 (always safe)
        matrix["int8"]["float32"] = [
            (pa.array([0, 1, -1, None], pa.int8()), pa.array([0.0, 1.0, -1.0, None], pa.float32())),
            (pa.array([127, -128], pa.int8()), pa.array([127.0, -128.0], pa.float32())),
            (pa.array([None, None], pa.int8()), pa.array([None, None], pa.float32())),
        ]
        # int8 -> float64 (always safe)
        matrix["int8"]["float64"] = [
            (pa.array([0, 1, -1, None], pa.int8()), pa.array([0.0, 1.0, -1.0, None], pa.float64())),
            (pa.array([127, -128], pa.int8()), pa.array([127.0, -128.0], pa.float64())),
            (pa.array([None, None], pa.int8()), pa.array([None, None], pa.float64())),
        ]

        # int8 -> bool (0 -> False, non-zero -> True)
        matrix["int8"]["bool"] = [
            (pa.array([0, 1, None], pa.int8()), pa.array([False, True, None], pa.bool_())),  # basic
            (
                pa.array([127, -128, -1], pa.int8()),
                pa.array([True, True, True], pa.bool_()),
            ),  # boundary
            (
                pa.array([0, 0, 0], pa.int8()),
                pa.array([False, False, False], pa.bool_()),
            ),  # all zeros
        ]

        # int8 -> string
        matrix["int8"]["string"] = [
            (pa.array([0, 1, -1, None], pa.int8()), pa.array(["0", "1", "-1", None], pa.string())),
            (pa.array([127, -128], pa.int8()), pa.array(["127", "-128"], pa.string())),
            (pa.array([None, None], pa.int8()), pa.array([None, None], pa.string())),
        ]
        # int8 -> large_string
        matrix["int8"]["large_string"] = [
            (
                pa.array([0, 1, -1, None], pa.int8()),
                pa.array(["0", "1", "-1", None], pa.large_string()),
            ),
            (pa.array([127, -128], pa.int8()), pa.array(["127", "-128"], pa.large_string())),
            (pa.array([None, None], pa.int8()), pa.array([None, None], pa.large_string())),
        ]

        # int8 -> binary (not supported)
        matrix["int8"]["binary"] = [
            (pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError),
            (pa.array([127], pa.int8()), pa.lib.ArrowNotImplementedError),
            (pa.array([-128], pa.int8()), pa.lib.ArrowNotImplementedError),
        ]
        # int8 -> large_binary (not supported)
        matrix["int8"]["large_binary"] = [
            (pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError),
            (pa.array([127], pa.int8()), pa.lib.ArrowNotImplementedError),
            (pa.array([-128], pa.int8()), pa.lib.ArrowNotImplementedError),
        ]

        # int8 -> decimal128
        matrix["int8"]["decimal128"] = [
            (
                pa.array([0, 1, -1, None], pa.int8()),
                pa.array(
                    [Decimal("0.00"), Decimal("1.00"), Decimal("-1.00"), None], pa.decimal128(12, 2)
                ),
            ),
            (
                pa.array([127, -128], pa.int8()),
                pa.array([Decimal("127.00"), Decimal("-128.00")], pa.decimal128(12, 2)),
            ),
            (pa.array([None, None], pa.int8()), pa.array([None, None], pa.decimal128(12, 2))),
        ]

        # int8 -> temporal (not supported for direct cast)
        for temporal_type in [
            "timestamp_s",
            "timestamp_ms",
            "timestamp_us",
            "timestamp_ns",
            "date32",
            "date64",
            "time32_s",
            "time32_ms",
            "time64_us",
            "time64_ns",
            "duration_s",
            "duration_ms",
            "duration_us",
            "duration_ns",
        ]:
            matrix["int8"][temporal_type] = [
                (pa.array([0], pa.int8()), pa.lib.ArrowNotImplementedError),
                (pa.array([127], pa.int8()), pa.lib.ArrowNotImplementedError),
                (pa.array([-128], pa.int8()), pa.lib.ArrowNotImplementedError),
            ]

        # ============================================================
        # int16
        # ============================================================
        # Boundaries: -32768 to 32767
        matrix["int16"]["int8"] = [
            (
                pa.array([0, 1, -1, 127, -128, None], pa.int16()),
                pa.array([0, 1, -1, 127, -128, None], pa.int8()),
            ),
            (pa.array([128], pa.int16()), pa.lib.ArrowInvalid),
            (pa.array([-129], pa.int16()), pa.lib.ArrowInvalid),
            (pa.array([32767], pa.int16()), pa.lib.ArrowInvalid),
            (pa.array([-32768], pa.int16()), pa.lib.ArrowInvalid),
        ]
        matrix["int16"]["int16"] = [
            (
                pa.array([32767, -32768, 0, None], pa.int16()),
                pa.array([32767, -32768, 0, None], pa.int16()),
            ),
        ]
        matrix["int16"]["int32"] = [
            (
                pa.array([32767, -32768, 0, None], pa.int16()),
                pa.array([32767, -32768, 0, None], pa.int32()),
            ),
        ]
        matrix["int16"]["int64"] = [
            (
                pa.array([32767, -32768, 0, None], pa.int16()),
                pa.array([32767, -32768, 0, None], pa.int64()),
            ),
        ]
        matrix["int16"]["uint8"] = [
            (pa.array([0, 1, 255, None], pa.int16()), pa.array([0, 1, 255, None], pa.uint8())),
            (pa.array([256], pa.int16()), pa.lib.ArrowInvalid),
            (pa.array([-1], pa.int16()), pa.lib.ArrowInvalid),
        ]
        matrix["int16"]["uint16"] = [
            (pa.array([0, 1, 32767, None], pa.int16()), pa.array([0, 1, 32767, None], pa.uint16())),
            (pa.array([-1], pa.int16()), pa.lib.ArrowInvalid),
        ]
        matrix["int16"]["uint32"] = [
            (pa.array([0, 1, 32767, None], pa.int16()), pa.array([0, 1, 32767, None], pa.uint32())),
            (pa.array([-1], pa.int16()), pa.lib.ArrowInvalid),
        ]
        matrix["int16"]["uint64"] = [
            (pa.array([0, 1, 32767, None], pa.int16()), pa.array([0, 1, 32767, None], pa.uint64())),
            (pa.array([-1], pa.int16()), pa.lib.ArrowInvalid),
        ]

        # ============================================================
        # int32
        # ============================================================
        # Boundaries: -2147483648 to 2147483647
        matrix["int32"]["int8"] = [
            (
                pa.array([0, 1, -1, 127, -128, None], pa.int32()),
                pa.array([0, 1, -1, 127, -128, None], pa.int8()),
            ),
            (pa.array([128], pa.int32()), pa.lib.ArrowInvalid),
            (pa.array([2147483647], pa.int32()), pa.lib.ArrowInvalid),
        ]
        matrix["int32"]["int16"] = [
            (
                pa.array([0, 1, -1, 32767, -32768, None], pa.int32()),
                pa.array([0, 1, -1, 32767, -32768, None], pa.int16()),
            ),
            (pa.array([32768], pa.int32()), pa.lib.ArrowInvalid),
        ]
        matrix["int32"]["int32"] = [
            (
                pa.array([2147483647, -2147483648, 0, None], pa.int32()),
                pa.array([2147483647, -2147483648, 0, None], pa.int32()),
            ),
        ]
        matrix["int32"]["int64"] = [
            (
                pa.array([2147483647, -2147483648, 0, None], pa.int32()),
                pa.array([2147483647, -2147483648, 0, None], pa.int64()),
            ),
        ]
        matrix["int32"]["uint8"] = [
            (pa.array([0, 1, 255, None], pa.int32()), pa.array([0, 1, 255, None], pa.uint8())),
            (pa.array([256], pa.int32()), pa.lib.ArrowInvalid),
            (pa.array([-1], pa.int32()), pa.lib.ArrowInvalid),
        ]
        matrix["int32"]["uint16"] = [
            (pa.array([0, 1, 65535, None], pa.int32()), pa.array([0, 1, 65535, None], pa.uint16())),
            (pa.array([65536], pa.int32()), pa.lib.ArrowInvalid),
        ]
        matrix["int32"]["uint32"] = [
            (
                pa.array([0, 1, 2147483647, None], pa.int32()),
                pa.array([0, 1, 2147483647, None], pa.uint32()),
            ),
            (pa.array([-1], pa.int32()), pa.lib.ArrowInvalid),
        ]
        matrix["int32"]["uint64"] = [
            (
                pa.array([0, 1, 2147483647, None], pa.int32()),
                pa.array([0, 1, 2147483647, None], pa.uint64()),
            ),
            (pa.array([-1], pa.int32()), pa.lib.ArrowInvalid),
        ]

        # ============================================================
        # int64
        # ============================================================
        # Boundaries: -9223372036854775808 to 9223372036854775807
        matrix["int64"]["int8"] = [
            (
                pa.array([0, 1, -1, 127, -128, None], pa.int64()),
                pa.array([0, 1, -1, 127, -128, None], pa.int8()),
            ),
            (pa.array([128], pa.int64()), pa.lib.ArrowInvalid),
            (pa.array([9223372036854775807], pa.int64()), pa.lib.ArrowInvalid),
        ]
        matrix["int64"]["int16"] = [
            (
                pa.array([0, 1, -1, 32767, -32768, None], pa.int64()),
                pa.array([0, 1, -1, 32767, -32768, None], pa.int16()),
            ),
            (pa.array([32768], pa.int64()), pa.lib.ArrowInvalid),
        ]
        matrix["int64"]["int32"] = [
            (
                pa.array([0, 1, -1, 2147483647, -2147483648, None], pa.int64()),
                pa.array([0, 1, -1, 2147483647, -2147483648, None], pa.int32()),
            ),
            (pa.array([2147483648], pa.int64()), pa.lib.ArrowInvalid),
        ]
        matrix["int64"]["int64"] = [
            (
                pa.array([9223372036854775807, -9223372036854775808, 0, None], pa.int64()),
                pa.array([9223372036854775807, -9223372036854775808, 0, None], pa.int64()),
            ),
        ]
        matrix["int64"]["uint8"] = [
            (pa.array([0, 1, 255, None], pa.int64()), pa.array([0, 1, 255, None], pa.uint8())),
            (pa.array([256], pa.int64()), pa.lib.ArrowInvalid),
        ]
        matrix["int64"]["uint16"] = [
            (pa.array([0, 1, 65535, None], pa.int64()), pa.array([0, 1, 65535, None], pa.uint16())),
            (pa.array([65536], pa.int64()), pa.lib.ArrowInvalid),
        ]
        matrix["int64"]["uint32"] = [
            (
                pa.array([0, 1, 4294967295, None], pa.int64()),
                pa.array([0, 1, 4294967295, None], pa.uint32()),
            ),
            (pa.array([4294967296], pa.int64()), pa.lib.ArrowInvalid),
        ]
        matrix["int64"]["uint64"] = [
            (
                pa.array([0, 1, 9223372036854775807, None], pa.int64()),
                pa.array([0, 1, 9223372036854775807, None], pa.uint64()),
            ),
            (pa.array([-1], pa.int64()), pa.lib.ArrowInvalid),
        ]

        # ============================================================
        # uint8
        # ============================================================
        # Boundaries: 0 to 255
        matrix["uint8"]["int8"] = [
            (pa.array([0, 1, 127, None], pa.uint8()), pa.array([0, 1, 127, None], pa.int8())),
            (pa.array([128], pa.uint8()), pa.lib.ArrowInvalid),
            (pa.array([255], pa.uint8()), pa.lib.ArrowInvalid),
        ]
        matrix["uint8"]["int16"] = [
            (pa.array([255, 0, 1, None], pa.uint8()), pa.array([255, 0, 1, None], pa.int16())),
        ]
        matrix["uint8"]["int32"] = [
            (pa.array([255, 0, 1, None], pa.uint8()), pa.array([255, 0, 1, None], pa.int32())),
        ]
        matrix["uint8"]["int64"] = [
            (pa.array([255, 0, 1, None], pa.uint8()), pa.array([255, 0, 1, None], pa.int64())),
        ]
        matrix["uint8"]["uint8"] = [
            (pa.array([255, 0, 1, None], pa.uint8()), pa.array([255, 0, 1, None], pa.uint8())),
        ]
        matrix["uint8"]["uint16"] = [
            (pa.array([255, 0, 1, None], pa.uint8()), pa.array([255, 0, 1, None], pa.uint16())),
        ]
        matrix["uint8"]["uint32"] = [
            (pa.array([255, 0, 1, None], pa.uint8()), pa.array([255, 0, 1, None], pa.uint32())),
        ]
        matrix["uint8"]["uint64"] = [
            (pa.array([255, 0, 1, None], pa.uint8()), pa.array([255, 0, 1, None], pa.uint64())),
        ]

        # ============================================================
        # uint16
        # ============================================================
        # Boundaries: 0 to 65535
        matrix["uint16"]["int8"] = [
            (pa.array([0, 1, 127, None], pa.uint16()), pa.array([0, 1, 127, None], pa.int8())),
            (pa.array([128], pa.uint16()), pa.lib.ArrowInvalid),
        ]
        matrix["uint16"]["int16"] = [
            (pa.array([0, 1, 32767, None], pa.uint16()), pa.array([0, 1, 32767, None], pa.int16())),
            (pa.array([32768], pa.uint16()), pa.lib.ArrowInvalid),
            (pa.array([65535], pa.uint16()), pa.lib.ArrowInvalid),
        ]
        matrix["uint16"]["int32"] = [
            (pa.array([65535, 0, 1, None], pa.uint16()), pa.array([65535, 0, 1, None], pa.int32())),
        ]
        matrix["uint16"]["int64"] = [
            (pa.array([65535, 0, 1, None], pa.uint16()), pa.array([65535, 0, 1, None], pa.int64())),
        ]
        matrix["uint16"]["uint8"] = [
            (pa.array([0, 1, 255, None], pa.uint16()), pa.array([0, 1, 255, None], pa.uint8())),
            (pa.array([256], pa.uint16()), pa.lib.ArrowInvalid),
        ]
        matrix["uint16"]["uint16"] = [
            (
                pa.array([65535, 0, 1, None], pa.uint16()),
                pa.array([65535, 0, 1, None], pa.uint16()),
            ),
        ]
        matrix["uint16"]["uint32"] = [
            (
                pa.array([65535, 0, 1, None], pa.uint16()),
                pa.array([65535, 0, 1, None], pa.uint32()),
            ),
        ]
        matrix["uint16"]["uint64"] = [
            (
                pa.array([65535, 0, 1, None], pa.uint16()),
                pa.array([65535, 0, 1, None], pa.uint64()),
            ),
        ]

        # ============================================================
        # uint32
        # ============================================================
        # Boundaries: 0 to 4294967295
        matrix["uint32"]["int8"] = [
            (pa.array([0, 1, 127, None], pa.uint32()), pa.array([0, 1, 127, None], pa.int8())),
            (pa.array([128], pa.uint32()), pa.lib.ArrowInvalid),
        ]
        matrix["uint32"]["int16"] = [
            (pa.array([0, 1, 32767, None], pa.uint32()), pa.array([0, 1, 32767, None], pa.int16())),
            (pa.array([32768], pa.uint32()), pa.lib.ArrowInvalid),
        ]
        matrix["uint32"]["int32"] = [
            (
                pa.array([0, 1, 2147483647, None], pa.uint32()),
                pa.array([0, 1, 2147483647, None], pa.int32()),
            ),
            (pa.array([2147483648], pa.uint32()), pa.lib.ArrowInvalid),
            (pa.array([4294967295], pa.uint32()), pa.lib.ArrowInvalid),
        ]
        matrix["uint32"]["int64"] = [
            (
                pa.array([4294967295, 0, 1, None], pa.uint32()),
                pa.array([4294967295, 0, 1, None], pa.int64()),
            ),
        ]
        matrix["uint32"]["uint8"] = [
            (pa.array([0, 1, 255, None], pa.uint32()), pa.array([0, 1, 255, None], pa.uint8())),
            (pa.array([256], pa.uint32()), pa.lib.ArrowInvalid),
        ]
        matrix["uint32"]["uint16"] = [
            (
                pa.array([0, 1, 65535, None], pa.uint32()),
                pa.array([0, 1, 65535, None], pa.uint16()),
            ),
            (pa.array([65536], pa.uint32()), pa.lib.ArrowInvalid),
        ]
        matrix["uint32"]["uint32"] = [
            (
                pa.array([4294967295, 0, 1, None], pa.uint32()),
                pa.array([4294967295, 0, 1, None], pa.uint32()),
            ),
        ]
        matrix["uint32"]["uint64"] = [
            (
                pa.array([4294967295, 0, 1, None], pa.uint32()),
                pa.array([4294967295, 0, 1, None], pa.uint64()),
            ),
        ]

        # ============================================================
        # uint64
        # ============================================================
        # Boundaries: 0 to 18446744073709551615
        matrix["uint64"]["int8"] = [
            (pa.array([0, 1, 127, None], pa.uint64()), pa.array([0, 1, 127, None], pa.int8())),
            (pa.array([128], pa.uint64()), pa.lib.ArrowInvalid),
        ]
        matrix["uint64"]["int16"] = [
            (pa.array([0, 1, 32767, None], pa.uint64()), pa.array([0, 1, 32767, None], pa.int16())),
            (pa.array([32768], pa.uint64()), pa.lib.ArrowInvalid),
        ]
        matrix["uint64"]["int32"] = [
            (
                pa.array([0, 1, 2147483647, None], pa.uint64()),
                pa.array([0, 1, 2147483647, None], pa.int32()),
            ),
            (pa.array([2147483648], pa.uint64()), pa.lib.ArrowInvalid),
        ]
        matrix["uint64"]["int64"] = [
            (
                pa.array([0, 1, 9223372036854775807, None], pa.uint64()),
                pa.array([0, 1, 9223372036854775807, None], pa.int64()),
            ),
            (pa.array([9223372036854775808], pa.uint64()), pa.lib.ArrowInvalid),
            (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
        ]
        matrix["uint64"]["uint8"] = [
            (pa.array([0, 1, 255, None], pa.uint64()), pa.array([0, 1, 255, None], pa.uint8())),
            (pa.array([256], pa.uint64()), pa.lib.ArrowInvalid),
        ]
        matrix["uint64"]["uint16"] = [
            (
                pa.array([0, 1, 65535, None], pa.uint64()),
                pa.array([0, 1, 65535, None], pa.uint16()),
            ),
            (pa.array([65536], pa.uint64()), pa.lib.ArrowInvalid),
        ]
        matrix["uint64"]["uint32"] = [
            (
                pa.array([0, 1, 4294967295, None], pa.uint64()),
                pa.array([0, 1, 4294967295, None], pa.uint32()),
            ),
            (pa.array([4294967296], pa.uint64()), pa.lib.ArrowInvalid),
        ]
        matrix["uint64"]["uint64"] = [
            (
                pa.array([18446744073709551615, 0, 1, None], pa.uint64()),
                pa.array([18446744073709551615, 0, 1, None], pa.uint64()),
            ),
        ]

        # ============================================================
        # INTEGER -> FLOAT conversions
        # Note: Large integers may exceed float's exact integer range
        # float16: ~2048, float32: ~16777216, float64: ~9007199254740992
        # ============================================================

        # ============================================================
        # int16 -> float
        # ============================================================
        matrix["int16"]["float16"] = [
            (
                pa.array([0, 1, -1, None], pa.int16()),
                pa.array([0.0, 1.0, -1.0, None], pa.float16()),
            ),
            # 32767 rounds to 32768.0 in float16
            (pa.array([32767], pa.int16()), pa.array([32768.0], pa.float16())),
        ]
        matrix["int16"]["float32"] = [
            (
                pa.array([32767, -32768, 0, None], pa.int16()),
                pa.array([32767.0, -32768.0, 0.0, None], pa.float32()),
            ),
        ]
        matrix["int16"]["float64"] = [
            (
                pa.array([32767, -32768, 0, None], pa.int16()),
                pa.array([32767.0, -32768.0, 0.0, None], pa.float64()),
            ),
        ]

        # ============================================================
        # int32 -> float
        # ============================================================
        matrix["int32"]["float16"] = [
            (
                pa.array([0, 1, -1, None], pa.int32()),
                pa.array([0.0, 1.0, -1.0, None], pa.float16()),
            ),
            (pa.array([2147483647], pa.int32()), pa.lib.ArrowInvalid),  # exceeds float16 range
        ]
        matrix["int32"]["float32"] = [
            (
                pa.array([0, 1, -1, None], pa.int32()),
                pa.array([0.0, 1.0, -1.0, None], pa.float32()),
            ),
            (
                pa.array([2147483647], pa.int32()),
                pa.lib.ArrowInvalid,
            ),  # exceeds float32 exact range (±16777216)
        ]
        matrix["int32"]["float64"] = [
            (
                pa.array([2147483647, -2147483648, 0, None], pa.int32()),
                pa.array([2147483647.0, -2147483648.0, 0.0, None], pa.float64()),
            ),
        ]

        # ============================================================
        # int64 -> float
        # ============================================================
        matrix["int64"]["float16"] = [
            (
                pa.array([0, 1, -1, None], pa.int64()),
                pa.array([0.0, 1.0, -1.0, None], pa.float16()),
            ),
            (pa.array([9223372036854775807], pa.int64()), pa.lib.ArrowInvalid),
        ]
        matrix["int64"]["float32"] = [
            (
                pa.array([0, 1, -1, None], pa.int64()),
                pa.array([0.0, 1.0, -1.0, None], pa.float32()),
            ),
            (pa.array([9223372036854775807], pa.int64()), pa.lib.ArrowInvalid),
        ]
        matrix["int64"]["float64"] = [
            (
                pa.array([0, 1, -1, None], pa.int64()),
                pa.array([0.0, 1.0, -1.0, None], pa.float64()),
            ),
            (
                pa.array([9007199254740993], pa.int64()),
                pa.lib.ArrowInvalid,
            ),  # > 2^53, precision loss
        ]

        # ============================================================
        # uint8 -> float
        # ============================================================
        matrix["uint8"]["float16"] = [
            (
                pa.array([255, 0, 1, None], pa.uint8()),
                pa.array([255.0, 0.0, 1.0, None], pa.float16()),
            ),
        ]
        matrix["uint8"]["float32"] = [
            (
                pa.array([255, 0, 1, None], pa.uint8()),
                pa.array([255.0, 0.0, 1.0, None], pa.float32()),
            ),
        ]
        matrix["uint8"]["float64"] = [
            (
                pa.array([255, 0, 1, None], pa.uint8()),
                pa.array([255.0, 0.0, 1.0, None], pa.float64()),
            ),
        ]

        # ============================================================
        # uint16 -> float
        # ============================================================
        matrix["uint16"]["float16"] = [
            (pa.array([0, 1, None], pa.uint16()), pa.array([0.0, 1.0, None], pa.float16())),
            (
                pa.array([65535], pa.uint16()),
                pa.array([float("inf")], pa.float16()),
            ),  # overflow to inf
        ]
        matrix["uint16"]["float32"] = [
            (
                pa.array([65535, 0, 1, None], pa.uint16()),
                pa.array([65535.0, 0.0, 1.0, None], pa.float32()),
            ),
        ]
        matrix["uint16"]["float64"] = [
            (
                pa.array([65535, 0, 1, None], pa.uint16()),
                pa.array([65535.0, 0.0, 1.0, None], pa.float64()),
            ),
        ]

        # ============================================================
        # uint32 -> float
        # ============================================================
        matrix["uint32"]["float16"] = [
            (pa.array([0, 1, None], pa.uint32()), pa.array([0.0, 1.0, None], pa.float16())),
            (pa.array([4294967295], pa.uint32()), pa.lib.ArrowInvalid),
        ]
        matrix["uint32"]["float32"] = [
            (pa.array([0, 1, None], pa.uint32()), pa.array([0.0, 1.0, None], pa.float32())),
            (
                pa.array([4294967295], pa.uint32()),
                pa.lib.ArrowInvalid,
            ),  # exceeds float32 exact range
        ]
        matrix["uint32"]["float64"] = [
            (
                pa.array([4294967295, 0, 1, None], pa.uint32()),
                pa.array([4294967295.0, 0.0, 1.0, None], pa.float64()),
            ),
        ]

        # ============================================================
        # uint64 -> float
        # ============================================================
        matrix["uint64"]["float16"] = [
            (pa.array([0, 1, None], pa.uint64()), pa.array([0.0, 1.0, None], pa.float16())),
        ]
        matrix["uint64"]["float32"] = [
            (pa.array([0, 1, None], pa.uint64()), pa.array([0.0, 1.0, None], pa.float32())),
        ]
        matrix["uint64"]["float64"] = [
            (pa.array([0, 1, None], pa.uint64()), pa.array([0.0, 1.0, None], pa.float64())),
            (pa.array([9007199254740993], pa.uint64()), pa.lib.ArrowInvalid),  # > 2^53
            (pa.array([18446744073709551615], pa.uint64()), pa.lib.ArrowInvalid),
        ]

        # ============================================================
        # FLOAT -> INTEGER conversions (only whole numbers succeed)
        # ============================================================

        # ============================================================
        # float16 -> integer
        # ============================================================
        matrix["float16"]["int8"] = [
            (
                pa.array([0.0, 1.0, -1.0, 127.0, -128.0, None], pa.float16()),
                pa.array([0, 1, -1, 127, -128, None], pa.int8()),
            ),
            (pa.array([1.5], pa.float16()), pa.lib.ArrowInvalid),  # fractional fails
            (pa.array([128.0], pa.float16()), pa.lib.ArrowInvalid),  # overflow
        ]
        matrix["float16"]["int16"] = [
            (
                pa.array([0.0, 1.0, -1.0, None], pa.float16()),
                pa.array([0, 1, -1, None], pa.int16()),
            ),
            (pa.array([1.5], pa.float16()), pa.lib.ArrowInvalid),
        ]
        matrix["float16"]["int32"] = [
            (
                pa.array([0.0, 1.0, -1.0, None], pa.float16()),
                pa.array([0, 1, -1, None], pa.int32()),
            ),
            (pa.array([1.5], pa.float16()), pa.lib.ArrowInvalid),
        ]
        matrix["float16"]["int64"] = [
            (
                pa.array([0.0, 1.0, -1.0, None], pa.float16()),
                pa.array([0, 1, -1, None], pa.int64()),
            ),
            (pa.array([1.5], pa.float16()), pa.lib.ArrowInvalid),
        ]
        matrix["float16"]["uint8"] = [
            (
                pa.array([0.0, 1.0, 255.0, None], pa.float16()),
                pa.array([0, 1, 255, None], pa.uint8()),
            ),
            (pa.array([-1.0], pa.float16()), pa.lib.ArrowInvalid),
        ]
        matrix["float16"]["uint16"] = [
            (pa.array([0.0, 1.0, None], pa.float16()), pa.array([0, 1, None], pa.uint16())),
            (pa.array([-1.0], pa.float16()), pa.lib.ArrowInvalid),
        ]
        matrix["float16"]["uint32"] = [
            (pa.array([0.0, 1.0, None], pa.float16()), pa.array([0, 1, None], pa.uint32())),
            (pa.array([-1.0], pa.float16()), pa.lib.ArrowInvalid),
        ]
        matrix["float16"]["uint64"] = [
            (pa.array([0.0, 1.0, None], pa.float16()), pa.array([0, 1, None], pa.uint64())),
            (pa.array([-1.0], pa.float16()), pa.lib.ArrowInvalid),
        ]

        # ============================================================
        # float32 -> integer
        # ============================================================
        for tgt in int_types:
            matrix["float32"][tgt] = [
                (
                    pa.array([0.0, 1.0, None], pa.float32()),
                    pa.array([0, 1, None], getattr(pa, tgt)()),
                ),
                (pa.array([1.5], pa.float32()), pa.lib.ArrowInvalid),  # fractional fails
            ]

        # ============================================================
        # float64 -> integer
        # ============================================================
        for tgt in int_types:
            matrix["float64"][tgt] = [
                (
                    pa.array([0.0, 1.0, None], pa.float64()),
                    pa.array([0, 1, None], getattr(pa, tgt)()),
                ),
                (pa.array([1.5], pa.float64()), pa.lib.ArrowInvalid),
            ]

        # Additional: NaN/Inf to integer fails
        matrix["float32"]["int32"].append(
            (pa.array([float("nan")], pa.float32()), pa.lib.ArrowInvalid)
        )
        matrix["float32"]["int32"].append(
            (pa.array([float("inf")], pa.float32()), pa.lib.ArrowInvalid)
        )
        matrix["float64"]["int64"].append(
            (pa.array([float("nan")], pa.float64()), pa.lib.ArrowInvalid)
        )

        # ============================================================
        # FLOAT -> FLOAT conversions (with precision loss and special values)
        # ============================================================

        # ============================================================
        # float16
        # ============================================================
        matrix["float16"]["float16"] = [
            (
                pa.array([0.0, 1.0, -1.0, 1.5, float("nan"), float("inf"), None], pa.float16()),
                pa.array([0.0, 1.0, -1.0, 1.5, float("nan"), float("inf"), None], pa.float16()),
            ),
        ]
        matrix["float16"]["float32"] = [
            (
                pa.array([0.0, 1.0, -1.0, 1.5, float("nan"), float("inf"), None], pa.float16()),
                pa.array([0.0, 1.0, -1.0, 1.5, float("nan"), float("inf"), None], pa.float32()),
            ),
        ]
        matrix["float16"]["float64"] = [
            (
                pa.array([0.0, 1.0, -1.0, 1.5, float("nan"), float("inf"), None], pa.float16()),
                pa.array([0.0, 1.0, -1.0, 1.5, float("nan"), float("inf"), None], pa.float64()),
            ),
        ]

        # ============================================================
        # float32
        # ============================================================
        matrix["float32"]["float16"] = [
            (
                pa.array([0.0, 1.0, -1.0, 1.5, None], pa.float32()),
                pa.array([0.0, 1.0, -1.0, 1.5, None], pa.float16()),
            ),
            (
                pa.array([1e20], pa.float32()),
                pa.array([float("inf")], pa.float16()),
            ),  # overflow to inf
        ]
        matrix["float32"]["float32"] = [
            (
                pa.array(
                    [0.0, 1.0, -1.0, 1.5, 1e20, float("nan"), float("inf"), None], pa.float32()
                ),
                pa.array(
                    [0.0, 1.0, -1.0, 1.5, 1e20, float("nan"), float("inf"), None], pa.float32()
                ),
            ),
            # Precision loss test: 1234.5678 in float32
            (pa.array([1234.5678], pa.float32()), pa.array([1234.5678], pa.float32())),
        ]
        matrix["float32"]["float64"] = [
            (
                pa.array([0.0, 1.0, -1.0, 1.5, float("nan"), float("inf"), None], pa.float32()),
                pa.array([0.0, 1.0, -1.0, 1.5, float("nan"), float("inf"), None], pa.float64()),
            ),
            # Precision is limited by float32 source
            (pa.array([1234.5678], pa.float32()), pa.array([1234.5677490234375], pa.float64())),
        ]

        # ============================================================
        # float64
        # ============================================================
        matrix["float64"]["float16"] = [
            (
                pa.array([0.0, 1.0, -1.0, 1.5, None], pa.float64()),
                pa.array([0.0, 1.0, -1.0, 1.5, None], pa.float16()),
            ),
            (pa.array([1e100], pa.float64()), pa.array([float("inf")], pa.float16())),  # overflow
        ]
        matrix["float64"]["float32"] = [
            (
                pa.array([0.0, 1.0, -1.0, 1.5, None], pa.float64()),
                pa.array([0.0, 1.0, -1.0, 1.5, None], pa.float32()),
            ),
            (pa.array([1e100], pa.float64()), pa.array([float("inf")], pa.float32())),
            # Large value precision loss
            (pa.array([1e20], pa.float64()), pa.array([1.0000000200408773e20], pa.float32())),
        ]
        matrix["float64"]["float64"] = [
            (
                pa.array(
                    [0.0, 1.0, -1.0, 1.5, 1e100, float("nan"), float("inf"), None], pa.float64()
                ),
                pa.array(
                    [0.0, 1.0, -1.0, 1.5, 1e100, float("nan"), float("inf"), None], pa.float64()
                ),
            ),
        ]

        # Special float values: subnormals
        matrix["float32"]["float64"].append(
            (
                pa.array([1.4e-45, 0.0, None], pa.float32()),
                pa.array([1.401298464324817e-45, 0.0, None], pa.float64()),
            )
        )

        # ============================================================
        # BOOLEAN conversions
        # ============================================================

        # ============================================================
        # bool
        # ============================================================
        matrix["bool"]["bool"] = [
            (pa.array([True, False, None], pa.bool_()), pa.array([True, False, None], pa.bool_())),
        ]

        # bool -> integers
        for tgt in int_types:
            matrix["bool"][tgt] = [
                (
                    pa.array([True, False, None], pa.bool_()),
                    pa.array([1, 0, None], getattr(pa, tgt)()),
                ),
            ]

        # bool -> float
        matrix["bool"]["float16"] = [
            (
                pa.array([True, False, None], pa.bool_()),
                pa.lib.ArrowNotImplementedError,
            ),  # Not supported!
        ]
        matrix["bool"]["float32"] = [
            (pa.array([True, False, None], pa.bool_()), pa.array([1.0, 0.0, None], pa.float32())),
        ]
        matrix["bool"]["float64"] = [
            (pa.array([True, False, None], pa.bool_()), pa.array([1.0, 0.0, None], pa.float64())),
        ]

        # bool -> string
        matrix["bool"]["string"] = [
            (
                pa.array([True, False, None], pa.bool_()),
                pa.array(["true", "false", None], pa.string()),
            ),
        ]
        matrix["bool"]["large_string"] = [
            (
                pa.array([True, False, None], pa.bool_()),
                pa.array(["true", "false", None], pa.large_string()),
            ),
        ]

        # ============================================================
        # integer -> bool
        # ============================================================
        # 0→False, non-zero→True
        for src in int_types:
            matrix[src]["bool"] = [
                (
                    pa.array([0, 1, 2, None], getattr(pa, src)()),
                    pa.array([False, True, True, None], pa.bool_()),
                ),
            ]

        # ============================================================
        # float -> bool
        # ============================================================
        matrix["float16"]["bool"] = [
            (pa.array([0.0, 1.0, None], pa.float16()), pa.lib.ArrowNotImplementedError),
        ]
        matrix["float32"]["bool"] = [
            (
                pa.array([0.0, 1.0, 1.5, None], pa.float32()),
                pa.array([False, True, True, None], pa.bool_()),
            ),
        ]
        matrix["float64"]["bool"] = [
            (
                pa.array([0.0, 1.0, 1.5, None], pa.float64()),
                pa.array([False, True, True, None], pa.bool_()),
            ),
        ]

        # ============================================================
        # STRING <-> STRING/BINARY conversions (with Unicode edge cases)
        # ============================================================

        # ============================================================
        # string
        # ============================================================
        matrix["string"]["string"] = [
            (
                pa.array(["hello", "123", "", "你好", None], pa.string()),
                pa.array(["hello", "123", "", "你好", None], pa.string()),
            ),
            # Multi-language strings
            (
                pa.array(["Hello", "こんにちは", "Привет", "مرحبا", None], pa.string()),
                pa.array(["Hello", "こんにちは", "Привет", "مرحبا", None], pa.string()),
            ),
            # Emojis
            (
                pa.array(["😀", "👨‍👩‍👧‍👦", "Hello 🌍", None], pa.string()),
                pa.array(["😀", "👨‍👩‍👧‍👦", "Hello 🌍", None], pa.string()),
            ),
            # Special characters
            (
                pa.array(["", " ", "\n", "\t", "\u200b", None], pa.string()),
                pa.array(["", " ", "\n", "\t", "\u200b", None], pa.string()),
            ),
        ]
        matrix["string"]["large_string"] = [
            (
                pa.array(["hello", "123", "", "你好", None], pa.string()),
                pa.array(["hello", "123", "", "你好", None], pa.large_string()),
            ),
        ]
        matrix["string"]["binary"] = [
            (
                pa.array(["hello", "", "你好", None], pa.string()),
                pa.array([b"hello", b"", "你好".encode(), None], pa.binary()),
            ),
            # Multi-language
            (
                pa.array(["Hello", "こんにちは", "Привет", None], pa.string()),
                pa.array([b"Hello", "こんにちは".encode(), "Привет".encode(), None], pa.binary()),
            ),
        ]
        matrix["string"]["large_binary"] = [
            (
                pa.array(["hello", "", None], pa.string()),
                pa.array([b"hello", b"", None], pa.large_binary()),
            ),
        ]

        # ============================================================
        # large_string
        # ============================================================
        matrix["large_string"]["string"] = [
            (
                pa.array(["hello", "123", "", "世界", None], pa.large_string()),
                pa.array(["hello", "123", "", "世界", None], pa.string()),
            ),
        ]
        matrix["large_string"]["large_string"] = [
            (
                pa.array(["hello", "123", "", "世界", None], pa.large_string()),
                pa.array(["hello", "123", "", "世界", None], pa.large_string()),
            ),
        ]
        matrix["large_string"]["binary"] = [
            (
                pa.array(["hello", "", None], pa.large_string()),
                pa.array([b"hello", b"", None], pa.binary()),
            ),
        ]
        matrix["large_string"]["large_binary"] = [
            (
                pa.array(["hello", "", None], pa.large_string()),
                pa.array([b"hello", b"", None], pa.large_binary()),
            ),
        ]

        # ============================================================
        # BINARY conversions
        # ============================================================

        # ============================================================
        # binary
        # ============================================================
        matrix["binary"]["binary"] = [
            (
                pa.array([b"hello", b"", b"\x00\xff", None], pa.binary()),
                pa.array([b"hello", b"", b"\x00\xff", None], pa.binary()),
            ),
        ]
        matrix["binary"]["large_binary"] = [
            (
                pa.array([b"hello", b"", None], pa.binary()),
                pa.array([b"hello", b"", None], pa.large_binary()),
            ),
        ]
        matrix["binary"]["string"] = [
            (
                pa.array([b"hello", b"", None], pa.binary()),
                pa.array(["hello", "", None], pa.string()),
            ),  # valid UTF-8
            (pa.array([b"\x00\xff"], pa.binary()), pa.lib.ArrowInvalid),  # invalid UTF-8
            (pa.array([b"\xff\xfe"], pa.binary()), pa.lib.ArrowInvalid),  # invalid UTF-8
        ]
        matrix["binary"]["large_string"] = [
            (pa.array([b"hello", None], pa.binary()), pa.array(["hello", None], pa.large_string())),
            (pa.array([b"\xff\xfe"], pa.binary()), pa.lib.ArrowInvalid),
        ]

        # ============================================================
        # large_binary
        # ============================================================
        matrix["large_binary"]["binary"] = [
            (
                pa.array([b"hello", b"", None], pa.large_binary()),
                pa.array([b"hello", b"", None], pa.binary()),
            ),
        ]
        matrix["large_binary"]["large_binary"] = [
            (
                pa.array([b"hello", b"", None], pa.large_binary()),
                pa.array([b"hello", b"", None], pa.large_binary()),
            ),
        ]
        matrix["large_binary"]["string"] = [
            (pa.array([b"hello", None], pa.large_binary()), pa.array(["hello", None], pa.string())),
            (pa.array([b"\xff"], pa.large_binary()), pa.lib.ArrowInvalid),
        ]
        matrix["large_binary"]["large_string"] = [
            (
                pa.array([b"hello", None], pa.large_binary()),
                pa.array(["hello", None], pa.large_string()),
            ),
            (pa.array([b"\xff"], pa.large_binary()), pa.lib.ArrowInvalid),
        ]

        # ============================================================
        # STRING -> NUMERIC
        # ============================================================

        # ============================================================
        # string -> numeric
        # ============================================================
        matrix["string"]["int8"] = [
            (
                pa.array(["0", "1", "-1", "127", None], pa.string()),
                pa.array([0, 1, -1, 127, None], pa.int8()),
            ),
            (pa.array(["hello"], pa.string()), pa.lib.ArrowInvalid),  # non-numeric fails
        ]
        matrix["string"]["int16"] = [
            (pa.array(["0", "1", "-1", None], pa.string()), pa.array([0, 1, -1, None], pa.int16())),
            (pa.array(["x"], pa.string()), pa.lib.ArrowInvalid),
        ]
        matrix["string"]["int32"] = [
            (pa.array(["0", "1", "-1", None], pa.string()), pa.array([0, 1, -1, None], pa.int32())),
            (pa.array(["x"], pa.string()), pa.lib.ArrowInvalid),
        ]
        matrix["string"]["int64"] = [
            (pa.array(["0", "1", "-1", None], pa.string()), pa.array([0, 1, -1, None], pa.int64())),
            (pa.array(["abc"], pa.string()), pa.lib.ArrowInvalid),
        ]
        matrix["string"]["uint8"] = [
            (pa.array(["0", "1", None], pa.string()), pa.array([0, 1, None], pa.uint8())),
            (pa.array(["x"], pa.string()), pa.lib.ArrowInvalid),
        ]
        matrix["string"]["uint16"] = [
            (pa.array(["0", "1", None], pa.string()), pa.array([0, 1, None], pa.uint16())),
            (pa.array(["x"], pa.string()), pa.lib.ArrowInvalid),
        ]
        matrix["string"]["uint32"] = [
            (pa.array(["0", "1", None], pa.string()), pa.array([0, 1, None], pa.uint32())),
            (pa.array(["x"], pa.string()), pa.lib.ArrowInvalid),
        ]
        matrix["string"]["uint64"] = [
            (pa.array(["0", "1", None], pa.string()), pa.array([0, 1, None], pa.uint64())),
            (pa.array(["x"], pa.string()), pa.lib.ArrowInvalid),
        ]
        matrix["string"]["float16"] = [
            (pa.array(["0", "1", None], pa.string()), pa.array([0.0, 1.0, None], pa.float16())),
            (pa.array(["x"], pa.string()), pa.lib.ArrowInvalid),
        ]
        matrix["string"]["float32"] = [
            (
                pa.array(["1.5", "2.5", "3.5", None], pa.string()),
                pa.array([1.5, 2.5, 3.5, None], pa.float32()),
            ),
            (pa.array(["x"], pa.string()), pa.lib.ArrowInvalid),
        ]
        matrix["string"]["float64"] = [
            (
                pa.array(["0", "1.5", "-1.0", None], pa.string()),
                pa.array([0.0, 1.5, -1.0, None], pa.float64()),
            ),
            (pa.array(["xyz"], pa.string()), pa.lib.ArrowInvalid),
        ]
        matrix["string"]["bool"] = [
            (
                pa.array(["true", "false", "1", "0", None], pa.string()),
                pa.array([True, False, True, False, None], pa.bool_()),
            ),
            (pa.array(["hello"], pa.string()), pa.lib.ArrowInvalid),
        ]

        # ============================================================
        # large_string -> numeric
        # ============================================================
        # (mostly fail on non-numeric)
        for tgt in int_types + ["float16", "float32", "float64"]:
            matrix["large_string"][tgt] = [
                (pa.array(["hello"], pa.large_string()), pa.lib.ArrowInvalid),
            ]
        matrix["large_string"]["bool"] = [
            (pa.array(["hello"], pa.large_string()), pa.lib.ArrowInvalid),
        ]

        # ============================================================
        # NUMERIC -> STRING
        # ============================================================

        # ============================================================
        # integer -> string
        # ============================================================
        for src in int_types:
            matrix[src]["string"] = [
                (
                    pa.array([0, 1, None], getattr(pa, src)()),
                    pa.array(["0", "1", None], pa.string()),
                ),
            ]
            matrix[src]["large_string"] = [
                (
                    pa.array([0, 1, None], getattr(pa, src)()),
                    pa.array(["0", "1", None], pa.large_string()),
                ),
            ]

        # ============================================================
        # float -> string
        # ============================================================
        # PyArrow omits decimal for whole numbers (0.0 -> "0")
        matrix["float32"]["string"] = [
            (pa.array([0.0, 1.5, None], pa.float32()), pa.array(["0", "1.5", None], pa.string())),
        ]
        matrix["float64"]["string"] = [
            (pa.array([0.0, 1.5, None], pa.float64()), pa.array(["0", "1.5", None], pa.string())),
        ]
        matrix["float16"]["string"] = [
            (pa.array([0.0, 1.0, None], pa.float16()), pa.array(["0", "1", None], pa.string())),
        ]
        matrix["float32"]["large_string"] = [
            (
                pa.array([0.0, 1.0, None], pa.float32()),
                pa.array(["0", "1", None], pa.large_string()),
            ),
        ]
        matrix["float64"]["large_string"] = [
            (
                pa.array([0.0, 1.0, None], pa.float64()),
                pa.array(["0", "1", None], pa.large_string()),
            ),
        ]
        matrix["float16"]["large_string"] = [
            (
                pa.array([0.0, 1.0, None], pa.float16()),
                pa.array(["0", "1", None], pa.large_string()),
            ),
        ]

        # ============================================================
        # NUMERIC -> BINARY (not supported)
        # ============================================================

        for src in int_types + ["float16", "float32", "float64", "bool"]:
            src_type = getattr(pa, src.replace("bool", "bool_"))()
            val = [0] if src != "bool" else [True]
            matrix[src]["binary"] = [
                (pa.array(val, src_type), pa.lib.ArrowNotImplementedError),
            ]
            matrix[src]["large_binary"] = [
                (pa.array(val, src_type), pa.lib.ArrowNotImplementedError),
            ]

        # ============================================================
        # BINARY -> NUMERIC (not supported)
        # ============================================================

        for tgt in int_types + ["float16", "float32", "float64", "bool"]:
            matrix["binary"][tgt] = [
                (pa.array([b"hello"], pa.binary()), pa.lib.ArrowInvalid),
            ]
            matrix["large_binary"][tgt] = [
                (pa.array([b"hello"], pa.large_binary()), pa.lib.ArrowInvalid),
            ]

        # ============================================================
        # DECIMAL conversions
        # ============================================================

        # ============================================================
        # numeric -> decimal
        # ============================================================
        matrix["int32"]["decimal128"] = [
            (
                pa.array([1, 2, 3, None], pa.int32()),
                pa.array(
                    [Decimal("1.00"), Decimal("2.00"), Decimal("3.00"), None], pa.decimal128(12, 2)
                ),
            ),
        ]
        matrix["int64"]["decimal128"] = [
            (
                pa.array([1, 2, 3, None], pa.int64()),
                pa.array(
                    [Decimal("1.00"), Decimal("2.00"), Decimal("3.00"), None], pa.decimal128(21, 2)
                ),
            ),
        ]
        matrix["float32"]["decimal128"] = [
            (
                pa.array([1.5, 2.5, 3.5, None], pa.float32()),
                pa.array(
                    [Decimal("1.50"), Decimal("2.50"), Decimal("3.50"), None], pa.decimal128(12, 2)
                ),
            ),
        ]
        matrix["float64"]["decimal128"] = [
            (
                pa.array([1.5, 2.5, 3.5, None], pa.float64()),
                pa.array(
                    [Decimal("1.50"), Decimal("2.50"), Decimal("3.50"), None], pa.decimal128(12, 2)
                ),
            ),
        ]
        matrix["string"]["decimal128"] = [
            (
                pa.array(["1.5", "2.5", "3.5", None], pa.string()),
                pa.array(
                    [Decimal("1.50"), Decimal("2.50"), Decimal("3.50"), None], pa.decimal128(12, 2)
                ),
            ),
        ]
        matrix["string"]["decimal256"] = [
            (
                pa.array(["1.5", "2.5", "3.5", None], pa.string()),
                pa.array(
                    [Decimal("1.50"), Decimal("2.50"), Decimal("3.50"), None], pa.decimal256(20, 2)
                ),
            ),
        ]

        # ============================================================
        # decimal128
        # ============================================================
        matrix["decimal128"]["int32"] = [
            (
                pa.array([Decimal("1"), Decimal("2"), Decimal("3"), None], pa.decimal128(10, 0)),
                pa.array([1, 2, 3, None], pa.int32()),
            ),
        ]
        matrix["decimal128"]["int64"] = [
            (
                pa.array([Decimal("1"), Decimal("2"), Decimal("3"), None], pa.decimal128(10, 0)),
                pa.array([1, 2, 3, None], pa.int64()),
            ),
        ]
        matrix["decimal128"]["float32"] = [
            (
                pa.array(
                    [Decimal("1.0"), Decimal("2.0"), Decimal("3.0"), None], pa.decimal128(10, 1)
                ),
                pa.array([1.0, 2.0, 3.0, None], pa.float32()),
            ),
        ]
        matrix["decimal128"]["float64"] = [
            (
                pa.array(
                    [Decimal("1.0"), Decimal("2.0"), Decimal("3.0"), None], pa.decimal128(10, 1)
                ),
                pa.array([1.0, 2.0, 3.0, None], pa.float64()),
            ),
        ]

        # ============================================================
        # decimal128 <-> decimal256
        # ============================================================
        matrix["decimal128"]["decimal256"] = [
            (
                pa.array(
                    [Decimal("1.00"), Decimal("2.00"), Decimal("3.00"), None], pa.decimal128(10, 2)
                ),
                pa.array(
                    [Decimal("1.00"), Decimal("2.00"), Decimal("3.00"), None], pa.decimal256(20, 2)
                ),
            ),
        ]
        matrix["decimal256"]["decimal128"] = [
            (
                pa.array(
                    [Decimal("1.00"), Decimal("2.00"), Decimal("3.00"), None], pa.decimal256(20, 2)
                ),
                pa.array(
                    [Decimal("1.00"), Decimal("2.00"), Decimal("3.00"), None], pa.decimal128(10, 2)
                ),
            ),
        ]

        # ============================================================
        # TEMPORAL: DATE conversions
        # ============================================================

        epoch_date = date(1970, 1, 1)
        leap_day = date(2024, 2, 29)
        far_future = date(2100, 12, 31)
        before_epoch = date(1969, 12, 31)

        # ============================================================
        # date32
        # ============================================================
        matrix["date32"]["date32"] = [
            (
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date32()),
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date32()),
            ),
            (
                pa.array([epoch_date, before_epoch, leap_day, far_future, None], pa.date32()),
                pa.array([epoch_date, before_epoch, leap_day, far_future, None], pa.date32()),
            ),
        ]
        matrix["date32"]["date64"] = [
            (
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date32()),
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date64()),
            ),
        ]
        matrix["date32"]["string"] = [
            (
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date32()),
                pa.array(["2024-01-01", "2024-12-31", None], pa.string()),
            ),
        ]
        matrix["date32"]["timestamp_s"] = [
            (
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date32()),
                pa.array([datetime(2024, 1, 1), datetime(2024, 12, 31), None], pa.timestamp("s")),
            ),
        ]

        # ============================================================
        # date64
        # ============================================================
        matrix["date64"]["date32"] = [
            (
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date64()),
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date32()),
            ),
        ]
        matrix["date64"]["date64"] = [
            (
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date64()),
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date64()),
            ),
        ]
        matrix["date64"]["string"] = [
            (
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date64()),
                pa.array(["2024-01-01", "2024-12-31", None], pa.string()),
            ),
        ]
        matrix["date64"]["timestamp_ms"] = [
            (
                pa.array([date(2024, 1, 1), date(2024, 12, 31), None], pa.date64()),
                pa.array([datetime(2024, 1, 1), datetime(2024, 12, 31), None], pa.timestamp("ms")),
            ),
        ]

        # ============================================================
        # TEMPORAL: TIMESTAMP unit conversions (precision hierarchy: s < ms < us < ns)
        # ============================================================

        epoch = datetime(1970, 1, 1, 0, 0, 0)
        before_epoch_ts = datetime(1969, 12, 31, 23, 59, 59)
        with_micros = datetime(2024, 1, 1, 12, 30, 45, 123456)
        far_future_ts = datetime(2100, 12, 31, 23, 59, 59)

        # ============================================================
        # timestamp_s
        # ============================================================
        matrix["timestamp_s"]["timestamp_s"] = [
            (
                pa.array([epoch, before_epoch_ts, far_future_ts, None], pa.timestamp("s")),
                pa.array([epoch, before_epoch_ts, far_future_ts, None], pa.timestamp("s")),
            ),
        ]
        matrix["timestamp_s"]["timestamp_ms"] = [
            (
                pa.array([epoch, before_epoch_ts, None], pa.timestamp("s")),
                pa.array([epoch, before_epoch_ts, None], pa.timestamp("ms")),
            ),  # upcast safe
        ]
        matrix["timestamp_s"]["timestamp_us"] = [
            (
                pa.array([epoch, None], pa.timestamp("s")),
                pa.array([epoch, None], pa.timestamp("us")),
            ),
        ]
        matrix["timestamp_s"]["timestamp_ns"] = [
            (
                pa.array([epoch, None], pa.timestamp("s")),
                pa.array([epoch, None], pa.timestamp("ns")),
            ),
        ]

        # ============================================================
        # timestamp_ms
        # ============================================================
        # downcast to s fails if fractional
        matrix["timestamp_ms"]["timestamp_s"] = [
            (
                pa.array([with_micros, None], pa.timestamp("ms")),
                pa.lib.ArrowInvalid,
            ),  # has 123ms fraction
        ]
        matrix["timestamp_ms"]["timestamp_ms"] = [
            (
                pa.array([with_micros, None], pa.timestamp("ms")),
                pa.array([with_micros, None], pa.timestamp("ms")),
            ),
        ]
        matrix["timestamp_ms"]["timestamp_us"] = [
            # ms truncates microseconds: 123456us -> 123ms = 123000us
            (
                pa.array([with_micros, None], pa.timestamp("ms")),
                pa.array([datetime(2024, 1, 1, 12, 30, 45, 123000), None], pa.timestamp("us")),
            ),
        ]
        matrix["timestamp_ms"]["timestamp_ns"] = [
            (
                pa.array([with_micros, None], pa.timestamp("ms")),
                pa.array([datetime(2024, 1, 1, 12, 30, 45, 123000), None], pa.timestamp("ns")),
            ),
        ]

        # ============================================================
        # timestamp_us
        # ============================================================
        # downcast to s/ms fails if fractional microseconds
        matrix["timestamp_us"]["timestamp_s"] = [
            (
                pa.array([with_micros, None], pa.timestamp("us")),
                pa.lib.ArrowInvalid,
            ),  # loses .123456 seconds
        ]
        matrix["timestamp_us"]["timestamp_ms"] = [
            (pa.array([with_micros, None], pa.timestamp("us")), pa.lib.ArrowInvalid),  # loses 456us
        ]
        matrix["timestamp_us"]["timestamp_us"] = [
            (
                pa.array([with_micros, None], pa.timestamp("us")),
                pa.array([with_micros, None], pa.timestamp("us")),
            ),
        ]
        matrix["timestamp_us"]["timestamp_ns"] = [
            (
                pa.array([with_micros, None], pa.timestamp("us")),
                pa.array([with_micros, None], pa.timestamp("ns")),
            ),
        ]

        # ============================================================
        # timestamp_ns
        # ============================================================
        # downcast fails if fractional nanoseconds
        matrix["timestamp_ns"]["timestamp_s"] = [
            (pa.array([1234567890123456789, None], pa.timestamp("ns")), pa.lib.ArrowInvalid),
        ]
        matrix["timestamp_ns"]["timestamp_ms"] = [
            (pa.array([1234567890123456789, None], pa.timestamp("ns")), pa.lib.ArrowInvalid),
        ]
        matrix["timestamp_ns"]["timestamp_us"] = [
            (
                pa.array([1234567890123456789, None], pa.timestamp("ns")),
                pa.lib.ArrowInvalid,
            ),  # loses 789ns
        ]
        matrix["timestamp_ns"]["timestamp_ns"] = [
            (
                pa.array([1234567890123456789, None], pa.timestamp("ns")),
                pa.array([1234567890123456789, None], pa.timestamp("ns")),
            ),
        ]

        # ============================================================
        # timestamp -> date
        # ============================================================
        # extracts date part
        matrix["timestamp_s"]["date32"] = [
            (
                pa.array([epoch, datetime(2024, 6, 15, 23, 59, 59), None], pa.timestamp("s")),
                pa.array([epoch_date, date(2024, 6, 15), None], pa.date32()),
            ),
        ]
        matrix["timestamp_ms"]["date32"] = [
            (
                pa.array([epoch, None], pa.timestamp("ms")),
                pa.array([epoch_date, None], pa.date32()),
            ),
        ]

        # ============================================================
        # TEMPORAL: DURATION conversions (s < ms < us < ns)
        # ============================================================

        # ============================================================
        # duration_s
        # ============================================================
        matrix["duration_s"]["duration_s"] = [
            (pa.array([86400, None], pa.duration("s")), pa.array([86400, None], pa.duration("s"))),
        ]
        matrix["duration_s"]["duration_ms"] = [
            (
                pa.array([86400, None], pa.duration("s")),
                pa.array([86400000, None], pa.duration("ms")),
            ),
        ]
        matrix["duration_s"]["duration_us"] = [
            (
                pa.array([86400, None], pa.duration("s")),
                pa.array([86400000000, None], pa.duration("us")),
            ),
        ]
        matrix["duration_s"]["duration_ns"] = [
            (
                pa.array([86400, None], pa.duration("s")),
                pa.array([86400000000000, None], pa.duration("ns")),
            ),
        ]

        # ============================================================
        # duration_ms
        # ============================================================
        # downcast to s fails if fractional
        matrix["duration_ms"]["duration_s"] = [
            (pa.array([1500, None], pa.duration("ms")), pa.lib.ArrowInvalid),  # 1.5s loses 500ms
        ]
        matrix["duration_ms"]["duration_ms"] = [
            (pa.array([1500, None], pa.duration("ms")), pa.array([1500, None], pa.duration("ms"))),
        ]
        matrix["duration_ms"]["duration_us"] = [
            (
                pa.array([1500, None], pa.duration("ms")),
                pa.array([1500000, None], pa.duration("us")),
            ),
        ]
        matrix["duration_ms"]["duration_ns"] = [
            (
                pa.array([1500, None], pa.duration("ms")),
                pa.array([1500000000, None], pa.duration("ns")),
            ),
        ]

        # ============================================================
        # duration_us
        # ============================================================
        matrix["duration_us"]["duration_s"] = [
            (pa.array([1500456, None], pa.duration("us")), pa.lib.ArrowInvalid),
        ]
        matrix["duration_us"]["duration_ms"] = [
            (pa.array([1500456, None], pa.duration("us")), pa.lib.ArrowInvalid),  # loses 456us
        ]
        matrix["duration_us"]["duration_us"] = [
            (
                pa.array([1500456, None], pa.duration("us")),
                pa.array([1500456, None], pa.duration("us")),
            ),
        ]
        matrix["duration_us"]["duration_ns"] = [
            (
                pa.array([1500456, None], pa.duration("us")),
                pa.array([1500456000, None], pa.duration("ns")),
            ),
        ]

        # ============================================================
        # duration_ns
        # ============================================================
        matrix["duration_ns"]["duration_s"] = [
            (pa.array([1500456789, None], pa.duration("ns")), pa.lib.ArrowInvalid),
        ]
        matrix["duration_ns"]["duration_ms"] = [
            (pa.array([1500456789, None], pa.duration("ns")), pa.lib.ArrowInvalid),
        ]
        matrix["duration_ns"]["duration_us"] = [
            (pa.array([1500456789, None], pa.duration("ns")), pa.lib.ArrowInvalid),  # loses 789ns
        ]
        matrix["duration_ns"]["duration_ns"] = [
            (
                pa.array([1500456789, None], pa.duration("ns")),
                pa.array([1500456789, None], pa.duration("ns")),
            ),
        ]

        # ============================================================
        # TEMPORAL: TIME conversions (time32[s/ms] vs time64[us/ns])
        # ============================================================

        # ============================================================
        # time32_s
        # ============================================================
        # 12:00:00
        matrix["time32_s"]["time32_s"] = [
            (pa.array([43200, None], pa.time32("s")), pa.array([43200, None], pa.time32("s"))),
        ]
        matrix["time32_s"]["time32_ms"] = [
            (pa.array([43200, None], pa.time32("s")), pa.array([43200000, None], pa.time32("ms"))),
        ]
        matrix["time32_s"]["time64_us"] = [
            (
                pa.array([43200, None], pa.time32("s")),
                pa.array([43200000000, None], pa.time64("us")),
            ),
        ]
        matrix["time32_s"]["time64_ns"] = [
            (
                pa.array([43200, None], pa.time32("s")),
                pa.array([43200000000000, None], pa.time64("ns")),
            ),
        ]

        # ============================================================
        # time32_ms
        # ============================================================
        # 12:00:00.123
        matrix["time32_ms"]["time32_s"] = [
            (pa.array([43200123, None], pa.time32("ms")), pa.lib.ArrowInvalid),  # loses 123ms
        ]
        matrix["time32_ms"]["time32_ms"] = [
            (
                pa.array([43200123, None], pa.time32("ms")),
                pa.array([43200123, None], pa.time32("ms")),
            ),
        ]
        matrix["time32_ms"]["time64_us"] = [
            (
                pa.array([43200123, None], pa.time32("ms")),
                pa.array([43200123000, None], pa.time64("us")),
            ),
        ]
        matrix["time32_ms"]["time64_ns"] = [
            (
                pa.array([43200123, None], pa.time32("ms")),
                pa.array([43200123000000, None], pa.time64("ns")),
            ),
        ]

        # ============================================================
        # time64_us
        # ============================================================
        # 12:00:00.123456
        matrix["time64_us"]["time32_s"] = [
            (pa.array([43200123456, None], pa.time64("us")), pa.lib.ArrowInvalid),
        ]
        matrix["time64_us"]["time32_ms"] = [
            (pa.array([43200123456, None], pa.time64("us")), pa.lib.ArrowInvalid),  # loses 456us
        ]
        matrix["time64_us"]["time64_us"] = [
            (
                pa.array([43200123456, None], pa.time64("us")),
                pa.array([43200123456, None], pa.time64("us")),
            ),
        ]
        matrix["time64_us"]["time64_ns"] = [
            (
                pa.array([43200123456, None], pa.time64("us")),
                pa.array([43200123456000, None], pa.time64("ns")),
            ),
        ]

        # ============================================================
        # time64_ns
        # ============================================================
        # 12:00:00.123456789
        matrix["time64_ns"]["time32_s"] = [
            (pa.array([43200123456789, None], pa.time64("ns")), pa.lib.ArrowInvalid),
        ]
        matrix["time64_ns"]["time32_ms"] = [
            (pa.array([43200123456789, None], pa.time64("ns")), pa.lib.ArrowInvalid),
        ]
        matrix["time64_ns"]["time64_us"] = [
            (pa.array([43200123456789, None], pa.time64("ns")), pa.lib.ArrowInvalid),  # loses 789ns
        ]
        matrix["time64_ns"]["time64_ns"] = [
            (
                pa.array([43200123456789, None], pa.time64("ns")),
                pa.array([43200123456789, None], pa.time64("ns")),
            ),
        ]

        # ============================================================
        # LIST type conversions
        # ============================================================

        # ============================================================
        # list_int32
        # ============================================================
        matrix["list_int32"]["list_int32"] = [
            (
                pa.array([[1, 2, 3], [4, 5], None], pa.list_(pa.int32())),
                pa.array([[1, 2, 3], [4, 5], None], pa.list_(pa.int32())),
            ),
        ]
        matrix["list_int32"]["list_int64"] = [
            (
                pa.array([[1, 2, 3], [4, 5], None], pa.list_(pa.int32())),
                pa.array([[1, 2, 3], [4, 5], None], pa.list_(pa.int64())),
            ),
        ]
        matrix["list_int32"]["list_float64"] = [
            (
                pa.array([[1, 2, 3], [4, 5], None], pa.list_(pa.int32())),
                pa.array([[1.0, 2.0, 3.0], [4.0, 5.0], None], pa.list_(pa.float64())),
            ),
        ]
        matrix["list_int32"]["large_list_int32"] = [
            (
                pa.array([[1, 2, 3], [4, 5], None], pa.list_(pa.int32())),
                pa.array([[1, 2, 3], [4, 5], None], pa.large_list(pa.int32())),
            ),
        ]
        matrix["list_int32"]["fixed_size_list_int32"] = [
            (
                pa.array([[1, 2, 3], [4, 5, 6], None], pa.list_(pa.int32())),
                pa.array([[1, 2, 3], [4, 5, 6], None], pa.list_(pa.int32(), 3)),
            ),
        ]

        # ============================================================
        # large_list_int32
        # ============================================================
        matrix["large_list_int32"]["list_int32"] = [
            (
                pa.array([[1, 2, 3], [4, 5], None], pa.large_list(pa.int32())),
                pa.array([[1, 2, 3], [4, 5], None], pa.list_(pa.int32())),
            ),
        ]
        matrix["large_list_int32"]["large_list_int32"] = [
            (
                pa.array([[1, 2, 3], [4, 5], None], pa.large_list(pa.int32())),
                pa.array([[1, 2, 3], [4, 5], None], pa.large_list(pa.int32())),
            ),
        ]

        # ============================================================
        # STRUCT type conversions
        # ============================================================

        # ============================================================
        # struct_ab_int32
        # ============================================================
        matrix["struct_ab_int32"]["struct_ab_int32"] = [
            (
                pa.array(
                    [{"a": 1, "b": 2}, {"a": 3, "b": 4}, None],
                    pa.struct([("a", pa.int32()), ("b", pa.int32())]),
                ),
                pa.array(
                    [{"a": 1, "b": 2}, {"a": 3, "b": 4}, None],
                    pa.struct([("a", pa.int32()), ("b", pa.int32())]),
                ),
            ),
        ]
        matrix["struct_ab_int32"]["struct_ab_int64"] = [
            (
                pa.array(
                    [{"a": 1, "b": 2}, {"a": 3, "b": 4}, None],
                    pa.struct([("a", pa.int32()), ("b", pa.int32())]),
                ),
                pa.array(
                    [{"a": 1, "b": 2}, {"a": 3, "b": 4}, None],
                    pa.struct([("a", pa.int64()), ("b", pa.int64())]),
                ),
            ),
        ]
        matrix["struct_ab_int32"]["struct_ab_float64"] = [
            (
                pa.array(
                    [{"a": 1, "b": 2}, {"a": 3, "b": 4}, None],
                    pa.struct([("a", pa.int32()), ("b", pa.int32())]),
                ),
                pa.array(
                    [{"a": 1.0, "b": 2.0}, {"a": 3.0, "b": 4.0}, None],
                    pa.struct([("a", pa.float64()), ("b", pa.float64())]),
                ),
            ),
        ]

        # ============================================================
        # MAP type conversions
        # ============================================================

        # ============================================================
        # map_str_int32
        # ============================================================
        matrix["map_str_int32"]["map_str_int32"] = [
            (
                pa.array(
                    [[("a", 1), ("b", 2)], [("c", 3)], None], pa.map_(pa.string(), pa.int32())
                ),
                pa.array(
                    [[("a", 1), ("b", 2)], [("c", 3)], None], pa.map_(pa.string(), pa.int32())
                ),
            ),
        ]
        matrix["map_str_int32"]["map_str_int64"] = [
            (
                pa.array(
                    [[("a", 1), ("b", 2)], [("c", 3)], None], pa.map_(pa.string(), pa.int32())
                ),
                pa.array(
                    [[("a", 1), ("b", 2)], [("c", 3)], None], pa.map_(pa.string(), pa.int64())
                ),
            ),
        ]
        matrix["map_str_int32"]["map_str_float64"] = [
            (
                pa.array(
                    [[("a", 1), ("b", 2)], [("c", 3)], None], pa.map_(pa.string(), pa.int32())
                ),
                pa.array(
                    [[("a", 1.0), ("b", 2.0)], [("c", 3.0)], None],
                    pa.map_(pa.string(), pa.float64()),
                ),
            ),
        ]

        # ============================================================
        # DEEPLY NESTED type conversions
        # ============================================================

        # ============================================================
        # nested_list_int32
        # ============================================================
        # list<list<int32>>
        matrix["nested_list_int32"]["nested_list_int32"] = [
            (
                pa.array([[[1, 2], [3, 4]], [[5, 6]], None], pa.list_(pa.list_(pa.int32()))),
                pa.array([[[1, 2], [3, 4]], [[5, 6]], None], pa.list_(pa.list_(pa.int32()))),
            ),
        ]
        matrix["nested_list_int32"]["nested_list_int64"] = [
            (
                pa.array([[[1, 2], [3, 4]], [[5, 6]], None], pa.list_(pa.list_(pa.int32()))),
                pa.array([[[1, 2], [3, 4]], [[5, 6]], None], pa.list_(pa.list_(pa.int64()))),
            ),
        ]
        matrix["nested_list_int32"]["nested_list_float64"] = [
            (
                pa.array([[[1, 2], [3, 4]], [[5, 6]], None], pa.list_(pa.list_(pa.int32()))),
                pa.array(
                    [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0]], None], pa.list_(pa.list_(pa.float64()))
                ),
            ),
        ]

        # ============================================================
        # nested_struct_int32
        # ============================================================
        # struct<a: struct<b: int32>>
        matrix["nested_struct_int32"]["nested_struct_int32"] = [
            (
                pa.array(
                    [{"a": {"b": 1}}, {"a": {"b": 2}}, None],
                    pa.struct([("a", pa.struct([("b", pa.int32())]))]),
                ),
                pa.array(
                    [{"a": {"b": 1}}, {"a": {"b": 2}}, None],
                    pa.struct([("a", pa.struct([("b", pa.int32())]))]),
                ),
            ),
        ]
        matrix["nested_struct_int32"]["nested_struct_int64"] = [
            (
                pa.array(
                    [{"a": {"b": 1}}, {"a": {"b": 2}}, None],
                    pa.struct([("a", pa.struct([("b", pa.int32())]))]),
                ),
                pa.array(
                    [{"a": {"b": 1}}, {"a": {"b": 2}}, None],
                    pa.struct([("a", pa.struct([("b", pa.int64())]))]),
                ),
            ),
        ]

        # ============================================================
        # list_of_struct_int32
        # ============================================================
        # list<struct<a: int32>>
        matrix["list_of_struct_int32"]["list_of_struct_int32"] = [
            (
                pa.array(
                    [[{"a": 1}, {"a": 2}], [{"a": 3}], None],
                    pa.list_(pa.struct([("a", pa.int32())])),
                ),
                pa.array(
                    [[{"a": 1}, {"a": 2}], [{"a": 3}], None],
                    pa.list_(pa.struct([("a", pa.int32())])),
                ),
            ),
        ]
        matrix["list_of_struct_int32"]["list_of_struct_int64"] = [
            (
                pa.array(
                    [[{"a": 1}, {"a": 2}], [{"a": 3}], None],
                    pa.list_(pa.struct([("a", pa.int32())])),
                ),
                pa.array(
                    [[{"a": 1}, {"a": 2}], [{"a": 3}], None],
                    pa.list_(pa.struct([("a", pa.int64())])),
                ),
            ),
        ]

        # ============================================================
        # NULL and EMPTY arrays
        # ============================================================

        # ============================================================
        # int8_nulls
        # ============================================================
        # All nulls - should succeed for any type conversion
        matrix["int8_nulls"]["int64"] = [
            (pa.array([None, None, None], pa.int8()), pa.array([None, None, None], pa.int64())),
        ]
        matrix["int8_nulls"]["float64"] = [
            (pa.array([None, None, None], pa.int8()), pa.array([None, None, None], pa.float64())),
        ]
        matrix["int8_nulls"]["string"] = [
            (pa.array([None, None, None], pa.int8()), pa.array([None, None, None], pa.string())),
        ]
        # ============================================================
        # int32_nulls
        # ============================================================
        matrix["int32_nulls"]["int64"] = [
            (pa.array([None, None, None], pa.int32()), pa.array([None, None, None], pa.int64())),
        ]
        matrix["int32_nulls"]["float64"] = [
            (pa.array([None, None, None], pa.int32()), pa.array([None, None, None], pa.float64())),
        ]
        # ============================================================
        # float64_nulls
        # ============================================================
        matrix["float64_nulls"]["int32"] = [
            (pa.array([None, None, None], pa.float64()), pa.array([None, None, None], pa.int32())),
        ]
        matrix["float64_nulls"]["string"] = [
            (pa.array([None, None, None], pa.float64()), pa.array([None, None, None], pa.string())),
        ]

        # ============================================================
        # int8_empty
        # ============================================================
        matrix["int8_empty"]["int64"] = [
            (pa.array([], pa.int8()), pa.array([], pa.int64())),
        ]
        matrix["int8_empty"]["float64"] = [
            (pa.array([], pa.int8()), pa.array([], pa.float64())),
        ]
        # ============================================================
        # int32_empty
        # ============================================================
        matrix["int32_empty"]["float64"] = [
            (pa.array([], pa.int32()), pa.array([], pa.float64())),
        ]
        matrix["int32_empty"]["string"] = [
            (pa.array([], pa.int32()), pa.array([], pa.string())),
        ]
        # ============================================================
        # string_empty
        # ============================================================
        matrix["string_empty"]["int32"] = [
            (pa.array([], pa.string()), pa.array([], pa.int32())),
        ]

        # ============================================================
        # float64_empty
        # ============================================================
        matrix["float64_empty"]["string"] = [
            (pa.array([], pa.float64()), pa.array([], pa.string())),
        ]

        return dict(matrix)

    @classmethod
    def _get_pa_type(cls, pa, type_name):
        """Map type name string to PyArrow type.

        Returns None for complex types that need special handling.
        """
        # Simple types
        simple_types = {
            "int8": pa.int8(),
            "int16": pa.int16(),
            "int32": pa.int32(),
            "int64": pa.int64(),
            "uint8": pa.uint8(),
            "uint16": pa.uint16(),
            "uint32": pa.uint32(),
            "uint64": pa.uint64(),
            "float16": pa.float16(),
            "float32": pa.float32(),
            "float64": pa.float64(),
            "bool": pa.bool_(),
            "string": pa.string(),
            "large_string": pa.large_string(),
            "binary": pa.binary(),
            "large_binary": pa.large_binary(),
            "date32": pa.date32(),
            "date64": pa.date64(),
        }

        if type_name in simple_types:
            return simple_types[type_name]

        # Temporal types with units
        temporal_mappings = {
            "timestamp_s": pa.timestamp("s"),
            "timestamp_ms": pa.timestamp("ms"),
            "timestamp_us": pa.timestamp("us"),
            "timestamp_ns": pa.timestamp("ns"),
            "duration_s": pa.duration("s"),
            "duration_ms": pa.duration("ms"),
            "duration_us": pa.duration("us"),
            "duration_ns": pa.duration("ns"),
            "time32_s": pa.time32("s"),
            "time32_ms": pa.time32("ms"),
            "time64_us": pa.time64("us"),
            "time64_ns": pa.time64("ns"),
        }

        if type_name in temporal_mappings:
            return temporal_mappings[type_name]

        # Complex/nested types - return None to skip
        return None

    def _arrays_equal_nan_aware(self, arr1, arr2):
        """Compare two PyArrow arrays, treating NaN as equal to NaN."""
        import math

        if len(arr1) != len(arr2) or arr1.type != arr2.type:
            return False

        for i in range(len(arr1)):
            v1, v2 = arr1[i].as_py(), arr2[i].as_py()
            if v1 is None and v2 is None:
                continue
            if v1 is None or v2 is None:
                return False
            if isinstance(v1, float) and isinstance(v2, float):
                if math.isnan(v1) and math.isnan(v2):
                    continue
                if math.isnan(v1) or math.isnan(v2):
                    return False
            if v1 != v2:
                return False
        return True

    def _run_cast_tests(self, source_type):
        """Helper to run cast tests for a single source type.

        Args:
            source_type: Source type name to test (e.g., "int8", "float32")
        """
        import pyarrow as pa

        if source_type not in self.CAST_MATRIX:
            self.skipTest(f"No test cases for {source_type}")
            return

        failed_cases = []
        targets = self.CAST_MATRIX[source_type]

        for tgt_type, test_pairs in targets.items():
            # test_pairs is a list of (source_arr, expected) tuples
            for i, (src_arr, expected) in enumerate(test_pairs):
                case_id = f"{source_type}->{tgt_type}[{i}]"

                # Determine target PyArrow type
                if isinstance(expected, pa.Array):
                    tgt_pa_type = expected.type
                else:
                    # For exceptions, derive target type from name using type mapping
                    tgt_pa_type = self._get_pa_type(pa, tgt_type)
                    if tgt_pa_type is None:
                        # Skip types we can't map
                        continue

                try:
                    if isinstance(expected, type) and issubclass(expected, Exception):
                        # Expect exception
                        try:
                            result = src_arr.cast(tgt_pa_type)
                            failed_cases.append(
                                f"{case_id}: expected {expected.__name__}, got success: {result.to_pylist()}"
                            )
                        except expected:
                            pass  # Expected exception occurred
                        except Exception as e:
                            failed_cases.append(
                                f"{case_id}: expected {expected.__name__}, got {type(e).__name__}: {e}"
                            )
                    elif isinstance(expected, pa.Array):
                        # Expect success
                        try:
                            result = src_arr.cast(tgt_pa_type)
                            if not self._arrays_equal_nan_aware(result, expected):
                                failed_cases.append(
                                    f"{case_id}: mismatch\n"
                                    f"  source: {src_arr.to_pylist()}\n"
                                    f"  expected: {expected.to_pylist()}\n"
                                    f"  got: {result.to_pylist()}"
                                )
                        except Exception as e:
                            failed_cases.append(
                                f"{case_id}: expected success, got {type(e).__name__}: {e}"
                            )
                except Exception as e:
                    failed_cases.append(f"{case_id}: test error: {e}")

        self.assertEqual(
            len(failed_cases),
            0,
            f"\n{source_type}: {len(failed_cases)} cast tests failed:\n"
            + "\n".join(failed_cases[:10]),
        )

    # ============================================================
    # INTEGER source type tests
    # ============================================================

    def test_int8_casts(self):
        """Test int8 -> all target types."""
        self._run_cast_tests("int8")

    def test_int16_casts(self):
        """Test int16 -> all target types."""
        self._run_cast_tests("int16")

    def test_int32_casts(self):
        """Test int32 -> all target types."""
        self._run_cast_tests("int32")

    def test_int64_casts(self):
        """Test int64 -> all target types."""
        self._run_cast_tests("int64")

    def test_uint8_casts(self):
        """Test uint8 -> all target types."""
        self._run_cast_tests("uint8")

    def test_uint16_casts(self):
        """Test uint16 -> all target types."""
        self._run_cast_tests("uint16")

    def test_uint32_casts(self):
        """Test uint32 -> all target types."""
        self._run_cast_tests("uint32")

    def test_uint64_casts(self):
        """Test uint64 -> all target types."""
        self._run_cast_tests("uint64")

    # ============================================================
    # FLOAT source type tests
    # ============================================================

    def test_float16_casts(self):
        """Test float16 -> all target types."""
        self._run_cast_tests("float16")

    def test_float32_casts(self):
        """Test float32 -> all target types."""
        self._run_cast_tests("float32")

    def test_float64_casts(self):
        """Test float64 -> all target types."""
        self._run_cast_tests("float64")

    # ============================================================
    # BOOLEAN source type tests
    # ============================================================

    def test_bool_casts(self):
        """Test bool -> all target types."""
        self._run_cast_tests("bool")

    # ============================================================
    # STRING/BINARY source type tests
    # ============================================================

    def test_string_casts(self):
        """Test string -> all target types."""
        self._run_cast_tests("string")

    def test_large_string_casts(self):
        """Test large_string -> all target types."""
        self._run_cast_tests("large_string")

    def test_binary_casts(self):
        """Test binary -> all target types."""
        self._run_cast_tests("binary")

    def test_large_binary_casts(self):
        """Test large_binary -> all target types."""
        self._run_cast_tests("large_binary")

    # ============================================================
    # DECIMAL source type tests
    # ============================================================

    def test_decimal128_casts(self):
        """Test decimal128 -> all target types."""
        self._run_cast_tests("decimal128")

    def test_decimal256_casts(self):
        """Test decimal256 -> all target types."""
        self._run_cast_tests("decimal256")

    # ============================================================
    # TEMPORAL: DATE source type tests
    # ============================================================

    def test_date32_casts(self):
        """Test date32 -> all target types."""
        self._run_cast_tests("date32")

    def test_date64_casts(self):
        """Test date64 -> all target types."""
        self._run_cast_tests("date64")

    # ============================================================
    # TEMPORAL: TIMESTAMP source type tests
    # ============================================================

    def test_timestamp_s_casts(self):
        """Test timestamp[s] -> all target types."""
        self._run_cast_tests("timestamp_s")

    def test_timestamp_ms_casts(self):
        """Test timestamp[ms] -> all target types."""
        self._run_cast_tests("timestamp_ms")

    def test_timestamp_us_casts(self):
        """Test timestamp[us] -> all target types."""
        self._run_cast_tests("timestamp_us")

    def test_timestamp_ns_casts(self):
        """Test timestamp[ns] -> all target types."""
        self._run_cast_tests("timestamp_ns")

    # ============================================================
    # TEMPORAL: DURATION source type tests
    # ============================================================

    def test_duration_s_casts(self):
        """Test duration[s] -> all target types."""
        self._run_cast_tests("duration_s")

    def test_duration_ms_casts(self):
        """Test duration[ms] -> all target types."""
        self._run_cast_tests("duration_ms")

    def test_duration_us_casts(self):
        """Test duration[us] -> all target types."""
        self._run_cast_tests("duration_us")

    def test_duration_ns_casts(self):
        """Test duration[ns] -> all target types."""
        self._run_cast_tests("duration_ns")

    # ============================================================
    # TEMPORAL: TIME source type tests
    # ============================================================

    def test_time32_s_casts(self):
        """Test time32[s] -> all target types."""
        self._run_cast_tests("time32_s")

    def test_time32_ms_casts(self):
        """Test time32[ms] -> all target types."""
        self._run_cast_tests("time32_ms")

    def test_time64_us_casts(self):
        """Test time64[us] -> all target types."""
        self._run_cast_tests("time64_us")

    def test_time64_ns_casts(self):
        """Test time64[ns] -> all target types."""
        self._run_cast_tests("time64_ns")

    # ============================================================
    # COMPLEX: LIST source type tests
    # ============================================================

    def test_list_int32_casts(self):
        """Test list<int32> -> all target types."""
        self._run_cast_tests("list_int32")

    def test_large_list_int32_casts(self):
        """Test large_list<int32> -> all target types."""
        self._run_cast_tests("large_list_int32")

    # ============================================================
    # COMPLEX: STRUCT source type tests
    # ============================================================

    def test_struct_casts(self):
        """Test struct -> all target types."""
        self._run_cast_tests("struct_ab_int32")

    # ============================================================
    # COMPLEX: MAP source type tests
    # ============================================================

    def test_map_casts(self):
        """Test map -> all target types."""
        self._run_cast_tests("map_str_int32")

    # ============================================================
    # COMPLEX: NESTED source type tests
    # ============================================================

    def test_nested_list_casts(self):
        """Test nested list<list<int32>> -> all target types."""
        self._run_cast_tests("nested_list_int32")

    def test_nested_struct_casts(self):
        """Test nested struct -> all target types."""
        self._run_cast_tests("nested_struct_int32")

    def test_list_of_struct_casts(self):
        """Test list<struct> -> all target types."""
        self._run_cast_tests("list_of_struct_int32")

    # ============================================================
    # EDGE CASE: NULL arrays
    # ============================================================

    def test_int8_nulls_casts(self):
        """Test casting int8 arrays containing only nulls."""
        self._run_cast_tests("int8_nulls")

    def test_int32_nulls_casts(self):
        """Test casting int32 arrays containing only nulls."""
        self._run_cast_tests("int32_nulls")

    def test_float64_nulls_casts(self):
        """Test casting float64 arrays containing only nulls."""
        self._run_cast_tests("float64_nulls")

    # ============================================================
    # EDGE CASE: EMPTY arrays
    # ============================================================

    def test_int8_empty_casts(self):
        """Test casting empty int8 arrays."""
        self._run_cast_tests("int8_empty")

    def test_int32_empty_casts(self):
        """Test casting empty int32 arrays."""
        self._run_cast_tests("int32_empty")

    def test_string_empty_casts(self):
        """Test casting empty string arrays."""
        self._run_cast_tests("string_empty")

    def test_float64_empty_casts(self):
        """Test casting empty float64 arrays."""
        self._run_cast_tests("float64_empty")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
