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


"""Arrow CDI contract tests that do not need a Spark JVM or JEP."""

import unittest

import pyarrow as pa
from pyarrow.cffi import ffi

from pyspark import cloudpickle
from pyspark.inprocess.runtime import _inprocess_invoke, _load_udf
from pyspark.inprocess.udf import inprocess_udf
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class InProcessRuntimeTests(unittest.TestCase):
    def tearDown(self):
        _load_udf.cache_clear()

    def invoke(self, func, inputs, return_type, rows=None, timezone="UTC"):
        arrays = [ffi.new("struct ArrowArray*") for _ in inputs]
        schemas = [ffi.new("struct ArrowSchema*") for _ in inputs]
        output = ffi.new("struct ArrowArray*")
        output_schema = ffi.new("struct ArrowSchema*")

        def address(value):
            return int(ffi.cast("uintptr_t", value))

        try:
            for value, array, schema in zip(inputs, arrays, schemas):
                value._export_to_c(address(array), address(schema))
            serialized = getattr(func, "_serialized", None)
            if serialized is None:
                serialized = cloudpickle.dumps(func)
            _inprocess_invoke(
                serialized,
                [address(a) for a in arrays],
                [address(s) for s in schemas],
                address(output),
                address(output_schema),
                len(inputs[0]) if rows is None else rows,
                return_type.json(),
                timezone,
            )
            return pa.Array._import_from_c(address(output), address(output_schema))
        finally:
            for value in arrays + schemas + [output, output_schema]:
                if value.release != ffi.NULL:
                    value.release(value)

    def test_identity_retains_buffers_and_nulls(self):
        value = pa.array([1, None, 3], type=pa.int64())
        result = self.invoke(lambda x: x, [value], LongType())
        self.assertEqual(result, value)
        self.assertEqual(result.buffers()[1].address, value.buffers()[1].address)

    def test_wrong_length(self):
        for delta in (-1, 1):
            with (
                self.subTest(delta=delta),
                self.assertRaisesRegex(RuntimeError, "returned .* rows; expected 3"),
            ):
                self.invoke(
                    lambda x: pa.array([1] * (len(x) + delta)),
                    [pa.array([1, 2, 3])],
                    LongType(),
                )

    def test_wrong_return_object_has_traceback(self):
        with self.assertRaisesRegex(RuntimeError, "must return a pyarrow.Array") as error:
            self.invoke(lambda x: [1, 2], [pa.array([1, 2])], LongType())
        self.assertIn("__INPROCESS_UDF_TRACEBACK__:", str(error.exception))
        self.assertIn("Traceback", str(error.exception))

    def test_wrong_declared_type(self):
        with self.assertRaisesRegex(RuntimeError, "expected string"):
            self.invoke(lambda x: x, [pa.array([1, 2])], StringType())

    def test_nested_schema_mismatch(self):
        expected = StructType([StructField("values", ArrayType(StringType()))])
        value = pa.array([{"values": [1, 2]}])
        with self.assertRaisesRegex(RuntimeError, "expected struct"):
            self.invoke(lambda x: x, [value], expected)

    def test_decimal_scale_mismatch(self):
        from decimal import Decimal

        value = pa.array([Decimal("1.2")], type=pa.decimal128(10, 1))
        with self.assertRaisesRegex(RuntimeError, "expected decimal128"):
            self.invoke(lambda x: x, [value], DecimalType(10, 2))

    def test_timestamp_timezone_mismatch(self):
        value = pa.array([0], type=pa.timestamp("us", tz="UTC"))
        with self.assertRaisesRegex(RuntimeError, "America/Los_Angeles"):
            self.invoke(lambda x: x, [value], TimestampType(), timezone="America/Los_Angeles")

    def test_primitive_cast_is_preserved(self):
        wrapper = inprocess_udf(IntegerType())(lambda x: x)
        result = self.invoke(wrapper, [pa.array([1, 2])], IntegerType())
        self.assertEqual(result.type, pa.int32())

    def test_zero_argument_udf(self):
        result = self.invoke(lambda: pa.array([7, 7]), [], LongType(), rows=2)
        self.assertEqual(result.to_pylist(), [7, 7])

    def test_empty_batch(self):
        value = pa.array([], type=pa.int64())
        self.assertEqual(self.invoke(lambda x: x, [value], LongType()), value)

    def test_user_error_includes_traceback(self):
        def fail(x):
            raise ValueError("expected failure")

        with self.assertRaisesRegex(RuntimeError, "expected failure") as error:
            self.invoke(fail, [pa.array([1])], LongType())
        self.assertIn("Traceback", str(error.exception))


if __name__ == "__main__":
    unittest.main()
