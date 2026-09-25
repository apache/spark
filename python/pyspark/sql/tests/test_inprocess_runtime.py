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

import sys
import unittest
from importlib.util import find_spec
from unittest.mock import patch

from pyspark import cloudpickle
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.testing.utils import have_pyarrow

_have_arrow_cdi = have_pyarrow and find_spec("cffi") is not None
if _have_arrow_cdi:
    import pyarrow as pa
    from pyarrow.cffi import ffi

    from pyspark.inprocess.runtime import (
        _inprocess_invoke,
        _inprocess_register,
        _inprocess_release,
        _udfs,
        _validate_result,
    )
    from pyspark.inprocess.udf import inprocess_udf


@unittest.skipUnless(_have_arrow_cdi, "Arrow CDI tests require PyArrow and cffi")
class InProcessRuntimeTests(unittest.TestCase):
    def tearDown(self):
        _udfs.clear()

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
            serialized = (
                func._serialize()
                if hasattr(func, "_serialize")
                else cloudpickle.dumps((func, return_type))
            )
            _inprocess_register("test", serialized, timezone, "%d.%d" % sys.version_info[:2])
            _inprocess_invoke(
                "test",
                [address(a) for a in arrays],
                [address(s) for s in schemas],
                address(output),
                address(output_schema),
                len(inputs[0]) if rows is None else rows,
            )
            return pa.Array._import_from_c(address(output), address(output_schema))
        finally:
            _inprocess_release(["test"])
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

    def test_primitive_types_do_not_implicitly_cast(self):
        cases = [
            (pa.array([1, 2]), IntegerType()),
            (pa.array(["1", "22"]), LongType()),
            (pa.array([1, 2], type=pa.timestamp("us", tz="UTC")), LongType()),
            (pa.array([1, 2], type=pa.date32()), IntegerType()),
            (pa.array([0.001, 0.0]), BooleanType()),
            (pa.array([1e300, 0.0]), FloatType()),
        ]
        for value, declared in cases:
            with self.subTest(value=value.type, declared=declared):
                wrapper = inprocess_udf(declared)(lambda x: x)
                with self.assertRaisesRegex(RuntimeError, "expected"):
                    self.invoke(wrapper, [value], declared)

    def test_zero_argument_udf_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "0-arg"):
            inprocess_udf(LongType())(lambda: pa.array([7]))

    def test_serialization_is_deferred_until_first_use(self):
        namespace = {"inprocess_udf": inprocess_udf, "LongType": LongType, "pa": pa}
        exec(
            "@inprocess_udf(LongType())\n"
            "def f(x): return pa.array([LOOKUP] * len(x), type=pa.int64())\n",
            namespace,
        )
        wrapper = namespace["f"]
        namespace["LOOKUP"] = 42
        self.assertEqual(self.invoke(wrapper, [pa.array([0])], LongType()).to_pylist(), [42])
        # Like other Python UDFs, the command is stable after first serialization.
        namespace["LOOKUP"] = 99
        self.assertEqual(self.invoke(wrapper, [pa.array([0])], LongType()).to_pylist(), [42])

    def test_empty_batch(self):
        value = pa.array([], type=pa.int64())
        self.assertEqual(self.invoke(lambda x: x, [value], LongType()), value)

    def test_user_error_includes_traceback(self):
        def fail(x):
            raise ValueError("expected failure")

        with self.assertRaisesRegex(RuntimeError, "expected failure") as error:
            self.invoke(fail, [pa.array([1])], LongType())
        self.assertIn("Traceback", str(error.exception))

    def test_system_exit_is_converted_to_an_ordinary_exception(self):
        def fail(x):
            raise SystemExit(0)

        with self.assertRaisesRegex(RuntimeError, "SystemExit"):
            self.invoke(fail, [pa.array([1])], LongType())

    def test_base_exception_during_deserialization_is_converted(self):
        def fail():
            raise SystemExit(0)

        class FailingLoad:
            def __reduce__(self):
                return fail, ()

        with self.assertRaisesRegex(RuntimeError, "SystemExit"):
            _inprocess_register(
                "bad",
                cloudpickle.dumps(FailingLoad()),
                "UTC",
                "%d.%d" % sys.version_info[:2],
            )
        self.assertNotIn("bad", _udfs)

    def test_python_version_is_checked_before_deserialization(self):
        with self.assertRaisesRegex(RuntimeError, "PYTHON_VERSION_MISMATCH"):
            _inprocess_register("bad", b"invalid pickle", "UTC", "0.0")
        self.assertNotIn("bad", _udfs)

    def test_registration_is_task_scoped(self):
        state = []

        def remember(x):
            state.append(x)
            return len(state)

        command = cloudpickle.dumps((remember, LongType()))
        for handle in ("first", "second"):
            _inprocess_register(handle, command, "UTC", "%d.%d" % sys.version_info[:2])
        self.assertEqual(_udfs["first"][0](1), 1)
        self.assertEqual(_udfs["first"][0](2), 2)
        self.assertEqual(_udfs["second"][0](3), 1)
        _inprocess_release(["first", "second", "unregistered"])
        self.assertFalse(_udfs)

    def test_slices_are_normalized_including_nested_child_offsets(self):
        for value in (
            pa.array([9, 1, None, 3]).slice(1),
            pa.array(["discard", "one", None, "three"]).slice(1),
            pa.array([[9], [1], None, [3]]).slice(1),
            pa.StructArray.from_arrays([pa.array([9, 1, None, 3]).slice(1)], names=["x"]),
        ):
            with self.subTest(data_type=value.type):
                normalized = _validate_result(value, 3, value.type)
                self.assertEqual(normalized.offset, 0)
                self.assertEqual(normalized.to_pylist(), value.to_pylist())
                if pa.types.is_struct(value.type):
                    self.assertEqual(normalized.field(0).offset, 0)

    def test_nested_nullability_accepts_compatible_values(self):
        nullable = pa.list_(pa.field("element", pa.string(), nullable=True))
        required = pa.list_(pa.field("element", pa.string(), nullable=False))
        for source, expected in ((nullable, required), (required, nullable)):
            value = pa.array([["a"], None, []], type=source)
            result = _validate_result(value, 3, expected)
            self.assertEqual(result.type, expected)
            self.assertEqual(result.to_pylist(), value.to_pylist())
        with self.assertRaisesRegex(ValueError, "non-nullable"):
            _validate_result(pa.array([[None]], type=nullable), 1, required)

    def test_null_struct_parents_do_not_violate_child_nullability(self):
        import pyarrow.compute as pc

        expected = pa.struct([pa.field("len", pa.int32(), nullable=False)])
        strings = pa.array([None, "a"])
        original = pa.StructArray.from_arrays(
            [pc.utf8_length(strings)], names=["len"], mask=pc.is_null(strings)
        )
        values = [original, pc.if_else(pc.is_valid(strings), original, None)]
        values.append(pc.take(original, pa.array([None, 1], type=pa.int32())))
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(value.field(0).null_count, 1)
                result = _validate_result(value, 2, expected)
                self.assertEqual(result.to_pylist(), [None, {"len": 1}])
                self.assertEqual(result.type, expected)
        visible_null = pa.StructArray.from_arrays([pa.array([None], pa.int32())], names=["len"])
        with self.assertRaisesRegex(ValueError, "non-nullable"):
            _validate_result(visible_null, 1, expected)

    def test_sliced_map_entries_are_normalized(self):
        map_type = pa.map_(pa.string(), pa.int64())
        entries = pa.StructArray.from_arrays(
            [pa.array(["hidden", "a", "b", "c"]), pa.array([None, 1, 2, 3])],
            fields=[map_type.key_field, map_type.item_field],
        )
        offsets = pa.array([0, 1, 3], type=pa.int32())
        value = pa.Array.from_buffers(
            map_type, 2, [None, offsets.buffers()[1]], children=[entries.slice(1)]
        )
        self.assertEqual(value.offset, 0)
        self.assertEqual(value.values.offset, 1)
        result = _validate_result(value, 2, map_type)
        self.assertEqual(result.values.offset, 0)
        self.assertEqual(result.to_pylist(), [[("a", 1)], [("b", 2), ("c", 3)]])

    def test_map_field_names_and_nested_metadata_are_normalized(self):
        value = pa.array(
            [[("a", 1)]],
            type=pa.map_(pa.field("k", pa.string(), False), pa.field("v", pa.int64())),
        )
        expected = pa.map_(pa.string(), pa.int64())
        result = _validate_result(value, 1, expected)
        self.assertEqual(result.type.key_field.name, "key")
        self.assertEqual(result.type.item_field.name, "value")
        expected_struct = pa.struct([pa.field("x", pa.int64(), metadata={b"type": b"required"})])
        result = _validate_result(pa.array([{"x": 1}]), 1, expected_struct)
        self.assertEqual(result.type[0].metadata, {b"type": b"required"})

    def test_map_nullability_and_sliced_results(self):
        nullable = pa.map_(pa.string(), pa.field("value", pa.int64()))
        required = pa.map_(pa.string(), pa.field("value", pa.int64(), nullable=False))
        value = pa.array([[("discard", 0)], [("a", 1)], None], type=nullable).slice(1)
        result = _validate_result(value, 2, required)
        self.assertEqual(result.offset, 0)
        self.assertEqual(result.type, required)
        self.assertEqual(result.to_pylist(), [[("a", 1)], None])
        with self.assertRaisesRegex(ValueError, "non-nullable"):
            _validate_result(pa.array([[("a", None)]], type=nullable), 1, required)

    def test_null_checks_skip_nullable_subtrees_and_null_free_parents(self):
        arrays = [
            pa.array([{"x": [1, None]}, {"x": None}, None]),
            pa.array([{"x": 1}, {"x": 2}], type=pa.struct([pa.field("x", pa.int64(), False)])),
            pa.array([[("a", 1)], [("b", 2)]], type=pa.map_(pa.string(), pa.int64())),
        ]
        for array in arrays:
            with (
                self.subTest(type=array.type),
                patch("pyspark.inprocess.runtime.pc.filter") as filtered,
                patch("pyspark.inprocess.runtime.pa.concat_arrays") as concat,
            ):
                result = _validate_result(array, len(array), array.type)
                self.assertEqual(result, array)
                filtered.assert_not_called()
                concat.assert_not_called()

    def test_null_checks_still_validate_required_descendants(self):
        required = pa.struct([pa.field("x", pa.list_(pa.field("element", pa.int64(), False)))])
        with self.assertRaisesRegex(ValueError, "non-nullable"):
            _validate_result(pa.array([{"x": [None]}, None], type=required), 2, required)
        hidden = pa.array([{"x": None}, None], type=required)
        self.assertEqual(_validate_result(hidden, 2, required), hidden)

    def test_map_entries_offset_respects_required_values(self):
        source = pa.map_(pa.string(), pa.int64())
        expected = pa.map_(pa.string(), pa.field("value", pa.int64(), False))
        for data in ([0, 1, 2, None], [None, 1, 2, 3]):
            entries = pa.StructArray.from_arrays(
                [pa.array(["hidden", "a", "b", "c"]), pa.array(data)],
                fields=[source.key_field, source.item_field],
            )
            offsets = pa.array([0, 1, 3], pa.int32()).buffers()[1]
            value = pa.Array.from_buffers(source, 2, [None, offsets], children=[entries.slice(1)])
            with self.subTest(data=data):
                if data[-1] is None:
                    with self.assertRaisesRegex(ValueError, "non-nullable"):
                        _validate_result(value, 2, expected)
                else:
                    result = _validate_result(value, 2, expected)
                    self.assertEqual(result.to_pylist(), [[("a", 1)], [("b", 2), ("c", 3)]])

    def test_null_parents_do_not_copy_null_free_children(self):
        struct = pa.StructArray.from_arrays(
            [pa.array([b"payload", b"value"])],
            fields=[pa.field("value", pa.binary(), False)],
            mask=pa.array([True, False]),
        )
        mapping = pa.MapArray.from_arrays(
            pa.array([0, 1, 2]),
            pa.array(["a", "b"]),
            pa.array([1, 2]),
            type=pa.map_(pa.string(), pa.field("value", pa.int64(), False)),
            mask=pa.array([True, False]),
        )
        for value in (struct, mapping):
            with (
                self.subTest(type=value.type),
                patch("pyspark.inprocess.runtime.pc.filter") as filtered,
                patch("pyspark.inprocess.runtime.pa.concat_arrays") as concat,
            ):
                result = _validate_result(value, 2, value.type)
                self.assertEqual(result, value)
                filtered.assert_not_called()
                concat.assert_not_called()

    def test_null_check_does_not_copy_unchecked_siblings(self):
        import pyarrow.compute as pc

        value = pa.StructArray.from_arrays(
            [pa.array([None, 1]), pa.array([b"a" * 4096, b"b" * 4096])],
            fields=[pa.field("required", pa.int64(), False), pa.field("payload", pa.binary())],
            mask=pa.array([True, False]),
        )
        with patch("pyspark.inprocess.runtime.pc.filter", wraps=pc.filter) as filtered:
            result = _validate_result(value, 2, value.type)
            self.assertEqual(result, value)
            filtered.assert_called_once()
            self.assertEqual(filtered.call_args.args[0].type, pa.int64())
            self.assertEqual(
                result.field(1).buffers()[2].address, value.field(1).buffers()[2].address
            )

    def test_unsupported_types_fail_before_serialization(self):
        from pyspark.errors import PySparkNotImplementedError
        from pyspark.sql.types import (
            CalendarIntervalType,
            CharType,
            VarcharType,
            YearMonthIntervalType,
        )

        for declared in (
            CalendarIntervalType(),
            CharType(5),
            VarcharType(5),
            YearMonthIntervalType(),
            ArrayType(YearMonthIntervalType()),
            StructType([StructField("x", CalendarIntervalType())]),
        ):
            with self.subTest(declared=declared):
                wrapper = inprocess_udf(declared)(lambda x: x)
                with self.assertRaises(PySparkNotImplementedError) as error:
                    wrapper._serialize()
                self.assertEqual(error.exception.getCondition(), "NOT_IMPLEMENTED")
                self.assertIsNone(wrapper._serialized)

    def test_large_binary_logical_types(self):
        from pyspark.inprocess.runtime import _large_binary_type
        from pyspark.sql.pandas.types import to_arrow_type
        from pyspark.sql.types import GeographyType, GeometryType, MapType, VariantType

        for data_type in [VariantType(), GeometryType(0), GeographyType(4326)]:
            for large in [False, True]:
                arrow_type = to_arrow_type(data_type, prefers_large_types=large)
                # Shared worker/toArrow conversion keeps its existing small-binary contract.
                self.assertEqual(arrow_type[-1].type, pa.binary())
                for nested in (
                    data_type,
                    ArrayType(data_type),
                    MapType(StringType(), data_type),
                    StructType([StructField("x", data_type)]),
                ):
                    raw = to_arrow_type(nested, prefers_large_types=large)
                    expected = _large_binary_type(raw) if large else raw
                    handle = "large"
                    _inprocess_register(
                        handle,
                        cloudpickle.dumps((lambda x: x, nested)),
                        "UTC",
                        "%d.%d" % sys.version_info[:2],
                        large,
                    )
                    self.assertEqual(_udfs[handle][1], expected)
                self.assertEqual(_large_binary_type(arrow_type)[-1].type, pa.large_binary())

    def test_wrapper_metadata_and_return_type_validation(self):
        from pyspark.errors import PySparkTypeError
        from pyspark.util import PythonEvalType

        def identity(x):
            """Identity documentation."""
            return x

        udf = inprocess_udf(LongType())(identity)
        self.assertEqual(udf.__name__, identity.__name__)
        self.assertEqual(udf.__doc__, identity.__doc__)
        self.assertIs(udf.func, identity)
        self.assertEqual(udf.returnType, LongType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_INPROCESS_UDF)
        self.assertTrue(udf.deterministic)
        self.assertIs(udf.asNondeterministic(), udf)
        self.assertFalse(udf.deterministic)
        with self.assertRaises(PySparkTypeError):
            inprocess_udf(42)(identity)
        # DDL decoration does not require a live Spark session.
        self.assertIsNone(inprocess_udf("long")(identity)._parsed_return_type)

    def test_registration_traceback_policy(self):
        for hide in [False, True]:
            with self.assertRaises(RuntimeError) as error:
                _inprocess_register("bad", b"", "UTC", "0.0", False, hide)
            self.assertEqual("Traceback" in str(error.exception), not hide)
            self.assertIn("PYTHON_VERSION_MISMATCH", str(error.exception))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
