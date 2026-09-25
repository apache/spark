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

"""Tests for the Arrow eval type handlers (``_arrow``).

Each handler has one test class. The helpers below build handler input in the
same wire format the serializers produce, so every test constructs its input the
same way: ``run(0, <input>)`` and assert on the output batches.
"""

import unittest

from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.sql.pandas.serializers import ArrowStreamCoGroupSerializer, ArrowStreamSerializer
from pyspark.sql.types import LongType, StructField, StructType
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message
from pyspark.util import PythonEvalType

if have_pyarrow:
    import pyarrow as pa

    from pyspark.eval_handlers._arrow import (
        ArrowCoGroupedMapUDFHandler,
        ArrowGroupedAggIterUDFHandler,
        ArrowGroupedAggUDFHandler,
        ArrowGroupedMapIterUDFHandler,
        ArrowGroupedMapUDFHandler,
        ArrowMapUDFHandler,
        ArrowScalarIterUDFHandler,
        ArrowScalarUDFHandler,
        ArrowWindowAggUDFHandler,
    )
    from pyspark.sql.conversion import ArrowBatchTransformer


class _RunnerConf:
    """Minimal stand-in for the worker's RunnerConf, exposing only the fields
    the handlers under test read."""

    use_large_var_types = False
    assign_cols_by_name = True
    map_in_batch_legacy_accept_any_iterable = False
    # One bound type per window UDF; the bounded test overrides this per instance.
    window_bound_types = ["unbounded"]


def _batch(**columns):
    """A RecordBatch of int64 columns, one per ``name=values`` kwarg."""
    return pa.RecordBatch.from_arrays(
        [pa.array(values, type=pa.int64()) for values in columns.values()],
        list(columns),
    )


def _struct_batch(**columns):
    """``_batch`` wrapped into a single struct column (the grouped/map wire format)."""
    return ArrowBatchTransformer.wrap_struct(_batch(**columns))


def _one_group(*batches):
    """One group of the given batches, shaped as the group serializer yields it."""
    return iter([iter(batches)])


def _one_cogroup(left, right):
    """One co-group of a left and a right batch, as the co-group serializer yields it."""
    return iter([([left], [right])])


def _grouped_arg_offsets(*dataframes):
    """Encode ``arg_offsets`` from ``(key_cols, value_cols)`` per DataFrame.

    Mirrors BasePandasGroupExec.resolveArgOffsets: each DataFrame is laid out as
    ``[length, num_keys, *key_cols, *value_cols]``.
    """
    offsets: list = []
    for key_cols, value_cols in dataframes:
        group = [len(key_cols), *key_cols, *value_cols]
        offsets += [len(group), *group]
    return offsets


def _scalar_handler(handler_cls, udf):
    """Build a scalar handler whose one UDF reads column 0 and returns LongType.

    The scalar UDF tuple is ``(func, args_offsets, kwargs_offsets, return_type)``.
    """
    return handler_cls(udfs=[(udf, [0], {}, LongType())], runner_conf=_RunnerConf(), eval_conf=None)


def _grouped_handler(handler_cls, udf, arg_offsets, num_udf_args):
    """Build a grouped/cogrouped-map handler with the shared return type.

    The grouped-map UDF tuple is ``(func, arg_offsets, return_type, num_udf_args)``.
    """
    return handler_cls(
        udfs=[(udf, arg_offsets, _RETURN_TYPE, num_udf_args)],
        runner_conf=_RunnerConf(),
        eval_conf=None,
    )


# arg_offsets for one DataFrame with key column 0 and value column 1.
_GROUP_OFFSETS = _grouped_arg_offsets(([0], [1]))
# arg_offsets for two DataFrames (co-group), each with key column 0 and value column 1.
_COGROUP_OFFSETS = _grouped_arg_offsets(([0], [1]), ([0], [1]))
_RETURN_TYPE = StructType([StructField("v", LongType())])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowEvalTypeHandlerRegistrationTests(unittest.TestCase):
    def test_arrow_eval_types_are_registered(self):
        # Every migrated Arrow eval type dispatches to its handler by lookup.
        for eval_type, handler_cls in (
            (PythonEvalType.SQL_SCALAR_ARROW_UDF, ArrowScalarUDFHandler),
            (PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF, ArrowScalarIterUDFHandler),
            (PythonEvalType.SQL_MAP_ARROW_ITER_UDF, ArrowMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF, ArrowGroupedMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF, ArrowGroupedMapIterUDFHandler),
            (PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF, ArrowCoGroupedMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF, ArrowGroupedAggUDFHandler),
            (PythonEvalType.SQL_GROUPED_AGG_ARROW_ITER_UDF, ArrowGroupedAggIterUDFHandler),
            (PythonEvalType.SQL_WINDOW_AGG_ARROW_UDF, ArrowWindowAggUDFHandler),
        ):
            self.assertIs(get_eval_type_handler(eval_type), handler_cls)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowScalarUDFHandlerTests(unittest.TestCase):
    def test_invokes_udf_per_batch(self):
        def add_one(col):
            return pa.array([v.as_py() + 1 for v in col], type=pa.int64())

        handler = _scalar_handler(ArrowScalarUDFHandler, add_one)
        out = list(handler.run(0, iter([_batch(a=[1, 2, 3])])))
        self.assertEqual([b.column(0).to_pylist() for b in out], [[2, 3, 4]])

    def test_coerces_output_to_return_type(self):
        # The UDF returns int32, but the declared return type is LongType (int64).
        def add_one(col):
            return pa.array([v.as_py() + 1 for v in col], type=pa.int32())

        handler = _scalar_handler(ArrowScalarUDFHandler, add_one)
        out = list(handler.run(0, iter([_batch(a=[10, 20])])))
        self.assertEqual(out[0].schema.field(0).type, pa.int64())
        self.assertEqual(out[0].column(0).to_pylist(), [11, 21])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowScalarIterUDFHandlerTests(unittest.TestCase):
    def test_invokes_udf_over_batch_stream(self):
        def add_one(col_iter):
            for col in col_iter:
                yield pa.array([v.as_py() + 1 for v in col], type=pa.int64())

        handler = _scalar_handler(ArrowScalarIterUDFHandler, add_one)
        out = list(handler.run(0, iter([_batch(a=[1, 2]), _batch(a=[3])])))
        self.assertEqual([b.column(0).to_pylist() for b in out], [[2, 3], [4]])

    def test_rejects_row_count_mismatch(self):
        # Emitting more rows than were consumed must fail (fail-fast row limit).
        def too_many(col_iter):
            for col in col_iter:
                yield pa.array(list(range(len(col) + 1)), type=pa.int64())

        handler = _scalar_handler(ArrowScalarIterUDFHandler, too_many)
        with self.assertRaises(PySparkRuntimeError):
            list(handler.run(0, iter([_batch(a=[1, 2])])))


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowMapUDFHandlerTests(unittest.TestCase):
    def test_maps_batch_stream(self):
        # mapInArrow flattens the wire struct for the UDF and re-wraps its output.
        def double_v(batch_iter):
            for batch in batch_iter:
                yield _batch(v=[c.as_py() * 2 for c in batch.column("v")])

        handler = ArrowMapUDFHandler(
            udfs=[(double_v, None, None, None)], runner_conf=_RunnerConf(), eval_conf=None
        )
        out = list(handler.run(0, iter([_struct_batch(v=[1, 2, 3])])))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [2, 4, 6])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedMapUDFHandlerTests(unittest.TestCase):
    def test_applies_udf_per_group(self):
        def grouped_udf(value_table):
            return pa.table({"v": pa.array([c.as_py() * 10 for c in value_table.column("v")])})

        handler = _grouped_handler(ArrowGroupedMapUDFHandler, grouped_udf, _GROUP_OFFSETS, 1)
        out = list(handler.run(0, _one_group(_struct_batch(k=[7, 7], v=[1, 2]))))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [10, 20])

    def test_passes_key_when_udf_takes_key(self):
        def grouped_udf(key, value_table):
            k = key[0].as_py()
            return pa.table({"v": pa.array([c.as_py() + k for c in value_table.column("v")])})

        handler = _grouped_handler(ArrowGroupedMapUDFHandler, grouped_udf, _GROUP_OFFSETS, 2)
        out = list(handler.run(0, _one_group(_struct_batch(k=[7, 7], v=[1, 2]))))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [8, 9])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedMapIterUDFHandlerTests(unittest.TestCase):
    def test_applies_udf_per_group(self):
        def grouped_udf(value_batches):
            for batch in value_batches:
                yield _batch(v=[c.as_py() + 1 for c in batch.column("v")])

        handler = _grouped_handler(ArrowGroupedMapIterUDFHandler, grouped_udf, _GROUP_OFFSETS, 1)
        out = list(handler.run(0, _one_group(_struct_batch(k=[7], v=[41]))))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [42])

    def test_passes_key_when_udf_takes_key(self):
        def grouped_udf(key, value_batches):
            k = key[0].as_py()
            for batch in value_batches:
                yield _batch(v=[c.as_py() + k for c in batch.column("v")])

        handler = _grouped_handler(ArrowGroupedMapIterUDFHandler, grouped_udf, _GROUP_OFFSETS, 2)
        out = list(handler.run(0, _one_group(_struct_batch(k=[10], v=[5]))))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [15])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowCoGroupedMapUDFHandlerTests(unittest.TestCase):
    # Co-group batches arrive un-wrapped (columns k, v), unlike the grouped-map wire format.
    def test_applies_udf_per_cogroup(self):
        def cogrouped_udf(left_values, right_values):
            total = left_values.column("v")[0].as_py() + right_values.column("v")[0].as_py()
            return pa.table({"v": pa.array([total], type=pa.int64())})

        handler = _grouped_handler(ArrowCoGroupedMapUDFHandler, cogrouped_udf, _COGROUP_OFFSETS, 2)
        out = list(handler.run(0, _one_cogroup(_batch(k=[5], v=[10]), _batch(k=[5], v=[20]))))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [30])

    def test_passes_key_when_udf_takes_key(self):
        def cogrouped_udf(key, left_values, right_values):
            k = key[0].as_py()
            total = left_values.column("v")[0].as_py() + right_values.column("v")[0].as_py()
            return pa.table({"v": pa.array([total + k], type=pa.int64())})

        handler = _grouped_handler(ArrowCoGroupedMapUDFHandler, cogrouped_udf, _COGROUP_OFFSETS, 3)
        out = list(handler.run(0, _one_cogroup(_batch(k=[5], v=[10]), _batch(k=[5], v=[20]))))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [35])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedAggUDFHandlerTests(unittest.TestCase):
    # Grouped-agg batches arrive un-wrapped (flat columns); the UDF reads columns by offset and
    # returns one scalar, emitted as a single-row batch per group.
    def test_reduces_group_to_one_row(self):
        def sum_udf(col):
            return sum(c.as_py() for c in col)

        handler = ArrowGroupedAggUDFHandler(
            udfs=[(sum_udf, [0], {}, LongType())], runner_conf=_RunnerConf(), eval_conf=None
        )
        out = list(handler.run(0, _one_group(_batch(v=[1, 2, 3]), _batch(v=[4]))))
        self.assertEqual(out[0].column("_0").to_pylist(), [10])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowGroupedAggIterUDFHandlerTests(unittest.TestCase):
    # The UDF receives the group's input columns as an iterator and returns one scalar.
    def test_reduces_group_to_one_row(self):
        def sum_iter_udf(col_iter):
            return sum(c.as_py() for col in col_iter for c in col)

        handler = ArrowGroupedAggIterUDFHandler(
            udfs=[(sum_iter_udf, [0], {}, LongType())], runner_conf=_RunnerConf(), eval_conf=None
        )
        out = list(handler.run(0, _one_group(_batch(v=[10]), _batch(v=[20]))))
        self.assertEqual(out[0].column("_0").to_pylist(), [30])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowWindowAggUDFHandlerTests(unittest.TestCase):
    # One output value per input row over the UDF's window frame.
    def test_unbounded_frame_repeats_one_value(self):
        def sum_udf(col):
            return sum(c.as_py() for c in col)

        conf = _RunnerConf()
        conf.window_bound_types = ["unbounded"]
        handler = ArrowWindowAggUDFHandler(
            udfs=[(sum_udf, [0], {}, LongType())], runner_conf=conf, eval_conf=None
        )
        out = list(handler.run(0, _one_group(_batch(v=[1, 2, 3]))))
        self.assertEqual(out[0].column("_0").to_pylist(), [6, 6, 6])

    def test_bounded_frame_slices_per_row(self):
        # args_offsets = [begin_col, end_col, *value_cols]; each row's frame is ``[begin, end)``.
        def sum_udf(col):
            return sum(c.as_py() for c in col)

        conf = _RunnerConf()
        conf.window_bound_types = ["bounded"]
        handler = ArrowWindowAggUDFHandler(
            udfs=[(sum_udf, [0, 1, 2], {}, LongType())], runner_conf=conf, eval_conf=None
        )
        # Row 0 frame [0, 1) -> [10]; row 1 frame [0, 2) -> [10, 20].
        out = list(handler.run(0, _one_group(_batch(begin=[0, 0], end=[1, 2], v=[10, 20]))))
        self.assertEqual(out[0].column("_0").to_pylist(), [10, 30])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class CoGroupedBatchTests(unittest.TestCase):
    def test_deserialized_co_group_is_a_pair_of_lists(self):
        # CoGroupedBatch must match what ArrowStreamCoGroupSerializer yields: the
        # serializer eagerly materializes each side as a list, not an iterator.
        import io

        from pyspark.serializers import write_int

        def arrow_bytes(batches):
            buf = io.BytesIO()
            ArrowStreamSerializer().dump_stream(iter(batches), buf)
            return buf.getvalue()

        stream = io.BytesIO()
        write_int(2, stream)  # two DataFrames in the co-group
        stream.write(arrow_bytes([_batch(v=[1, 2])]))
        stream.write(arrow_bytes([_batch(v=[9])]))
        write_int(0, stream)  # end of stream
        stream.seek(0)

        groups = list(ArrowStreamCoGroupSerializer().load_stream(stream))
        self.assertEqual(len(groups), 1)
        left_side, right_side = groups[0]
        self.assertIsInstance(left_side, list)
        self.assertIsInstance(right_side, list)
        self.assertEqual([b.num_rows for b in left_side], [2])
        self.assertEqual([b.num_rows for b in right_side], [1])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
