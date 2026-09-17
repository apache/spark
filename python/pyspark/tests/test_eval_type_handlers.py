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

import unittest

from pyspark.eval_handlers._arrow import ArrowScalarUDFHandler
from pyspark.eval_handlers._base import (
    BatchEvalTypeHandler,
    CoGroupedEvalTypeHandler,
    EvalTypeHandler,
    GroupedEvalTypeHandler,
    _eval_type_handlers,
    get_eval_type_handler,
)
from pyspark.sql.pandas.serializers import (
    ArrowStreamCoGroupSerializer,
    ArrowStreamGroupSerializer,
    ArrowStreamSerializer,
)
from pyspark.sql.types import LongType
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message
from pyspark.util import PythonEvalType


class _RunnerConf:
    """Minimal stand-in for the worker's RunnerConf, exposing only the fields
    the handlers under test read."""

    use_large_var_types = False


class EvalTypeHandlerTests(unittest.TestCase):
    def test_scalar_arrow_udf_is_registered(self):
        self.assertIs(
            get_eval_type_handler(PythonEvalType.SQL_SCALAR_ARROW_UDF),
            ArrowScalarUDFHandler,
        )

    def test_category_bases_are_abstract(self):
        # The interface and the three category bases must not be instantiable:
        # they leave ``run`` abstract.
        for base in (
            EvalTypeHandler,
            BatchEvalTypeHandler,
            GroupedEvalTypeHandler,
            CoGroupedEvalTypeHandler,
        ):
            with self.assertRaises(TypeError):
                base([], _RunnerConf(), None)

    def test_category_bases_are_not_registered(self):
        # Only concrete subclasses that declare an eval type are registered.
        registered = set(_eval_type_handlers.values())
        for base in (
            EvalTypeHandler,
            BatchEvalTypeHandler,
            GroupedEvalTypeHandler,
            CoGroupedEvalTypeHandler,
        ):
            self.assertNotIn(base, registered)

    def test_default_serializer_per_category(self):
        class _Batch(BatchEvalTypeHandler["pa.RecordBatch"]):
            def run(self, split_index, data):
                return data

        class _Grouped(GroupedEvalTypeHandler["pa.RecordBatch"]):
            def run(self, split_index, data):
                return data

        class _CoGrouped(CoGroupedEvalTypeHandler["pa.RecordBatch"]):
            def run(self, split_index, data):
                return data

        self.assertIsInstance(_Batch([], _RunnerConf(), None).serializer, ArrowStreamSerializer)
        self.assertIsInstance(
            _Grouped([], _RunnerConf(), None).serializer, ArrowStreamGroupSerializer
        )
        self.assertIsInstance(
            _CoGrouped([], _RunnerConf(), None).serializer, ArrowStreamCoGroupSerializer
        )

    def test_run_produces_output(self):
        class _Doubler(BatchEvalTypeHandler["pa.RecordBatch"]):
            def run(self, split_index, data):
                for item in data:
                    yield item * 2

        handler = _Doubler([], _RunnerConf(), None)
        self.assertEqual(list(handler.run(0, iter([1, 2, 3]))), [2, 4, 6])

    def test_duplicate_eval_type_rejected(self):
        def _define_duplicate():
            class _Dup(BatchEvalTypeHandler["pa.RecordBatch"]):
                eval_type = PythonEvalType.SQL_SCALAR_ARROW_UDF

                def run(self, split_index, data):
                    return data

        self.assertRaises(AssertionError, _define_duplicate)
        # The failed definition must not clobber the existing registration.
        self.assertIs(
            get_eval_type_handler(PythonEvalType.SQL_SCALAR_ARROW_UDF),
            ArrowScalarUDFHandler,
        )

    def test_abstract_handler_with_eval_type_rejected(self):
        # A subclass that declares an eval_type but leaves run abstract must be
        # rejected at class definition.
        unused_eval_type = -1

        def _define_abstract():
            class _Abstract(BatchEvalTypeHandler["pa.RecordBatch"]):
                eval_type = unused_eval_type
                # run left abstract on purpose

        self.assertRaises(AssertionError, _define_abstract)
        self.assertNotIn(unused_eval_type, _eval_type_handlers)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowScalarUDFHandlerTests(unittest.TestCase):
    def test_end_to_end_output(self):
        import pyarrow as pa

        # One UDF reading column 0 (a pa.Array) and returning column + 1.
        def add_one(col):
            return pa.array([v.as_py() + 1 for v in col], type=pa.int64())

        udfs = [(add_one, [0], {}, LongType())]
        handler = ArrowScalarUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)

        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3], type=pa.int64())], ["_0"])
        out = list(handler.run(0, iter([batch])))

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].num_columns, 1)
        self.assertEqual(out[0].column(0).to_pylist(), [2, 3, 4])

    def test_output_schema_enforced(self):
        import pyarrow as pa

        # The UDF returns int32, but the declared return type is LongType (int64).
        # run must enforce the declared schema onto the output batch.
        def add_one(col):
            return pa.array([v.as_py() + 1 for v in col], type=pa.int32())

        udfs = [(add_one, [0], {}, LongType())]
        handler = ArrowScalarUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)

        batch = pa.RecordBatch.from_arrays([pa.array([10, 20], type=pa.int64())], ["_0"])
        out = list(handler.run(0, iter([batch])))
        # The int32 the UDF produced is coerced to the declared LongType (int64).
        self.assertEqual(out[0].schema.field(0).type, pa.int64())
        self.assertEqual(out[0].column(0).to_pylist(), [11, 21])


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class CoGroupedBatchTests(unittest.TestCase):
    def test_deserialized_co_group_is_a_pair_of_lists(self):
        # CoGroupedBatch must match what ArrowStreamCoGroupSerializer yields: the
        # serializer eagerly materializes each side as a list, not an iterator.
        import io

        import pyarrow as pa

        from pyspark.serializers import write_int

        def arrow_bytes(batches):
            buf = io.BytesIO()
            ArrowStreamSerializer().dump_stream(iter(batches), buf)
            return buf.getvalue()

        left = pa.RecordBatch.from_arrays([pa.array([1, 2])], ["_0"])
        right = pa.RecordBatch.from_arrays([pa.array([9])], ["_0"])

        stream = io.BytesIO()
        write_int(2, stream)  # two DataFrames in the co-group
        stream.write(arrow_bytes([left]))
        stream.write(arrow_bytes([right]))
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
