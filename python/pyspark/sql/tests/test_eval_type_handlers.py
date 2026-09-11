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

from pyspark.sql.pandas.eval_type_handlers import (
    _EVAL_TYPE_HANDLERS,
    ArrowScalarUDFHandler,
    BatchEvalTypeHandler,
    CoGroupedEvalTypeHandler,
    EvalTypeHandler,
    GroupedEvalTypeHandler,
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
            _EVAL_TYPE_HANDLERS.get(PythonEvalType.SQL_SCALAR_ARROW_UDF),
            ArrowScalarUDFHandler,
        )

    def test_category_bases_are_abstract(self):
        # The interface and the three category bases must not be instantiable:
        # they leave the pipeline stages abstract.
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
        registered = set(_EVAL_TYPE_HANDLERS.values())
        for base in (
            EvalTypeHandler,
            BatchEvalTypeHandler,
            GroupedEvalTypeHandler,
            CoGroupedEvalTypeHandler,
        ):
            self.assertNotIn(base, registered)

    def test_default_serializer_per_category(self):
        class _Batch(BatchEvalTypeHandler["pa.RecordBatch"]):
            def pre_process(self, data):
                return data

            def process(self, work_items):
                return work_items

            def post_process(self, results):
                return results

        class _Grouped(GroupedEvalTypeHandler["pa.RecordBatch"]):
            def pre_process(self, data):
                return data

            def process(self, work_items):
                return work_items

            def post_process(self, results):
                return results

        class _CoGrouped(CoGroupedEvalTypeHandler["pa.RecordBatch"]):
            def pre_process(self, data):
                return data

            def process(self, work_items):
                return work_items

            def post_process(self, results):
                return results

        self.assertIsInstance(
            _Batch([], _RunnerConf(), None).select_serializer(), ArrowStreamSerializer
        )
        self.assertIsInstance(
            _Grouped([], _RunnerConf(), None).select_serializer(), ArrowStreamGroupSerializer
        )
        self.assertIsInstance(
            _CoGrouped([], _RunnerConf(), None).select_serializer(),
            ArrowStreamCoGroupSerializer,
        )

    def test_run_chains_stages_in_order(self):
        calls = []

        class _Recording(BatchEvalTypeHandler["pa.RecordBatch"]):
            def pre_process(self, data):
                calls.append("pre")
                for item in data:
                    yield item + 1

            def process(self, work_items):
                calls.append("process")
                for item in work_items:
                    yield item * 10

            def post_process(self, results):
                calls.append("post")
                for item in results:
                    yield item - 2

        handler = _Recording([], _RunnerConf(), None)
        out = list(handler.run(0, iter([1, 2, 3])))
        # Each element flows pre -> process -> post: (x + 1) * 10 - 2.
        self.assertEqual(out, [18, 28, 38])
        # All three stages participate in the pipeline.
        self.assertEqual(set(calls), {"pre", "process", "post"})

    def test_duplicate_eval_type_rejected(self):
        def _define_duplicate():
            class _Dup(BatchEvalTypeHandler["pa.RecordBatch"]):
                eval_type = PythonEvalType.SQL_SCALAR_ARROW_UDF

                def pre_process(self, data):
                    return data

                def process(self, work_items):
                    return work_items

                def post_process(self, results):
                    return results

        self.assertRaises(AssertionError, _define_duplicate)
        # The failed definition must not clobber the existing registration.
        self.assertIs(
            _EVAL_TYPE_HANDLERS[PythonEvalType.SQL_SCALAR_ARROW_UDF],
            ArrowScalarUDFHandler,
        )


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
        # post_process must enforce the declared schema onto the output batch.
        def add_one(col):
            return pa.array([v.as_py() + 1 for v in col], type=pa.int32())

        udfs = [(add_one, [0], {}, LongType())]
        handler = ArrowScalarUDFHandler(udfs=udfs, runner_conf=_RunnerConf(), eval_conf=None)

        batch = pa.RecordBatch.from_arrays([pa.array([10, 20], type=pa.int64())], ["_0"])
        out = list(handler.run(0, iter([batch])))
        # The int32 the UDF produced is coerced to the declared LongType (int64).
        self.assertEqual(out[0].schema.field(0).type, pa.int64())
        self.assertEqual(out[0].column(0).to_pylist(), [11, 21])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
