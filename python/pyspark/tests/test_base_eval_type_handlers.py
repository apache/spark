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

"""Framework-level tests for the eval type handler pipeline (``_base``).

Handler-flavor tests live alongside their module, e.g. the Arrow handlers in
``test_arrow_eval_type_handlers``.
"""

import unittest

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


class _RunnerConf:
    """Minimal stand-in for the worker's RunnerConf, exposing only the fields
    the handlers under test read."""

    use_large_var_types = False
    assign_cols_by_name = True
    map_in_batch_legacy_accept_any_iterable = False


class EvalTypeHandlerTests(unittest.TestCase):
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
        unused_eval_type = -2

        class _First(BatchEvalTypeHandler["pa.RecordBatch"]):
            eval_type = unused_eval_type

            def run(self, split_index, data):
                return data

        try:

            def _define_duplicate():
                class _Dup(BatchEvalTypeHandler["pa.RecordBatch"]):
                    eval_type = unused_eval_type

                    def run(self, split_index, data):
                        return data

            self.assertRaises(AssertionError, _define_duplicate)
            # The failed definition must not clobber the existing registration.
            self.assertIs(get_eval_type_handler(unused_eval_type), _First)
        finally:
            _eval_type_handlers.pop(unused_eval_type, None)

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


if __name__ == "__main__":
    from pyspark.testing import main

    main()
