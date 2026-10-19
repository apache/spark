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

import os
from types import SimpleNamespace
from unittest.mock import patch

from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import (
    BatchEvalTypeHandler,
    CoGroupedEvalTypeHandler,
    EvalTypeHandler,
    GroupedEvalTypeHandler,
    _eval_type_handlers,
    get_eval_type_handler,
)
from pyspark.eval_handlers._typing import UDF
from pyspark.serializers import CloudPickleSerializer
from pyspark.sql.pandas.serializers import (
    ArrowStreamCoGroupSerializer,
    ArrowStreamGroupSerializer,
    ArrowStreamSerializer,
)
from pyspark.sql.types import LongType
from pyspark.testing.utils import PySparkBaseTestCase
from pyspark.util import PythonEvalType
from pyspark.worker_message import UDFInfo

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker import read_single_udf
    from pyspark.worker_util import RunnerConf, pickleSer


class EvalTypeHandlerTests(PySparkBaseTestCase):
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
                base([], RunnerConf({}), None)

    def test_category_bases_are_not_registered(self):
        # Only concrete subclasses that declare an eval type are registered.
        registered = {
            cls for _, variants in _eval_type_handlers.values() for cls in variants.values()
        }
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

        self.assertIsInstance(_Batch([], RunnerConf({}), None).serializer, ArrowStreamSerializer)
        self.assertIsInstance(
            _Grouped([], RunnerConf({}), None).serializer, ArrowStreamGroupSerializer
        )
        self.assertIsInstance(
            _CoGrouped([], RunnerConf({}), None).serializer, ArrowStreamCoGroupSerializer
        )

    def test_run_produces_output(self):
        class _Doubler(BatchEvalTypeHandler["pa.RecordBatch"]):
            def run(self, split_index, data):
                for item in data:
                    yield item * 2

        handler = _Doubler([], RunnerConf({}), None)
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
            self.assertIs(get_eval_type_handler(unused_eval_type, RunnerConf({})), _First)
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

    def test_variants_selected_by_runner_conf(self):
        unused_eval_type = -3

        class _Default(BatchEvalTypeHandler["pa.RecordBatch"]):
            eval_type = unused_eval_type
            variant_conf = "use_large_var_types"
            variant_value = False

            def run(self, split_index, data):
                return data

        class _Large(BatchEvalTypeHandler["pa.RecordBatch"]):
            eval_type = unused_eval_type
            variant_conf = "use_large_var_types"
            variant_value = True

            def run(self, split_index, data):
                return data

        try:
            large = RunnerConf({"spark.sql.execution.arrow.useLargeVarTypes": "true"})
            # Selected per lookup, so one worker serves tasks with either conf value.
            self.assertIs(get_eval_type_handler(unused_eval_type, RunnerConf({})), _Default)
            self.assertIs(get_eval_type_handler(unused_eval_type, large), _Large)
            self.assertIs(get_eval_type_handler(unused_eval_type, RunnerConf({})), _Default)
        finally:
            _eval_type_handlers.pop(unused_eval_type, None)

    def test_missing_variant_raises(self):
        unused_eval_type = -4

        class _Large(BatchEvalTypeHandler["pa.RecordBatch"]):
            eval_type = unused_eval_type
            variant_conf = "use_large_var_types"
            variant_value = True

            def run(self, split_index, data):
                return data

        try:
            with self.assertRaisesRegex(RuntimeError, "-4 with use_large_var_types=False"):
                get_eval_type_handler(unused_eval_type, RunnerConf({}))
        finally:
            _eval_type_handlers.pop(unused_eval_type, None)

    def test_mixed_variant_conf_rejected(self):
        unused_eval_type = -5

        class _First(BatchEvalTypeHandler["pa.RecordBatch"]):
            eval_type = unused_eval_type
            variant_conf = "use_large_var_types"
            variant_value = True

            def run(self, split_index, data):
                return data

        def _define_other_conf():
            class _OtherConf(BatchEvalTypeHandler["pa.RecordBatch"]):
                eval_type = unused_eval_type
                variant_conf = "use_legacy_pandas_udf_conversion"
                variant_value = False

                def run(self, split_index, data):
                    return data

        def _define_no_conf():
            class _NoConf(BatchEvalTypeHandler["pa.RecordBatch"]):
                eval_type = unused_eval_type

                def run(self, split_index, data):
                    return data

        try:
            self.assertRaises(AssertionError, _define_other_conf)
            self.assertRaises(AssertionError, _define_no_conf)
            self.assertEqual(
                _eval_type_handlers[unused_eval_type], ("use_large_var_types", {True: _First})
            )
        finally:
            _eval_type_handlers.pop(unused_eval_type, None)

    def test_duplicate_variant_rejected(self):
        unused_eval_type = -6

        def _define(value):
            class _Variant(BatchEvalTypeHandler["pa.RecordBatch"]):
                eval_type = unused_eval_type
                variant_conf = "use_large_var_types"
                variant_value = value

                def run(self, split_index, data):
                    return data

            return _Variant

        try:
            first = _define(True)
            self.assertRaises(AssertionError, _define, True)
            self.assertEqual(
                _eval_type_handlers[unused_eval_type], ("use_large_var_types", {True: first})
            )
        finally:
            _eval_type_handlers.pop(unused_eval_type, None)

    def test_variant_conf_and_value_declared_together(self):
        unused_eval_type = -7

        def _define_conf_only():
            class _ConfOnly(BatchEvalTypeHandler["pa.RecordBatch"]):
                eval_type = unused_eval_type
                variant_conf = "use_large_var_types"

                def run(self, split_index, data):
                    return data

        def _define_value_only():
            class _ValueOnly(BatchEvalTypeHandler["pa.RecordBatch"]):
                eval_type = unused_eval_type
                variant_value = True

                def run(self, split_index, data):
                    return data

        self.assertRaises(AssertionError, _define_conf_only)
        self.assertRaises(AssertionError, _define_value_only)
        self.assertNotIn(unused_eval_type, _eval_type_handlers)


class ReadSingleUDFTests(PySparkBaseTestCase):
    def test_returns_udf_record(self):
        def stop(x):
            raise StopIteration(x)

        udf_info = UDFInfo(
            udfs=[CloudPickleSerializer().dumps((stop, LongType()))],
            args=[1],
            kwargs={"y": 2},
            result_id=0,
        )
        # read_single_udf only reads these two attributes of the handler class.
        handler_cls = SimpleNamespace(eval_type=PythonEvalType.SQL_BATCHED_UDF, is_iter_based=False)
        udf = read_single_udf(pickleSer, udf_info, handler_cls, RunnerConf({}))

        self.assertIsInstance(udf, UDF)
        self.assertEqual(udf.args_offsets, [1])
        self.assertEqual(udf.kwargs_offsets, {"y": 2})
        self.assertEqual(udf.return_type, LongType())
        # func is wrapped in fail_on_stopiteration; raw_func is the unwrapped function.
        with self.assertRaises(PySparkRuntimeError):
            udf.func(1)
        with self.assertRaises(StopIteration):
            udf.raw_func(1)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
