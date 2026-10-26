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
import unittest
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
from pyspark.sql.pandas.serializers import (
    ArrowStreamCoGroupSerializer,
    ArrowStreamGroupSerializer,
    ArrowStreamSerializer,
)
from pyspark.sql.types import LongType
from pyspark.testing.utils import (
    PySparkBaseTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import PythonEvalType

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker_util import RunnerConf


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
            _multi_udf = True

            def run(self, split_index, data):
                return data

        class _Grouped(GroupedEvalTypeHandler["pa.RecordBatch"]):
            _multi_udf = True

            def run(self, split_index, data):
                return data

        class _CoGrouped(CoGroupedEvalTypeHandler["pa.RecordBatch"]):
            _multi_udf = True

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
            _multi_udf = True

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


class HandlerUDFsTests(PySparkBaseTestCase):
    class _Handler(BatchEvalTypeHandler["pa.RecordBatch"]):
        _multi_udf = True

        def run(self, split_index, data):
            return data

    @staticmethod
    def _stop(x):
        raise StopIteration(x)

    @staticmethod
    def _udf(*funcs):
        return UDF(
            funcs=list(funcs),
            args_offsets=[1],
            kwargs_offsets={"y": 2},
            return_type=LongType(),
            result_id=0,
        )

    def _new(self, handler_cls, runner_conf=None):
        return handler_cls([self._udf(self._stop)], runner_conf or RunnerConf({}), None)

    def test_wraps_udfs(self):
        handler = self._new(self._Handler)
        udf = handler._udf_descs[0]

        self.assertIsInstance(udf, UDF)
        self.assertEqual(udf.args_offsets, [1])
        self.assertEqual(udf.kwargs_offsets, {"y": 2})
        self.assertEqual(udf.return_type, LongType())
        # The handler keeps the function wrapped in fail_on_stopiteration; the record and the
        # handler's raw function keep it unwrapped.
        with self.assertRaises(PySparkRuntimeError):
            handler._funcs[0](1)
        with self.assertRaises(StopIteration):
            udf.funcs[0](1)
        self.assertIs(handler._raw_funcs[0], self._stop)

    def test_fail_on_stopiteration_per_handler(self):
        not_wrapped = {
            PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
            PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF,
            PythonEvalType.SQL_ARROW_BATCHED_UDF,
        }
        for _, variants in list(_eval_type_handlers.values()):
            for handler_cls in variants.values():
                with self.subTest(handler=handler_cls.__name__):
                    self.assertEqual(
                        handler_cls._fail_on_stopiteration,
                        handler_cls.eval_type not in not_wrapped,
                    )

        class _NotWrapped(self._Handler):
            _fail_on_stopiteration = False

        with self.assertRaises(StopIteration):
            self._new(_NotWrapped)._funcs[0](1)

    def test_profiler_selected_by_conf(self):
        def f(x):
            return x

        def data_source_f(x):
            return x

        data_source_f.__module__ = "pyspark.sql.worker.plan_data_source_read"

        udf_perf = self._Handler([], RunnerConf({"spark.sql.pyspark.udf.profiler": "perf"}), None)
        data_source_perf = self._Handler(
            [], RunnerConf({"spark.sql.pyspark.dataSource.profiler": "perf"}), None
        )
        udf_memory = self._Handler(
            [], RunnerConf({"spark.sql.pyspark.udf.profiler": "memory"}), None
        )
        no_profiler = self._Handler([], RunnerConf({}), None)
        with patch.object(
            EvalTypeHandler, "_with_perf_profiler", side_effect=lambda func, result_id: "perf"
        ) as perf:
            self.assertEqual(udf_perf._with_profiler(f, 0), "perf")
            self.assertIs(data_source_perf._with_profiler(f, 0), f)
            self.assertIs(udf_perf._with_profiler(data_source_f, 0), data_source_f)
            self.assertEqual(data_source_perf._with_profiler(data_source_f, 0), "perf")
            self.assertEqual(perf.call_count, 2)
        with patch.object(
            EvalTypeHandler, "_with_memory_profiler", side_effect=lambda func, result_id: "memory"
        ):
            self.assertEqual(udf_memory._with_profiler(f, 0), "memory")
        self.assertIs(no_profiler._with_profiler(f, 0), f)

    def test_chains_funcs_in_order(self):
        handler = self._Handler(
            [self._udf(lambda x: x + 1, lambda x: x * 2, str)], RunnerConf({}), None
        )
        self.assertEqual(handler._raw_funcs[0](3), "8")
        self.assertEqual(handler._funcs[0](3), "8")

    def test_single_func_not_chained(self):
        # A single function is kept as is, so its signature (and an incremental Aggregator
        # object) survives.
        def f(key, value):
            return value

        handler = self._Handler([self._udf(f)], RunnerConf({}), None)
        self.assertIs(handler._raw_funcs[0], f)

    def test_single_udf(self):
        class _Single(BatchEvalTypeHandler["pa.RecordBatch"]):
            def run(self, split_index, data):
                return data

        def f(x):
            return x + 1

        handler = _Single([self._udf(f)], RunnerConf({}), None)
        self.assertIs(handler._udf_desc, handler._udf_descs[0])
        self.assertIs(handler._raw_func, f)
        self.assertIs(handler._func, handler._funcs[0])
        self.assertEqual(handler._func(1), 2)
        for udf_descs in ([], [self._udf(f), self._udf(f)]):
            with self.subTest(num_udfs=len(udf_descs)):
                with self.assertRaisesRegex(
                    PySparkRuntimeError,
                    "^One _Single UDF expected, but got {}.$".format(len(udf_descs)),
                ):
                    _Single(udf_descs, RunnerConf({}), None)

        handler = self._Handler([self._udf(f), self._udf(f)], RunnerConf({}), None)
        self.assertFalse(hasattr(handler, "_udf_desc"))

    @unittest.skipIf(
        not (have_pandas and have_pyarrow),
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_single_udf_count_error_names_eval_type(self):
        for eval_type, expected in (
            (PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF, "COGROUPED_MAP_ARROW UDF"),
            (
                PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
                "GROUPED_MAP_PANDAS_UDF_WITH_STATE UDF",
            ),
            (PythonEvalType.SQL_TABLE_UDF, "UDTF"),
        ):
            handler_cls = _eval_type_handlers[eval_type][1][None]
            with self.subTest(handler=handler_cls.__name__):
                with self.assertRaisesRegex(
                    PySparkRuntimeError, "^One {} expected, but got 2.$".format(expected)
                ):
                    handler_cls([self._udf(str), self._udf(str)], RunnerConf({}), None)

    def test_multi_udf_per_handler(self):
        multi = {
            PythonEvalType.SQL_BATCHED_UDF,
            PythonEvalType.SQL_ARROW_BATCHED_UDF,
            PythonEvalType.SQL_SCALAR_PANDAS_UDF,
            PythonEvalType.SQL_SCALAR_ARROW_UDF,
            PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF,
            PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF,
            PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF,
            PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
            PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF,
            PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
            PythonEvalType.SQL_WINDOW_AGG_ARROW_UDF,
            PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_PARTIAL_UDF,
            PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF,
            PythonEvalType.SQL_WINDOW_AGG_ARROW_INCREMENTAL_UDF,
        }
        for _, variants in list(_eval_type_handlers.values()):
            for handler_cls in variants.values():
                with self.subTest(handler=handler_cls.__name__):
                    self.assertEqual(handler_cls._multi_udf, handler_cls.eval_type in multi)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
