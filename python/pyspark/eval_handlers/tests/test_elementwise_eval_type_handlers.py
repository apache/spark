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

"""Tests for the element-wise eval type handlers (``_elementwise``).

Every test runs a handler over one shared input layout (``ElementwiseEvalTypeHandlerTestCase``):
column ``a`` is ``array<long>``, ``b`` is ``array<array<long>>`` and ``c`` is ``array<long>``
aligned with ``a``. The scalar and scalar iterator tests are written once against flavor hooks
and run for both the Arrow and the pandas handler.
"""

import os
import unittest
from unittest.mock import patch

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.eval_handlers._typing import UDF
from pyspark.sql.pandas.serializers import ArrowStreamSerializer
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType
from pyspark.testing.utils import (
    PySparkBaseTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import PythonEvalType

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker_util import EvalConf, RunnerConf

if have_pandas and have_pyarrow:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.compute as pc

    from pyspark.eval_handlers._elementwise import (
        ArrowElementwiseUDFHandler,
        ArrowScalarElementwiseUDFHandler,
        ArrowScalarIterElementwiseUDFHandler,
        PandasScalarElementwiseUDFHandler,
        PandasScalarIterElementwiseUDFHandler,
        _ScalarElementwiseUDFHandler,
        _ScalarIterElementwiseUDFHandler,
    )


class ElementwiseEvalTypeHandlerTestCase(PySparkBaseTestCase):
    """Builders for the shared ``a`` / ``b`` / ``c`` input layout."""

    _INPUT_TYPE = StructType(
        [
            StructField("a", ArrayType(LongType())),
            StructField("b", ArrayType(ArrayType(LongType()))),
            StructField("c", ArrayType(LongType())),
        ]
    )

    # Input column values, and the expected outputs of ``a + 1``, ``b + 1`` and ``a * c``.
    _A = [[1, 2], [], None, [3]]
    _B = [[[1], [2, 3]], None, [[], None], [[4]]]
    _C = [[10, 20], [], None, [30]]
    _A_PLUS_ONE = [[2, 3], [], None, [4]]
    _B_PLUS_ONE = [[[2], [3, 4]], None, [[], None], [[5]]]
    _A_TIMES_C = [[10, 40], [], None, [90]]

    @classmethod
    def _batch(cls, a=None, b=None, c=None, large_list=False):
        """A RecordBatch of the ``a`` / ``b`` / ``c`` columns, defaulting to ``_A`` / ``_B`` /
        ``_C``."""
        list_ = pa.large_list if large_list else pa.list_
        return pa.RecordBatch.from_arrays(
            [
                pa.array(cls._A if a is None else a, type=list_(pa.int64())),
                pa.array(cls._B if b is None else b, type=list_(list_(pa.int64()))),
                pa.array(cls._C if c is None else c, type=list_(pa.int64())),
            ],
            ["a", "b", "c"],
        )

    def _handler(self, handler_cls, *udfs, nesting=None, input_type=None, runner_conf=None):
        """Build ``handler_cls`` from ``(func, args_offsets, kwargs_offsets, return_type)``
        tuples, with ``nesting`` as the per-UDF ``elementwise_nesting``."""
        eval_conf = {"input_type": (input_type or self._INPUT_TYPE).json()}
        if nesting is not None:
            eval_conf["elementwise_nesting"] = ",".join(map(str, nesting))
        handler = handler_cls(
            runner_conf=RunnerConf(runner_conf or {"spark.sql.session.timeZone": "UTC"}),
            eval_conf=EvalConf(eval_conf),
        )
        handler._init_udfs(
            [
                UDF(
                    func=func,
                    raw_func=func,
                    args_offsets=args_offsets,
                    kwargs_offsets=kwargs_offsets,
                    return_type=return_type,
                )
                for func, args_offsets, kwargs_offsets, return_type in udfs
            ]
        )
        return handler

    @staticmethod
    def _column(out, index=0):
        """Concatenate column ``index`` of the output batches into one Python list."""
        return [v for batch in out for v in batch.column(index).to_pylist()]


@unittest.skipIf(
    not (have_pandas and have_pyarrow), pandas_requirement_message or pyarrow_requirement_message
)
class ElementwiseEvalTypeHandlerRegistrationTests(PySparkBaseTestCase):
    def test_elementwise_eval_types_are_registered(self):
        for eval_type, handler_cls in (
            (PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF, ArrowElementwiseUDFHandler),
            (PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF, PandasScalarElementwiseUDFHandler),
            (
                PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF,
                PandasScalarIterElementwiseUDFHandler,
            ),
            (PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF, ArrowScalarElementwiseUDFHandler),
            (
                PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF,
                ArrowScalarIterElementwiseUDFHandler,
            ),
        ):
            with self.subTest(eval_type=eval_type):
                self.assertIs(get_eval_type_handler(eval_type, RunnerConf({})), handler_cls)

    def test_shared_bases_are_abstract(self):
        for base in (_ScalarElementwiseUDFHandler, _ScalarIterElementwiseUDFHandler):
            with self.subTest(base=base.__name__):
                self.assertIsNone(base.eval_type)
                self.assertTrue(base.__abstractmethods__)


@unittest.skipIf(
    not (have_pandas and have_pyarrow), pandas_requirement_message or pyarrow_requirement_message
)
class ArrowElementwiseUDFHandlerTests(ElementwiseEvalTypeHandlerTestCase):
    def test_serializer(self):
        handler = self._handler(ArrowElementwiseUDFHandler, (lambda x: x, [0], {}, LongType()))
        self.assertIsInstance(handler.serializer, ArrowStreamSerializer)

    def test_invokes_udf_per_element(self):
        handler = self._handler(ArrowElementwiseUDFHandler, (lambda x: x + 1, [0], {}, LongType()))
        out = list(handler.run(0, iter([self._batch()])))
        self.assertEqual(out[0].schema.names, ["_0"])
        self.assertEqual(self._column(out), self._A_PLUS_ONE)

    def test_nesting_greater_than_one(self):
        handler = self._handler(
            ArrowElementwiseUDFHandler, (lambda x: x + 1, [1], {}, LongType()), nesting=[2]
        )
        out = list(handler.run(0, iter([self._batch()])))
        self.assertEqual(self._column(out), self._B_PLUS_ONE)

    def test_fused_udfs_renest_by_own_shape(self):
        handler = self._handler(
            ArrowElementwiseUDFHandler,
            (lambda x: x + 1, [0], {}, LongType()),
            (lambda x: x + 1, [1], {}, LongType()),
            (lambda x, y: x * y, [0, 2], {}, LongType()),
            nesting=[1, 2, 1],
        )
        out = list(handler.run(0, iter([self._batch(), self._batch()])))
        self.assertEqual([b.schema.names for b in out], [["_0", "_1", "_2"]] * 2)
        self.assertEqual(self._column(out, 0), self._A_PLUS_ONE * 2)
        self.assertEqual(self._column(out, 1), self._B_PLUS_ONE * 2)
        self.assertEqual(self._column(out, 2), self._A_TIMES_C * 2)

    def test_passes_arg_by_keyword_offset(self):
        handler = self._handler(
            ArrowElementwiseUDFHandler, (lambda x, y: y - x, [0], {"y": 2}, LongType())
        )
        out = list(handler.run(0, iter([self._batch()])))
        self.assertEqual(self._column(out), [[9, 18], [], None, [27]])

    def test_preserves_large_list(self):
        handler = self._handler(
            ArrowElementwiseUDFHandler, (lambda x: x + 1, [1], {}, LongType()), nesting=[2]
        )
        out = list(handler.run(0, iter([self._batch(large_list=True)])))
        self.assertEqual(out[0].schema.field(0).type, pa.large_list(pa.large_list(pa.int64())))
        self.assertEqual(self._column(out), self._B_PLUS_ONE)

    def test_casts_result_to_return_type(self):
        handler = self._handler(ArrowElementwiseUDFHandler, (lambda x: x, [0], {}, StringType()))
        out = list(handler.run(0, iter([self._batch()])))
        self.assertEqual(out[0].schema.field(0).type, pa.list_(pa.string()))
        self.assertEqual(self._column(out), [["1", "2"], [], None, ["3"]])

    def test_concurrency_level(self):
        conf = {"spark.sql.execution.pythonUDF.arrow.concurrency.level": "4"}
        handler = self._handler(
            ArrowElementwiseUDFHandler, (lambda x: x + 1, [0], {}, LongType()), runner_conf=conf
        )
        out = list(handler.run(0, iter([self._batch()])))
        self.assertEqual(self._column(out), self._A_PLUS_ONE)

    def test_stop_iteration_raises_runtime_error(self):
        def stop(x):
            raise StopIteration()

        handler = self._handler(ArrowElementwiseUDFHandler, (stop, [0], {}, LongType()))
        with self.assertRaises(PySparkRuntimeError):
            list(handler.run(0, iter([self._batch()])))


class ScalarElementwiseUDFHandlerTestsMixin:
    """Tests shared by both scalar element-wise flavors; subclasses set ``handler_cls`` and define
    ``_add(x, n)`` / ``_mul(x, y)`` over the flavor's UDF input (``pa.Array`` or ``pd.Series``)."""

    handler_cls: type

    def test_serializer(self):
        handler = self._handler(self.handler_cls, (lambda x: x, [0], {}, LongType()))
        self.assertIsInstance(handler.serializer, ArrowStreamSerializer)

    def test_invokes_udf_per_batch(self):
        calls = []

        def udf(x):
            calls.append(len(x))
            return self._add(x, 1)

        handler = self._handler(self.handler_cls, (udf, [0], {}, LongType()))
        out = list(handler.run(0, iter([self._batch()])))
        self.assertEqual(calls, [3])
        self.assertEqual(out[0].schema.names, ["_0"])
        self.assertEqual(self._column(out), self._A_PLUS_ONE)

    def test_nesting_greater_than_one(self):
        handler = self._handler(
            self.handler_cls, (lambda x: self._add(x, 1), [1], {}, LongType()), nesting=[2]
        )
        out = list(handler.run(0, iter([self._batch()])))
        self.assertEqual(self._column(out), self._B_PLUS_ONE)

    def test_fused_udfs_renest_by_own_shape(self):
        handler = self._handler(
            self.handler_cls,
            (lambda x: self._add(x, 1), [0], {}, LongType()),
            (lambda x: self._add(x, 1), [1], {}, LongType()),
            (lambda x, y: self._mul(x, y), [0, 2], {}, LongType()),
            nesting=[1, 2, 1],
        )
        out = list(handler.run(0, iter([self._batch(), self._batch()])))
        self.assertEqual([b.schema.names for b in out], [["_0", "_1", "_2"]] * 2)
        self.assertEqual(self._column(out, 0), self._A_PLUS_ONE * 2)
        self.assertEqual(self._column(out, 1), self._B_PLUS_ONE * 2)
        self.assertEqual(self._column(out, 2), self._A_TIMES_C * 2)

    def test_passes_arg_by_keyword_offset(self):
        handler = self._handler(
            self.handler_cls, (lambda x, y: self._mul(x, y), [0], {"y": 2}, LongType())
        )
        out = list(handler.run(0, iter([self._batch()])))
        self.assertEqual(self._column(out), self._A_TIMES_C)

    def test_preserves_large_list(self):
        handler = self._handler(
            self.handler_cls, (lambda x: self._add(x, 1), [1], {}, LongType()), nesting=[2]
        )
        out = list(handler.run(0, iter([self._batch(large_list=True)])))
        self.assertEqual(out[0].schema.field(0).type, pa.large_list(pa.large_list(pa.int64())))
        self.assertEqual(self._column(out), self._B_PLUS_ONE)

    def test_all_empty_and_null_arrays(self):
        handler = self._handler(self.handler_cls, (lambda x: self._add(x, 1), [0], {}, LongType()))
        batch = self._batch(a=[[], None], b=[None, None], c=[[], None])
        out = list(handler.run(0, iter([batch])))
        self.assertEqual(self._column(out), [[], None])

    def test_rejects_result_length_mismatch(self):
        handler = self._handler(
            self.handler_cls, (lambda x, y: self._mul(x, y)[:1], [0, 2], {}, LongType())
        )
        with self.assertRaisesRegex(PySparkRuntimeError, "RESULT_ROWS_MISMATCH"):
            list(handler.run(0, iter([self._batch()])))

    def test_rejects_non_array_like_result(self):
        handler = self._handler(self.handler_cls, (lambda x: 1, [0], {}, LongType()))
        with self.assertRaisesRegex(PySparkTypeError, "UDF_RETURN_TYPE"):
            list(handler.run(0, iter([self._batch()])))


@unittest.skipIf(
    not (have_pandas and have_pyarrow), pandas_requirement_message or pyarrow_requirement_message
)
class ArrowScalarElementwiseUDFHandlerTests(
    ScalarElementwiseUDFHandlerTestsMixin, ElementwiseEvalTypeHandlerTestCase
):
    handler_cls = ArrowScalarElementwiseUDFHandler if have_pyarrow else None

    @staticmethod
    def _add(x, n):
        return pc.add(x, n)

    @staticmethod
    def _mul(x, y):
        return pc.multiply(x, y)

    def test_passes_pa_arrays(self):
        seen = []

        def udf(x):
            seen.append(x)
            return x

        handler = self._handler(self.handler_cls, (udf, [0], {}, LongType()))
        list(handler.run(0, iter([self._batch()])))
        self.assertIsInstance(seen[0], pa.Array)
        self.assertEqual(seen[0].to_pylist(), [1, 2, 3])

    def test_coerces_result_to_return_type(self):
        handler = self._handler(
            self.handler_cls, (lambda x: pc.cast(x, pa.int32()), [0], {}, LongType())
        )
        out = list(handler.run(0, iter([self._batch()])))
        self.assertEqual(out[0].schema.field(0).type, pa.list_(pa.int64()))
        self.assertEqual(self._column(out), self._A)


@unittest.skipIf(
    not (have_pandas and have_pyarrow), pandas_requirement_message or pyarrow_requirement_message
)
class PandasScalarElementwiseUDFHandlerTests(
    ScalarElementwiseUDFHandlerTestsMixin, ElementwiseEvalTypeHandlerTestCase
):
    handler_cls = PandasScalarElementwiseUDFHandler if have_pandas and have_pyarrow else None

    @staticmethod
    def _add(x, n):
        return x + n

    @staticmethod
    def _mul(x, y):
        return x * y

    def test_passes_unnamed_series(self):
        seen = []

        def udf(x):
            seen.append(x)
            return x

        handler = self._handler(self.handler_cls, (udf, [0], {}, LongType()))
        list(handler.run(0, iter([self._batch()])))
        self.assertIsInstance(seen[0], pd.Series)
        self.assertIsNone(seen[0].name)
        self.assertEqual(seen[0].tolist(), [1, 2, 3])

    def test_passes_struct_leaf_as_dataframe(self):
        element = StructType([StructField("v", LongType())])
        input_type = StructType([StructField("s", ArrayType(element))])
        batch = pa.RecordBatch.from_arrays(
            [pa.array([[{"v": 1}, {"v": 2}], None], type=pa.list_(pa.struct([("v", pa.int64())])))],
            ["s"],
        )
        handler = self._handler(
            self.handler_cls, (lambda df: df["v"] + 1, [0], {}, LongType()), input_type=input_type
        )
        self.assertEqual(self._column(list(handler.run(0, iter([batch])))), [[2, 3], None])

    def test_struct_return_type_requires_dataframe(self):
        return_type = StructType([StructField("v", LongType())])
        handler = self._handler(self.handler_cls, (lambda x: x, [0], {}, return_type))
        with self.assertRaises(PySparkValueError):
            list(handler.run(0, iter([self._batch()])))

    def test_struct_return_type(self):
        return_type = StructType([StructField("v", LongType())])
        handler = self._handler(
            self.handler_cls, (lambda x: pd.DataFrame({"v": x + 1}), [0], {}, return_type)
        )
        out = list(handler.run(0, iter([self._batch()])))
        self.assertEqual(self._column(out), [[{"v": 2}, {"v": 3}], [], None, [{"v": 4}]])


class ScalarIterElementwiseUDFHandlerTestsMixin:
    """Tests shared by both scalar iterator element-wise flavors; subclasses set ``handler_cls``
    and define ``_to_list(x)`` / ``_from_list(values)`` to convert between the flavor's UDF batch
    (``pa.Array`` or ``pd.Series``) and a Python list."""

    handler_cls: type

    def _iter_handler(self, udf, args_offsets=(0,), nesting=None, return_type=None):
        return self._handler(
            self.handler_cls,
            (udf, list(args_offsets), {}, return_type or LongType()),
            nesting=nesting,
        )

    def _plus_one(self, batches):
        for x in batches:
            yield self._from_list([v + 1 for v in self._to_list(x)])

    def test_serializer(self):
        self.assertIsInstance(self._iter_handler(self._plus_one).serializer, ArrowStreamSerializer)

    def test_maps_batch_stream(self):
        batches = [
            self._batch(a=self._A[:2], b=self._B[:2], c=self._C[:2]),
            self._batch(a=self._A[2:], b=self._B[2:], c=self._C[2:]),
        ]
        out = list(self._iter_handler(self._plus_one).run(0, iter(batches)))
        self.assertEqual({b.schema.names[0] for b in out}, {"_0"})
        self.assertEqual(self._column(out), self._A_PLUS_ONE)

    def test_regroups_rechunked_results(self):
        # The UDF consumes every input batch before yielding one element per chunk, so the
        # output chunks straddle the input rows and batches.
        def udf(batches):
            values = [v for x in batches for v in self._to_list(x)]
            for v in values:
                yield self._from_list([v + 1])

        out = list(self._iter_handler(udf).run(0, iter([self._batch(), self._batch()])))
        self.assertEqual(self._column(out), self._A_PLUS_ONE * 2)

    def test_nesting_greater_than_one(self):
        out = list(
            self._iter_handler(self._plus_one, (1,), nesting=[2]).run(0, iter([self._batch()]))
        )
        self.assertEqual(self._column(out), self._B_PLUS_ONE)

    def test_multiple_args(self):
        def udf(batches):
            for x, y in batches:
                yield self._from_list([a * b for a, b in zip(self._to_list(x), self._to_list(y))])

        out = list(self._iter_handler(udf, (0, 2)).run(0, iter([self._batch()])))
        self.assertEqual(self._column(out), self._A_TIMES_C)

    def test_emits_empty_and_null_arrays_without_results(self):
        # The UDF yields nothing for an all-empty batch; its rows are still emitted.
        def udf(batches):
            for x in batches:
                values = self._to_list(x)
                if values:
                    yield self._from_list([v + 1 for v in values])

        batch = self._batch(a=[[], None], b=[None, None], c=[[], None])
        out = list(self._iter_handler(udf).run(0, iter([batch])))
        self.assertEqual(self._column(out), [[], None])

    def test_rejects_extra_output_elements(self):
        def udf(batches):
            for x in batches:
                yield self._from_list(self._to_list(x) * 2)

        with self.assertRaisesRegex(PySparkRuntimeError, "OUTPUT_EXCEEDS_INPUT_ROWS"):
            list(self._iter_handler(udf).run(0, iter([self._batch()])))

    def test_rejects_missing_output_elements(self):
        def udf(batches):
            for x in batches:
                yield self._from_list(self._to_list(x)[:1])

        with self.assertRaisesRegex(PySparkRuntimeError, "RESULT_ROWS_MISMATCH"):
            list(self._iter_handler(udf).run(0, iter([self._batch()])))

    def test_rejects_unconsumed_input(self):
        def udf(batches):
            yield self._from_list([v + 1 for v in self._to_list(next(batches))])

        with self.assertRaisesRegex(PySparkRuntimeError, "INPUT_NOT_FULLY_CONSUMED"):
            list(
                self._iter_handler(udf).run(
                    0, iter([self._batch(), self._batch(a=[[]], b=[None], c=[[]])])
                )
            )

    def test_rejects_non_iterator_result(self):
        with self.assertRaisesRegex(PySparkTypeError, "UDF_RETURN_TYPE"):
            list(self._iter_handler(lambda batches: [1]).run(0, iter([self._batch()])))

    def test_rejects_wrong_element_type(self):
        def udf(batches):
            for x in batches:
                yield self._to_list(x)

        with self.assertRaisesRegex(PySparkTypeError, "UDF_RETURN_TYPE"):
            list(self._iter_handler(udf).run(0, iter([self._batch()])))


@unittest.skipIf(
    not (have_pandas and have_pyarrow), pandas_requirement_message or pyarrow_requirement_message
)
class ArrowScalarIterElementwiseUDFHandlerTests(
    ScalarIterElementwiseUDFHandlerTestsMixin, ElementwiseEvalTypeHandlerTestCase
):
    handler_cls = ArrowScalarIterElementwiseUDFHandler if have_pyarrow else None

    @staticmethod
    def _to_list(x):
        return x.to_pylist()

    @staticmethod
    def _from_list(values):
        return pa.array(values, type=pa.int64())

    def test_coerces_result_to_return_type(self):
        def udf(batches):
            for x in batches:
                yield pc.cast(x, pa.int32())

        out = list(self._iter_handler(udf).run(0, iter([self._batch()])))
        self.assertEqual(out[0].schema.field(0).type, pa.list_(pa.int64()))
        self.assertEqual(self._column(out), self._A)


@unittest.skipIf(
    not (have_pandas and have_pyarrow), pandas_requirement_message or pyarrow_requirement_message
)
class PandasScalarIterElementwiseUDFHandlerTests(
    ScalarIterElementwiseUDFHandlerTestsMixin, ElementwiseEvalTypeHandlerTestCase
):
    handler_cls = PandasScalarIterElementwiseUDFHandler if have_pandas and have_pyarrow else None

    @staticmethod
    def _to_list(x):
        return x.tolist()

    @staticmethod
    def _from_list(values):
        return pd.Series(values, dtype="int64")

    def test_struct_return_type_requires_dataframe_iterator(self):
        return_type = StructType([StructField("v", LongType())])
        with self.assertRaisesRegex(PySparkTypeError, "UDF_RETURN_TYPE"):
            list(
                self._iter_handler(self._plus_one, return_type=return_type).run(
                    0, iter([self._batch()])
                )
            )

    def test_verifies_each_chunk_schema(self):
        return_type = StructType([StructField("v", LongType())])

        def udf(batches):
            for x in batches:
                yield pd.DataFrame({"w": x})

        with self.assertRaisesRegex(PySparkRuntimeError, "RESULT_COLUMN_NAMES_MISMATCH"):
            list(self._iter_handler(udf, return_type=return_type).run(0, iter([self._batch()])))

    def test_struct_return_type(self):
        return_type = StructType([StructField("v", LongType())])

        def udf(batches):
            for x in batches:
                yield pd.DataFrame({"v": x + 1})

        out = list(self._iter_handler(udf, return_type=return_type).run(0, iter([self._batch()])))
        self.assertEqual(self._column(out), [[{"v": 2}, {"v": 3}], [], None, [{"v": 4}]])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
