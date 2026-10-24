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

"""Tests for the pandas eval type handlers (``_pandas``).

The handler tests build handler input in the same wire format the serializers
produce -- a flat RecordBatch, one column per UDF argument -- and assert on the
output batches via ``run(0, <input>)``. Handlers are built on a real ``RunnerConf``
(from a plain conf dict), so the tests exercise the actual conf keys the handler
reads; the forwarding tests pass a single conf key to confirm it reaches the
Arrow<->pandas conversion.
"""

import os
import unittest
from unittest.mock import patch

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.eval_handlers._typing import UDF
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType
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

    from pyspark.eval_handlers._pandas import (
        ArrowBatchedUDFLegacyPandasHandler,
        PandasCoGroupedMapUDFHandler,
        PandasGroupedAggIterUDFHandler,
        PandasGroupedAggUDFHandler,
        PandasGroupedMapIterUDFHandler,
        PandasGroupedMapUDFHandler,
        PandasMapUDFHandler,
        PandasScalarIterUDFHandler,
        PandasScalarUDFHandler,
        PandasWindowAggUDFHandler,
    )
    from pyspark.sql.conversion import ArrowBatchTransformer

_missing_message = pandas_requirement_message or pyarrow_requirement_message


class PandasEvalTypeHandlerTestCase(PySparkBaseTestCase):
    """Builders for handler input in the wire format the serializers produce."""

    _GROUPED_RETURN_TYPE = StructType([StructField("v", LongType())])

    @staticmethod
    def _batch(**columns):
        """A RecordBatch of int64 columns, one per ``name=values`` kwarg."""
        return pa.RecordBatch.from_arrays(
            [pa.array(values, type=pa.int64()) for values in columns.values()],
            list(columns),
        )

    @staticmethod
    def _grouped_arg_offsets(*dataframes):
        """Encode ``arg_offsets`` from ``(key_cols, value_cols)`` per DataFrame, laid out
        as ``[length, num_keys, *key_cols, *value_cols]`` (PandasGroupUtils.resolveArgOffsets).
        """
        offsets: list = []
        for key_cols, value_cols in dataframes:
            group = [len(key_cols), *key_cols, *value_cols]
            offsets += [len(group), *group]
        return offsets

    @classmethod
    def _grouped_handler(cls, handler_cls, udf, arg_offsets, runner_conf=None):
        """Build a grouped/cogrouped-map handler. The function the handler wraps loses its
        signature, so the handler reads it from its ``_raw_func``."""
        return handler_cls(
            udfs=[
                UDF(
                    funcs=[udf],
                    args_offsets=arg_offsets,
                    kwargs_offsets={},
                    return_type=cls._GROUPED_RETURN_TYPE,
                    result_id=0,
                )
            ],
            runner_conf=runner_conf or RunnerConf({}),
            eval_conf=None,
        )

    @staticmethod
    def _struct_values(batches):
        """Flatten the ``v`` field of each output batch's wrapping struct column."""
        return [v for b in batches for v in b.column(0).field("v").to_pylist()]

    @staticmethod
    def _udf(func, return_type=None, args=(0,), kwargs=None):
        """A ``UDF`` record for a pandas UDF, as the worker builds it."""
        return UDF(
            funcs=[func],
            args_offsets=list(args),
            kwargs_offsets=dict(kwargs or {}),
            return_type=return_type or LongType(),
            result_id=0,
        )

    @staticmethod
    def _agg_handler(handler_cls, *udfs, runner_conf=None):
        """Build a grouped-agg/window-agg handler from one or more ``_udf`` records."""
        return handler_cls(
            udfs=list(udfs), runner_conf=runner_conf or RunnerConf({}), eval_conf=None
        )

    @staticmethod
    def _columns(batches):
        """Each output column's values, concatenated across batches."""
        return [
            [v for b in batches for v in b.column(i).to_pylist()]
            for i in range(batches[0].num_columns)
        ]


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasEvalTypeHandlerRegistrationTests(PySparkBaseTestCase):
    def test_pandas_eval_types_are_registered(self):
        # Each migrated pandas eval type dispatches to its handler by lookup.
        for eval_type, handler_cls in (
            (PythonEvalType.SQL_SCALAR_PANDAS_UDF, PandasScalarUDFHandler),
            (PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF, PandasScalarIterUDFHandler),
            (PythonEvalType.SQL_MAP_PANDAS_ITER_UDF, PandasMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF, PandasGroupedMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_MAP_PANDAS_ITER_UDF, PandasGroupedMapIterUDFHandler),
            (PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF, PandasCoGroupedMapUDFHandler),
            (PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF, PandasGroupedAggUDFHandler),
            (PythonEvalType.SQL_GROUPED_AGG_PANDAS_ITER_UDF, PandasGroupedAggIterUDFHandler),
            (PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF, PandasWindowAggUDFHandler),
        ):
            self.assertIs(get_eval_type_handler(eval_type, RunnerConf({})), handler_cls)

    def test_arrow_batched_udf_legacy_pandas_variant_is_registered(self):
        # SQL_ARROW_BATCHED_UDF picks its variant by the legacy pandas conversion conf.
        conf = RunnerConf(
            {"spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": "true"}
        )
        self.assertIs(
            get_eval_type_handler(PythonEvalType.SQL_ARROW_BATCHED_UDF, conf),
            ArrowBatchedUDFLegacyPandasHandler,
        )


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class ArrowBatchedUDFLegacyPandasHandlerTests(PandasEvalTypeHandlerTestCase):
    _INPUT_TYPE = StructType([StructField("a", LongType()), StructField("b", LongType())])

    def _handler(self, *udfs, runner_conf=None, input_type=None):
        """Build the handler from one or more ``_udf`` records."""
        return ArrowBatchedUDFLegacyPandasHandler(
            udfs=list(udfs),
            runner_conf=runner_conf or RunnerConf({}),
            eval_conf=EvalConf({"input_type": (input_type or self._INPUT_TYPE).json()}),
        )

    def test_invokes_udf_per_row(self):
        handler = self._handler(
            self._udf(lambda a: a + 1),
            self._udf(lambda a, b: a * b, args=(0, 1)),
        )
        out = list(
            handler.run(0, iter([self._batch(a=[1, 2], b=[10, 20]), self._batch(a=[3], b=[4])]))
        )
        self.assertEqual(
            [b.to_pydict() for b in out],
            [{"_0": [2, 3], "_1": [10, 40]}, {"_0": [4], "_1": [12]}],
        )

    def test_passes_arg_by_keyword_offset(self):
        handler = self._handler(self._udf(lambda x, y: x - y, args=(0,), kwargs={"y": 1}))
        out = list(handler.run(0, iter([self._batch(a=[10, 20], b=[3, 5])])))
        self.assertEqual(out[0].column(0).to_pylist(), [7, 15])

    def test_zero_arg_udf(self):
        handler = self._handler(self._udf(lambda: 7, args=()), input_type=StructType())
        empty = pa.RecordBatch.from_pydict({"a": [1, 2, 3]}).select([])
        out = list(handler.run(0, iter([empty])))
        self.assertEqual(out[0].column(0).to_pylist(), [7, 7, 7])

    def test_coerces_string_and_binary_results(self):
        # The legacy conversion coerces results with str() / bytes() for String / Binary.
        handler = self._handler(
            self._udf(lambda a: a, return_type=StringType()),
            self._udf(lambda a: None if a == 2 else bytearray(b"x"), return_type=BinaryType()),
        )
        out = list(handler.run(0, iter([self._batch(a=[1, 2], b=[0, 0])])))
        self.assertEqual(out[0].column(0).to_pylist(), ["1", "2"])
        self.assertEqual(out[0].column(1).to_pylist(), [b"x", None])

    def test_concurrency_level(self):
        conf = RunnerConf({"spark.sql.execution.pythonUDF.arrow.concurrency.level": "4"})
        handler = self._handler(self._udf(lambda a: a * 2), runner_conf=conf)
        out = list(handler.run(0, iter([self._batch(a=list(range(10)), b=[0] * 10)])))
        self.assertEqual(out[0].column(0).to_pylist(), [v * 2 for v in range(10)])

    def test_stop_iteration_raises_runtime_error(self):
        def stop(a):
            raise StopIteration()

        handler = self._handler(self._udf(stop))
        with self.assertRaises(PySparkRuntimeError):
            list(handler.run(0, iter([self._batch(a=[1], b=[0])])))


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasScalarUDFHandlerTests(PandasEvalTypeHandlerTestCase):
    @staticmethod
    def _handler(*udfs, runner_conf=None):
        """Build a PandasScalarUDFHandler from one or more ``_udf`` records."""
        return PandasScalarUDFHandler(
            udfs=list(udfs), runner_conf=runner_conf or RunnerConf({}), eval_conf=None
        )

    def test_invokes_udf_per_batch(self):
        handler = self._handler(self._udf(lambda s: s + 1))
        out = list(handler.run(0, iter([self._batch(a=[1, 2, 3])])))
        self.assertEqual([b.column(0).to_pylist() for b in out], [[2, 3, 4]])

    def test_coerces_output_to_return_type(self):
        # The UDF returns int32, but the declared return type is LongType (int64).
        handler = self._handler(self._udf(lambda s: (s + 1).astype("int32")))
        out = list(handler.run(0, iter([self._batch(a=[10, 20])])))
        self.assertEqual(out[0].schema.field(0).type, pa.int64())
        self.assertEqual(out[0].column(0).to_pylist(), [11, 21])

    def test_multiple_udfs_produce_one_column_each(self):
        handler = self._handler(
            self._udf(lambda s: s + 1, args=(0,)),
            self._udf(lambda s: s * 2, args=(1,)),
        )
        out = list(handler.run(0, iter([self._batch(a=[1, 2], b=[10, 20])])))
        self.assertEqual(out[0].column(0).to_pylist(), [2, 3])
        self.assertEqual(out[0].column(1).to_pylist(), [20, 40])

    def test_passes_arg_by_keyword_offset(self):
        # Validates the inline args/kwargs offset handling: y is bound by keyword.
        handler = self._handler(self._udf(lambda x, y: x - y, args=(0,), kwargs={"y": 1}))
        out = list(handler.run(0, iter([self._batch(a=[10, 20], b=[3, 5])])))
        self.assertEqual(out[0].column(0).to_pylist(), [7, 15])

    def test_struct_return_type_takes_dataframe(self):
        # struct_in_pandas="dict" + df_for_struct=True: a struct return is a DataFrame.
        struct_type = StructType([StructField("x", LongType()), StructField("y", LongType())])
        handler = self._handler(
            self._udf(lambda s: pd.DataFrame({"x": s, "y": s * 10}), return_type=struct_type)
        )
        out = list(handler.run(0, iter([self._batch(a=[1, 2])])))
        self.assertEqual(
            out[0].column(0).to_pylist(),
            [{"x": 1, "y": 10}, {"x": 2, "y": 20}],
        )

    def test_rejects_non_sized_result(self):
        handler = self._handler(self._udf(lambda s: 42))
        with self.assertRaises(PySparkTypeError):
            list(handler.run(0, iter([self._batch(a=[1, 2])])))

    def test_rejects_row_count_mismatch(self):
        handler = self._handler(self._udf(lambda s: s.head(1)))
        with self.assertRaises(PySparkRuntimeError):
            list(handler.run(0, iter([self._batch(a=[1, 2, 3])])))

    def test_requires_dataframe_for_struct_return(self):
        struct_type = StructType([StructField("x", LongType())])
        handler = self._handler(self._udf(lambda s: s, return_type=struct_type))
        with self.assertRaises(PySparkValueError):
            list(handler.run(0, iter([self._batch(a=[1, 2])])))

    def test_forwards_prefer_int_ext_dtype_to_input_conversion(self):
        # preferIntExtensionDtype flows to the Arrow->pandas conversion: the UDF sees
        # a nullable "Int64" input instead of the default "int64".
        dtype_name = self._udf(
            lambda s: pd.Series([str(s.dtype)] * len(s)), return_type=StringType()
        )
        conf = RunnerConf({"spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype": "true"})
        out = list(
            self._handler(dtype_name, runner_conf=conf).run(0, iter([self._batch(a=[1, 2])]))
        )
        self.assertEqual(out[0].column(0).to_pylist(), ["Int64", "Int64"])

    def test_forwards_use_large_var_types_to_output_conversion(self):
        # useLargeVarTypes flows to the pandas->Arrow conversion: a string result
        # becomes large_string rather than string.
        to_str = self._udf(lambda s: s.astype(str), return_type=StringType())
        conf = RunnerConf({"spark.sql.execution.arrow.useLargeVarTypes": "true"})
        out = list(self._handler(to_str, runner_conf=conf).run(0, iter([self._batch(a=[1, 2])])))
        self.assertTrue(pa.types.is_large_string(out[0].schema.field(0).type))


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasScalarIterUDFHandlerTests(PandasEvalTypeHandlerTestCase):
    @staticmethod
    def _handler(udf, return_type=None, args=(0,), runner_conf=None):
        """Build a PandasScalarIterUDFHandler whose one UDF reads the given arg offsets."""
        return PandasScalarIterUDFHandler(
            udfs=[PandasEvalTypeHandlerTestCase._udf(udf, return_type, args)],
            runner_conf=runner_conf or RunnerConf({}),
            eval_conf=None,
        )

    def test_invokes_udf_over_batch_stream(self):
        def add_one(series_iter):
            for s in series_iter:
                yield s + 1

        handler = self._handler(add_one)
        out = list(handler.run(0, iter([self._batch(a=[1, 2]), self._batch(a=[3])])))
        self.assertEqual([b.column(0).to_pylist() for b in out], [[2, 3], [4]])

    def test_coerces_output_to_return_type(self):
        # The UDF yields int32, but the declared return type is LongType (int64).
        def add_one(series_iter):
            for s in series_iter:
                yield (s + 1).astype("int32")

        handler = self._handler(add_one)
        out = list(handler.run(0, iter([self._batch(a=[10, 20])])))
        self.assertEqual(out[0].schema.field(0).type, pa.int64())
        self.assertEqual(out[0].column(0).to_pylist(), [11, 21])

    def test_rejects_too_many_rows(self):
        # Emitting more rows than consumed fails fast via the output row-limit guard.
        def too_many(series_iter):
            for s in series_iter:
                yield pd.Series(list(range(len(s) + 1)))

        handler = self._handler(too_many)
        with self.assertRaises(PySparkRuntimeError) as cm:
            list(handler.run(0, iter([self._batch(a=[1, 2])])))
        self.assertEqual(cm.exception.getCondition(), "OUTPUT_EXCEEDS_INPUT_ROWS")

    def test_rejects_too_few_rows(self):
        # Emitting fewer rows than consumed fails the final row-count equality guard.
        def too_few(series_iter):
            for s in series_iter:
                yield s.head(len(s) - 1)

        handler = self._handler(too_few)
        with self.assertRaises(PySparkRuntimeError) as cm:
            list(handler.run(0, iter([self._batch(a=[1, 2, 3])])))
        self.assertEqual(cm.exception.getCondition(), "RESULT_ROWS_MISMATCH")

    def test_rejects_unconsumed_input(self):
        # A UDF that stops reading before the input stream is exhausted must raise
        # INPUT_NOT_FULLY_CONSUMED.
        def first_only(series_iter):
            yield next(series_iter)

        handler = self._handler(first_only)
        with self.assertRaises(PySparkRuntimeError) as cm:
            list(handler.run(0, iter([self._batch(a=[1, 2]), self._batch(a=[3, 4])])))
        self.assertEqual(cm.exception.getCondition(), "INPUT_NOT_FULLY_CONSUMED")

    def test_struct_return_takes_dataframe(self):
        # struct_in_pandas="dict" + df_for_struct=True: a struct return is a DataFrame.
        struct_type = StructType([StructField("x", LongType()), StructField("y", LongType())])

        def to_struct(series_iter):
            for s in series_iter:
                yield pd.DataFrame({"x": s, "y": s * 10})

        handler = self._handler(to_struct, return_type=struct_type)
        out = list(handler.run(0, iter([self._batch(a=[1, 2])])))
        self.assertEqual(
            out[0].column(0).to_pylist(),
            [{"x": 1, "y": 10}, {"x": 2, "y": 20}],
        )


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasMapUDFHandlerTests(PandasEvalTypeHandlerTestCase):
    _return_type = StructType([StructField("v", LongType())])

    @staticmethod
    def _handler(udf, return_type, runner_conf=None):
        """Build a PandasMapUDFHandler; map UDFs take no arg offsets."""
        return PandasMapUDFHandler(
            udfs=[PandasEvalTypeHandlerTestCase._udf(udf, return_type, args=())],
            runner_conf=runner_conf or RunnerConf({}),
            eval_conf=None,
        )

    def test_maps_batch_stream(self):
        # mapInPandas expands the wire struct into a DataFrame and re-wraps the output.
        def double_v(df_iter):
            for df in df_iter:
                yield pd.DataFrame({"v": df["v"] * 2})

        handler = self._handler(double_v, self._return_type)
        # mapInPandas sends a single struct column per batch; the handler expands it to
        # a DataFrame, so build the input in that wire format here.
        struct_batch = ArrowBatchTransformer.wrap_struct(self._batch(v=[1, 2, 3]))
        out = list(handler.run(0, iter([struct_batch])))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [2, 4, 6])

    def test_legacy_accepts_any_iterable(self):
        # With the legacy flag on (its default), a UDF may return any iterable -- here a
        # plain list of DataFrames -- which the handler adapts via iter() before verifying.
        def as_list(df_iter):
            return [pd.DataFrame({"v": df["v"] * 2}) for df in df_iter]

        handler = self._handler(as_list, self._return_type)
        struct_batch = ArrowBatchTransformer.wrap_struct(self._batch(v=[1, 2, 3]))
        out = list(handler.run(0, iter([struct_batch])))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [2, 4, 6])

    def test_rejects_non_iterator_result(self):
        # With the legacy accept-any-iterable flag off, a UDF returning a DataFrame (not
        # an iterator of them) is rejected as a non-iterator before the input stream is
        # read, so the input batch shape is irrelevant.
        strict = RunnerConf(
            {"spark.sql.execution.pythonUDF.mapInBatch.legacy.acceptAnyIterable.enabled": "false"}
        )
        handler = self._handler(
            lambda df_iter: pd.DataFrame({"v": [1]}), self._return_type, runner_conf=strict
        )
        with self.assertRaises(PySparkTypeError) as cm:
            list(handler.run(0, iter([self._batch(v=[1, 2])])))
        # The non-iterator branch reports the actual type ("DataFrame"); the per-element
        # branch (test_rejects_wrong_element_type) reports "iterator of ...".
        self.assertEqual(cm.exception.getMessageParameters()["actual"], "DataFrame")

    def test_rejects_wrong_element_type(self):
        # Each yielded element must be a DataFrame for a struct return type.
        def yields_series(df_iter):
            for _ in df_iter:
                yield pd.Series([1, 2])  # a Series, not a DataFrame

        handler = self._handler(yields_series, self._return_type)
        with self.assertRaises(PySparkTypeError):
            list(handler.run(0, iter([self._batch(v=[1, 2])])))


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasGroupedMapUDFHandlerTests(PandasEvalTypeHandlerTestCase):
    # arg_offsets for key column 0 and value column 1.
    _KEY0_VALUE1 = PandasEvalTypeHandlerTestCase._grouped_arg_offsets(([0], [1]))

    def test_applies_udf_per_group(self):
        # Each group (a lazy iterator of batches) is concatenated into one DataFrame.
        handler = self._grouped_handler(
            PandasGroupedMapUDFHandler,
            lambda df: pd.DataFrame({"v": df["b"] * 2}),
            self._KEY0_VALUE1,
        )
        groups = [
            iter([self._batch(a=[1, 1], b=[1, 2]), self._batch(a=[1], b=[3])]),
            iter([self._batch(a=[2], b=[5])]),
        ]
        out = list(handler.run(0, iter(groups)))
        self.assertEqual(len(out), 2)
        self.assertEqual(self._struct_values(out[:1]), [2, 4, 6])
        self.assertEqual(self._struct_values(out[1:]), [10])

    def test_passes_grouping_key(self):
        # A two-argument UDF receives the grouping key tuple from the key columns.
        handler = self._grouped_handler(
            PandasGroupedMapUDFHandler,
            lambda key, df: pd.DataFrame({"v": [key[0]] * len(df)}),
            self._KEY0_VALUE1,
        )
        out = list(handler.run(0, iter([iter([self._batch(a=[7, 7], b=[1, 2])])])))
        self.assertEqual(self._struct_values(out), [7, 7])

    def test_coerces_output_to_return_type(self):
        handler = self._grouped_handler(
            PandasGroupedMapUDFHandler,
            lambda df: pd.DataFrame({"v": df["b"].astype("int32")}),
            self._KEY0_VALUE1,
        )
        out = list(handler.run(0, iter([iter([self._batch(a=[1], b=[3])])])))
        self.assertEqual(out[0].column(0).type.field("v").type, pa.int64())

    def test_rejects_non_dataframe_result(self):
        handler = self._grouped_handler(
            PandasGroupedMapUDFHandler, lambda df: df["b"], self._KEY0_VALUE1
        )
        with self.assertRaises(PySparkTypeError):
            list(handler.run(0, iter([iter([self._batch(a=[1], b=[3])])])))

    def test_resizes_output_to_byte_cap(self):
        # A positive output byte cap splits one large group result into several batches.
        conf = RunnerConf({"spark.sql.execution.pythonUDF.arrow.workerOutputBatchMaxBytes": "64"})
        handler = self._grouped_handler(
            PandasGroupedMapUDFHandler,
            lambda df: pd.DataFrame({"v": df["b"]}),
            self._KEY0_VALUE1,
            runner_conf=conf,
        )
        values = list(range(100))
        out = list(handler.run(0, iter([iter([self._batch(a=[0] * 100, b=values)])])))
        self.assertGreater(len(out), 1)
        self.assertEqual(self._struct_values(out), values)


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasGroupedMapIterUDFHandlerTests(PandasEvalTypeHandlerTestCase):
    # arg_offsets for key column 0 and value column 1.
    _KEY0_VALUE1 = PandasEvalTypeHandlerTestCase._grouped_arg_offsets(([0], [1]))

    def test_applies_udf_over_group_batches(self):
        # The UDF sees one DataFrame per input batch of the group.
        def double(df_iter):
            for df in df_iter:
                yield pd.DataFrame({"v": df["b"] * 2})

        handler = self._grouped_handler(PandasGroupedMapIterUDFHandler, double, self._KEY0_VALUE1)
        groups = [
            iter([self._batch(a=[1, 1], b=[1, 2]), self._batch(a=[1], b=[3])]),
            iter([self._batch(a=[2], b=[5])]),
        ]
        out = list(handler.run(0, iter(groups)))
        self.assertEqual([self._struct_values([b]) for b in out], [[2, 4], [6], [10]])

    def test_passes_grouping_key(self):
        def with_key(key, df_iter):
            for df in df_iter:
                yield pd.DataFrame({"v": [key[0]] * len(df)})

        handler = self._grouped_handler(PandasGroupedMapIterUDFHandler, with_key, self._KEY0_VALUE1)
        out = list(handler.run(0, iter([iter([self._batch(a=[7, 7], b=[1, 2])])])))
        self.assertEqual(self._struct_values(out), [7, 7])

    def test_drains_unconsumed_group_batches(self):
        # A UDF that reads only the first batch must not leave the rest of its group
        # in the stream: the next group still starts at its own first batch.
        def first_only(df_iter):
            yield pd.DataFrame({"v": next(df_iter)["b"]})

        handler = self._grouped_handler(
            PandasGroupedMapIterUDFHandler, first_only, self._KEY0_VALUE1
        )
        group1 = iter([self._batch(a=[1], b=[1]), self._batch(a=[1], b=[2])])
        group2 = iter([self._batch(a=[2], b=[3])])
        out = list(handler.run(0, iter([group1, group2])))
        self.assertEqual(self._struct_values(out), [1, 3])
        self.assertIsNone(next(group1, None))

    def test_rejects_non_dataframe_element(self):
        def yields_series(df_iter):
            for df in df_iter:
                yield df["b"]

        handler = self._grouped_handler(
            PandasGroupedMapIterUDFHandler, yields_series, self._KEY0_VALUE1
        )
        with self.assertRaises(PySparkTypeError):
            list(handler.run(0, iter([iter([self._batch(a=[1], b=[3])])])))


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasCoGroupedMapUDFHandlerTests(PandasEvalTypeHandlerTestCase):
    # Both sides: key column 0, value column 1.
    _offsets = PandasEvalTypeHandlerTestCase._grouped_arg_offsets(([0], [1]), ([0], [1]))

    def test_applies_udf_per_cogroup(self):
        handler = self._grouped_handler(
            PandasCoGroupedMapUDFHandler,
            lambda left, right: pd.DataFrame({"v": [left["b"].sum() + right["b"].sum()]}),
            self._offsets,
        )
        cogroup = (
            [self._batch(a=[1], b=[1]), self._batch(a=[1], b=[2])],
            [self._batch(a=[1], b=[10])],
        )
        out = list(handler.run(0, iter([cogroup])))
        self.assertEqual(self._struct_values(out), [13])

    def test_key_from_right_when_left_empty(self):
        # A three-argument UDF reads the key from the left side, falling back to the
        # right side when the left side has no rows.
        handler = self._grouped_handler(
            PandasCoGroupedMapUDFHandler,
            lambda key, left, right: pd.DataFrame({"v": [key[0]]}),
            self._offsets,
        )
        cogroups = [
            ([self._batch(a=[4], b=[1])], [self._batch(a=[], b=[])]),
            ([self._batch(a=[], b=[])], [self._batch(a=[9], b=[1])]),
        ]
        out = list(handler.run(0, iter(cogroups)))
        self.assertEqual(self._struct_values(out), [4, 9])

    def test_rejects_non_dataframe_result(self):
        handler = self._grouped_handler(
            PandasCoGroupedMapUDFHandler, lambda left, right: 1, self._offsets
        )
        cogroup = ([self._batch(a=[1], b=[1])], [self._batch(a=[1], b=[2])])
        with self.assertRaises(PySparkTypeError):
            list(handler.run(0, iter([cogroup])))


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasGroupedAggUDFHandlerTests(PandasEvalTypeHandlerTestCase):
    def test_one_row_per_group_one_column_per_udf(self):
        handler = self._agg_handler(
            PandasGroupedAggUDFHandler,
            self._udf(lambda s: s.sum(), args=(1,)),
            self._udf(lambda a, b: (a * b).max(), args=(0, 1)),
        )
        groups = [
            iter([self._batch(a=[1, 1], b=[1, 2]), self._batch(a=[1], b=[3])]),
            iter([self._batch(a=[2], b=[5])]),
        ]
        out = list(handler.run(0, iter(groups)))
        self.assertEqual([b.num_rows for b in out], [1, 1])
        self.assertEqual(self._columns(out), [[6, 5], [3, 10]])

    def test_passes_kwargs(self):
        handler = self._agg_handler(
            PandasGroupedAggUDFHandler, self._udf(lambda *, x: x.sum(), args=(), kwargs={"x": 0})
        )
        out = list(handler.run(0, iter([iter([self._batch(a=[1, 2, 3])])])))
        self.assertEqual(self._columns(out), [[6]])

    def test_skips_empty_group(self):
        handler = self._agg_handler(PandasGroupedAggUDFHandler, self._udf(lambda s: s.sum()))
        out = list(handler.run(0, iter([iter([]), iter([self._batch(a=[4])])])))
        self.assertEqual(self._columns(out), [[4]])

    def test_honors_large_var_types(self):
        conf = RunnerConf({"spark.sql.execution.arrow.useLargeVarTypes": "true"})
        handler = self._agg_handler(
            PandasGroupedAggUDFHandler,
            self._udf(lambda s: str(s.sum()), return_type=StringType()),
            runner_conf=conf,
        )
        out = list(handler.run(0, iter([iter([self._batch(a=[1, 2])])])))
        self.assertEqual(out[0].column(0).type, pa.large_string())


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasGroupedAggIterUDFHandlerTests(PandasEvalTypeHandlerTestCase):
    def test_single_arg_receives_series_per_batch(self):
        def total(series_iter):
            return sum(s.sum() for s in series_iter)

        handler = self._agg_handler(PandasGroupedAggIterUDFHandler, self._udf(total, args=(1,)))
        groups = [
            iter([self._batch(a=[1, 1], b=[1, 2]), self._batch(a=[1], b=[3])]),
            iter([self._batch(a=[2], b=[5])]),
        ]
        out = list(handler.run(0, iter(groups)))
        self.assertEqual(self._columns(out), [[6, 5]])

    def test_multiple_args_receive_tuple_of_series(self):
        def dot(pairs):
            return sum((a * b).sum() for a, b in pairs)

        handler = self._agg_handler(PandasGroupedAggIterUDFHandler, self._udf(dot, args=(0, 1)))
        group = iter([self._batch(a=[1, 2], b=[3, 4]), self._batch(a=[5], b=[6])])
        out = list(handler.run(0, iter([group])))
        self.assertEqual(self._columns(out), [[41]])

    def test_drains_unconsumed_group_batches(self):
        # A UDF that reads only the first batch must not leave the rest of its group
        # in the stream.
        handler = self._agg_handler(
            PandasGroupedAggIterUDFHandler, self._udf(lambda it: next(it).sum())
        )
        group1 = iter([self._batch(a=[1]), self._batch(a=[2])])
        group2 = iter([self._batch(a=[3])])
        out = list(handler.run(0, iter([group1, group2])))
        self.assertEqual(self._columns(out), [[1, 3]])
        self.assertIsNone(next(group1, None))

    def test_ignores_large_var_types(self):
        # This eval type always emits non-large Arrow types, regardless of the conf.
        conf = RunnerConf({"spark.sql.execution.arrow.useLargeVarTypes": "true"})
        handler = self._agg_handler(
            PandasGroupedAggIterUDFHandler,
            self._udf(lambda it: str(sum(s.sum() for s in it)), return_type=StringType()),
            runner_conf=conf,
        )
        out = list(handler.run(0, iter([iter([self._batch(a=[1, 2])])])))
        self.assertEqual(out[0].column(0).type, pa.string())


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasWindowAggUDFHandlerTests(PandasEvalTypeHandlerTestCase):
    @staticmethod
    def _conf(*bound_types):
        return RunnerConf({"window_bound_types": ",".join(bound_types)})

    def test_unbounded_repeats_result_per_row(self):
        handler = self._agg_handler(
            PandasWindowAggUDFHandler,
            self._udf(lambda s: s.sum()),
            runner_conf=self._conf("unbounded"),
        )
        groups = [iter([self._batch(a=[1, 2]), self._batch(a=[3])]), iter([self._batch(a=[10])])]
        out = list(handler.run(0, iter(groups)))
        self.assertEqual([b.num_rows for b in out], [3, 1])
        self.assertEqual(self._columns(out), [[6, 6, 6, 10]])

    def test_bounded_slices_per_row(self):
        # args_offsets[0] / [1] are the per-row frame begin / end indexes.
        handler = self._agg_handler(
            PandasWindowAggUDFHandler,
            self._udf(lambda s: s.sum(), args=(0, 1, 2)),
            runner_conf=self._conf("bounded"),
        )
        group = iter([self._batch(begin=[0, 0, 1], end=[1, 2, 3], v=[1, 2, 3])])
        out = list(handler.run(0, iter([group])))
        self.assertEqual(self._columns(out), [[1, 3, 5]])

    def test_bounded_slices_kwargs(self):
        handler = self._agg_handler(
            PandasWindowAggUDFHandler,
            self._udf(lambda *, x: x.max(), args=(0, 1), kwargs={"x": 2}),
            runner_conf=self._conf("bounded"),
        )
        group = iter([self._batch(begin=[0, 1], end=[2, 2], v=[4, 7])])
        out = list(handler.run(0, iter([group])))
        self.assertEqual(self._columns(out), [[7, 7]])

    def test_mixed_bound_types(self):
        handler = self._agg_handler(
            PandasWindowAggUDFHandler,
            self._udf(lambda s: s.sum(), args=(2,)),
            self._udf(lambda s: s.sum(), args=(0, 1, 2)),
            runner_conf=self._conf("unbounded", "bounded"),
        )
        group = iter([self._batch(begin=[0, 1], end=[1, 2], v=[3, 4])])
        out = list(handler.run(0, iter([group])))
        self.assertEqual(self._columns(out), [[7, 7], [3, 4]])

    def test_skips_empty_group(self):
        handler = self._agg_handler(
            PandasWindowAggUDFHandler,
            self._udf(lambda s: s.sum()),
            runner_conf=self._conf("unbounded"),
        )
        out = list(handler.run(0, iter([iter([]), iter([self._batch(a=[5])])])))
        self.assertEqual(self._columns(out), [[5]])

    def test_rejects_invalid_bound_type(self):
        handler = self._agg_handler(
            PandasWindowAggUDFHandler, self._udf(lambda s: s.sum()), runner_conf=self._conf("bogus")
        )
        with self.assertRaises(PySparkRuntimeError) as ctx:
            list(handler.run(0, iter([iter([self._batch(a=[1])])])))
        self.assertEqual(ctx.exception.getCondition(), "INVALID_WINDOW_BOUND_TYPE")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
