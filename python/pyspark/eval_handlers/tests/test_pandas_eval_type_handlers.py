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
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import PythonEvalType

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker_util import RunnerConf

if have_pandas and have_pyarrow:
    import pandas as pd
    import pyarrow as pa

    from pyspark.eval_handlers._pandas import (
        PandasMapUDFHandler,
        PandasScalarIterUDFHandler,
        PandasScalarUDFHandler,
    )
    from pyspark.sql.conversion import ArrowBatchTransformer

_missing_message = pandas_requirement_message or pyarrow_requirement_message


def _batch(**columns):
    """A RecordBatch of int64 columns, one per ``name=values`` kwarg."""
    return pa.RecordBatch.from_arrays(
        [pa.array(values, type=pa.int64()) for values in columns.values()],
        list(columns),
    )


def _udf(func, return_type=None, args=(0,), kwargs=None):
    """A scalar pandas UDF tuple ``(func, args_offsets, kwargs_offsets, return_type)``."""
    return (func, list(args), dict(kwargs or {}), return_type or LongType())


def _handler(*udfs, runner_conf=None):
    """Build a PandasScalarUDFHandler from one or more ``_udf`` tuples."""
    return PandasScalarUDFHandler(
        udfs=list(udfs), runner_conf=runner_conf or RunnerConf({}), eval_conf=None
    )


def _scalar_iter_handler(udf, return_type=None, args=(0,), runner_conf=None):
    """Build a PandasScalarIterUDFHandler whose one UDF reads the given arg offsets."""
    return PandasScalarIterUDFHandler(
        udfs=[(udf, list(args), {}, return_type or LongType())],
        runner_conf=runner_conf or RunnerConf({}),
        eval_conf=None,
    )


def _map_handler(udf, return_type, runner_conf=None):
    """Build a PandasMapUDFHandler from its ``(func, None, None, return_type)`` UDF tuple."""
    return PandasMapUDFHandler(
        udfs=[(udf, None, None, return_type)],
        runner_conf=runner_conf or RunnerConf({}),
        eval_conf=None,
    )


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasEvalTypeHandlerRegistrationTests(unittest.TestCase):
    def test_pandas_eval_types_are_registered(self):
        # Each migrated pandas eval type dispatches to its handler by lookup.
        for eval_type, handler_cls in (
            (PythonEvalType.SQL_SCALAR_PANDAS_UDF, PandasScalarUDFHandler),
            (PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF, PandasScalarIterUDFHandler),
            (PythonEvalType.SQL_MAP_PANDAS_ITER_UDF, PandasMapUDFHandler),
        ):
            self.assertIs(get_eval_type_handler(eval_type), handler_cls)


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasScalarUDFHandlerTests(unittest.TestCase):
    def test_invokes_udf_per_batch(self):
        handler = _handler(_udf(lambda s: s + 1))
        out = list(handler.run(0, iter([_batch(a=[1, 2, 3])])))
        self.assertEqual([b.column(0).to_pylist() for b in out], [[2, 3, 4]])

    def test_coerces_output_to_return_type(self):
        # The UDF returns int32, but the declared return type is LongType (int64).
        handler = _handler(_udf(lambda s: (s + 1).astype("int32")))
        out = list(handler.run(0, iter([_batch(a=[10, 20])])))
        self.assertEqual(out[0].schema.field(0).type, pa.int64())
        self.assertEqual(out[0].column(0).to_pylist(), [11, 21])

    def test_multiple_udfs_produce_one_column_each(self):
        handler = _handler(
            _udf(lambda s: s + 1, args=(0,)),
            _udf(lambda s: s * 2, args=(1,)),
        )
        out = list(handler.run(0, iter([_batch(a=[1, 2], b=[10, 20])])))
        self.assertEqual(out[0].column(0).to_pylist(), [2, 3])
        self.assertEqual(out[0].column(1).to_pylist(), [20, 40])

    def test_passes_arg_by_keyword_offset(self):
        # Validates the inline args/kwargs offset handling: y is bound by keyword.
        handler = _handler(_udf(lambda x, y: x - y, args=(0,), kwargs={"y": 1}))
        out = list(handler.run(0, iter([_batch(a=[10, 20], b=[3, 5])])))
        self.assertEqual(out[0].column(0).to_pylist(), [7, 15])

    def test_struct_return_type_takes_dataframe(self):
        # struct_in_pandas="dict" + df_for_struct=True: a struct return is a DataFrame.
        struct_type = StructType([StructField("x", LongType()), StructField("y", LongType())])
        handler = _handler(
            _udf(lambda s: pd.DataFrame({"x": s, "y": s * 10}), return_type=struct_type)
        )
        out = list(handler.run(0, iter([_batch(a=[1, 2])])))
        self.assertEqual(
            out[0].column(0).to_pylist(),
            [{"x": 1, "y": 10}, {"x": 2, "y": 20}],
        )

    def test_rejects_non_sized_result(self):
        handler = _handler(_udf(lambda s: 42))
        with self.assertRaises(PySparkTypeError):
            list(handler.run(0, iter([_batch(a=[1, 2])])))

    def test_rejects_row_count_mismatch(self):
        handler = _handler(_udf(lambda s: s.head(1)))
        with self.assertRaises(PySparkRuntimeError):
            list(handler.run(0, iter([_batch(a=[1, 2, 3])])))

    def test_requires_dataframe_for_struct_return(self):
        struct_type = StructType([StructField("x", LongType())])
        handler = _handler(_udf(lambda s: s, return_type=struct_type))
        with self.assertRaises(PySparkValueError):
            list(handler.run(0, iter([_batch(a=[1, 2])])))

    def test_forwards_prefer_int_ext_dtype_to_input_conversion(self):
        # preferIntExtensionDtype flows to the Arrow->pandas conversion: the UDF sees
        # a nullable "Int64" input instead of the default "int64".
        dtype_name = _udf(lambda s: pd.Series([str(s.dtype)] * len(s)), return_type=StringType())
        conf = RunnerConf({"spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype": "true"})
        out = list(_handler(dtype_name, runner_conf=conf).run(0, iter([_batch(a=[1, 2])])))
        self.assertEqual(out[0].column(0).to_pylist(), ["Int64", "Int64"])

    def test_forwards_use_large_var_types_to_output_conversion(self):
        # useLargeVarTypes flows to the pandas->Arrow conversion: a string result
        # becomes large_string rather than string.
        to_str = _udf(lambda s: s.astype(str), return_type=StringType())
        conf = RunnerConf({"spark.sql.execution.arrow.useLargeVarTypes": "true"})
        out = list(_handler(to_str, runner_conf=conf).run(0, iter([_batch(a=[1, 2])])))
        self.assertTrue(pa.types.is_large_string(out[0].schema.field(0).type))


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasScalarIterUDFHandlerTests(unittest.TestCase):
    def test_invokes_udf_over_batch_stream(self):
        def add_one(series_iter):
            for s in series_iter:
                yield s + 1

        handler = _scalar_iter_handler(add_one)
        out = list(handler.run(0, iter([_batch(a=[1, 2]), _batch(a=[3])])))
        self.assertEqual([b.column(0).to_pylist() for b in out], [[2, 3], [4]])

    def test_coerces_output_to_return_type(self):
        # The UDF yields int32, but the declared return type is LongType (int64).
        def add_one(series_iter):
            for s in series_iter:
                yield (s + 1).astype("int32")

        handler = _scalar_iter_handler(add_one)
        out = list(handler.run(0, iter([_batch(a=[10, 20])])))
        self.assertEqual(out[0].schema.field(0).type, pa.int64())
        self.assertEqual(out[0].column(0).to_pylist(), [11, 21])

    def test_rejects_too_many_rows(self):
        # Emitting more rows than consumed fails fast via the output row-limit guard.
        def too_many(series_iter):
            for s in series_iter:
                yield pd.Series(list(range(len(s) + 1)))

        handler = _scalar_iter_handler(too_many)
        with self.assertRaises(PySparkRuntimeError) as cm:
            list(handler.run(0, iter([_batch(a=[1, 2])])))
        self.assertEqual(cm.exception.getCondition(), "OUTPUT_EXCEEDS_INPUT_ROWS")

    def test_rejects_too_few_rows(self):
        # Emitting fewer rows than consumed fails the final row-count equality guard.
        def too_few(series_iter):
            for s in series_iter:
                yield s.head(len(s) - 1)

        handler = _scalar_iter_handler(too_few)
        with self.assertRaises(PySparkRuntimeError) as cm:
            list(handler.run(0, iter([_batch(a=[1, 2, 3])])))
        self.assertEqual(cm.exception.getCondition(), "RESULT_ROWS_MISMATCH")

    def test_rejects_unconsumed_input(self):
        # A UDF that stops reading before the input stream is exhausted must raise
        # INPUT_NOT_FULLY_CONSUMED.
        def first_only(series_iter):
            yield next(series_iter)

        handler = _scalar_iter_handler(first_only)
        with self.assertRaises(PySparkRuntimeError) as cm:
            list(handler.run(0, iter([_batch(a=[1, 2]), _batch(a=[3, 4])])))
        self.assertEqual(cm.exception.getCondition(), "INPUT_NOT_FULLY_CONSUMED")

    def test_struct_return_takes_dataframe(self):
        # struct_in_pandas="dict" + df_for_struct=True: a struct return is a DataFrame.
        struct_type = StructType([StructField("x", LongType()), StructField("y", LongType())])

        def to_struct(series_iter):
            for s in series_iter:
                yield pd.DataFrame({"x": s, "y": s * 10})

        handler = _scalar_iter_handler(to_struct, return_type=struct_type)
        out = list(handler.run(0, iter([_batch(a=[1, 2])])))
        self.assertEqual(
            out[0].column(0).to_pylist(),
            [{"x": 1, "y": 10}, {"x": 2, "y": 20}],
        )


@unittest.skipIf(not (have_pandas and have_pyarrow), _missing_message)
class PandasMapUDFHandlerTests(unittest.TestCase):
    _return_type = StructType([StructField("v", LongType())])

    def test_maps_batch_stream(self):
        # mapInPandas expands the wire struct into a DataFrame and re-wraps the output.
        def double_v(df_iter):
            for df in df_iter:
                yield pd.DataFrame({"v": df["v"] * 2})

        handler = _map_handler(double_v, self._return_type)
        # mapInPandas sends a single struct column per batch; the handler expands it to
        # a DataFrame, so build the input in that wire format here.
        struct_batch = ArrowBatchTransformer.wrap_struct(_batch(v=[1, 2, 3]))
        out = list(handler.run(0, iter([struct_batch])))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [2, 4, 6])

    def test_legacy_accepts_any_iterable(self):
        # With the legacy flag on (its default), a UDF may return any iterable -- here a
        # plain list of DataFrames -- which the handler adapts via iter() before verifying.
        def as_list(df_iter):
            return [pd.DataFrame({"v": df["v"] * 2}) for df in df_iter]

        handler = _map_handler(as_list, self._return_type)
        struct_batch = ArrowBatchTransformer.wrap_struct(_batch(v=[1, 2, 3]))
        out = list(handler.run(0, iter([struct_batch])))
        self.assertEqual(out[0].column(0).field("v").to_pylist(), [2, 4, 6])

    def test_rejects_non_iterator_result(self):
        # With the legacy accept-any-iterable flag off, a UDF returning a DataFrame (not
        # an iterator of them) is rejected as a non-iterator before the input stream is
        # read, so the input batch shape is irrelevant.
        strict = RunnerConf(
            {"spark.sql.execution.pythonUDF.mapInBatch.legacy.acceptAnyIterable.enabled": "false"}
        )
        handler = _map_handler(
            lambda df_iter: pd.DataFrame({"v": [1]}), self._return_type, runner_conf=strict
        )
        with self.assertRaises(PySparkTypeError) as cm:
            list(handler.run(0, iter([_batch(v=[1, 2])])))
        # The non-iterator branch reports the actual type ("DataFrame"); the per-element
        # branch (test_rejects_wrong_element_type) reports "iterator of ...".
        self.assertEqual(cm.exception.getMessageParameters()["actual"], "DataFrame")

    def test_rejects_wrong_element_type(self):
        # Each yielded element must be a DataFrame for a struct return type.
        def yields_series(df_iter):
            for _ in df_iter:
                yield pd.Series([1, 2])  # a Series, not a DataFrame

        handler = _map_handler(yields_series, self._return_type)
        with self.assertRaises(PySparkTypeError):
            list(handler.run(0, iter([_batch(v=[1, 2])])))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
