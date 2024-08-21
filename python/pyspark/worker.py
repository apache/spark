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

"""
Worker that receives input from Piped RDD.
"""
import os
import sys
import dataclasses
import time
import inspect
import json
from typing import Any, Callable, Iterable, Iterator, Optional
import faulthandler

from pyspark.accumulators import (
    SpecialAccumulatorIds,
    _accumulatorRegistry,
    _deserialize_accumulator,
)
from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.taskcontext import BarrierTaskContext, TaskContext
from pyspark.resource import ResourceInformation
from pyspark.util import PythonEvalType, local_connect_and_auth
from pyspark.serializers import (
    write_int,
    read_long,
    read_bool,
    write_long,
    read_int,
    SpecialLengths,
    UTF8Deserializer,
    CPickleSerializer,
    BatchedSerializer,
)
from pyspark.sql.functions import SkipRestOfInputTableException
from pyspark.sql.pandas.serializers import (
    ArrowStreamPandasUDFSerializer,
    ArrowStreamPandasUDTFSerializer,
    CogroupArrowUDFSerializer,
    CogroupPandasUDFSerializer,
    ArrowStreamUDFSerializer,
    ArrowStreamGroupUDFSerializer,
    ApplyInPandasWithStateSerializer,
    TransformWithStateInPandasSerializer,
)
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DataType,
    MapType,
    Row,
    StringType,
    StructType,
    _create_row,
    _parse_datatype_json_string,
)
from pyspark.util import fail_on_stopiteration, handle_worker_exception
from pyspark import shuffle
from pyspark.errors import PySparkRuntimeError, PySparkTypeError
from pyspark.worker_util import (
    check_python_version,
    read_command,
    pickleSer,
    send_accumulator_updates,
    setup_broadcasts,
    setup_memory_limits,
    setup_spark_files,
    utf8_deserializer,
)

try:
    import memory_profiler  # noqa: F401

    has_memory_profiler = True
except Exception:
    has_memory_profiler = False


def report_times(outfile, boot, init, finish):
    write_int(SpecialLengths.TIMING_DATA, outfile)
    write_long(int(1000 * boot), outfile)
    write_long(int(1000 * init), outfile)
    write_long(int(1000 * finish), outfile)


def chain(f, g):
    """chain two functions together"""
    return lambda *a: g(f(*a))


def wrap_udf(f, args_offsets, kwargs_offsets, return_type):
    func, args_kwargs_offsets = wrap_kwargs_support(f, args_offsets, kwargs_offsets)

    if return_type.needConversion():
        toInternal = return_type.toInternal
        return args_kwargs_offsets, lambda *a: toInternal(func(*a))
    else:
        return args_kwargs_offsets, lambda *a: func(*a)


def wrap_scalar_pandas_udf(f, args_offsets, kwargs_offsets, return_type):
    func, args_kwargs_offsets = wrap_kwargs_support(f, args_offsets, kwargs_offsets)

    arrow_return_type = to_arrow_type(return_type)

    def verify_result_type(result):
        if not hasattr(result, "__len__"):
            pd_type = "pandas.DataFrame" if type(return_type) == StructType else "pandas.Series"
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={
                    "expected": pd_type,
                    "actual": type(result).__name__,
                },
            )
        return result

    def verify_result_length(result, length):
        if len(result) != length:
            raise PySparkRuntimeError(
                errorClass="SCHEMA_MISMATCH_FOR_PANDAS_UDF",
                messageParameters={
                    "expected": str(length),
                    "actual": str(len(result)),
                },
            )
        return result

    return (
        args_kwargs_offsets,
        lambda *a: (
            verify_result_length(verify_result_type(func(*a)), len(a[0])),
            arrow_return_type,
        ),
    )


def wrap_arrow_batch_udf(f, args_offsets, kwargs_offsets, return_type):
    import pandas as pd

    func, args_kwargs_offsets = wrap_kwargs_support(f, args_offsets, kwargs_offsets)

    arrow_return_type = to_arrow_type(return_type)

    # "result_func" ensures the result of a Python UDF to be consistent with/without Arrow
    # optimization.
    # Otherwise, an Arrow-optimized Python UDF raises "pyarrow.lib.ArrowTypeError: Expected a
    # string or bytes dtype, got ..." whereas a non-Arrow-optimized Python UDF returns
    # successfully.
    result_func = lambda pdf: pdf  # noqa: E731
    if type(return_type) == StringType:
        result_func = lambda r: str(r) if r is not None else r  # noqa: E731
    elif type(return_type) == BinaryType:
        result_func = lambda r: bytes(r) if r is not None else r  # noqa: E731

    @fail_on_stopiteration
    def evaluate(*args: pd.Series) -> pd.Series:
        return pd.Series([result_func(func(*row)) for row in zip(*args)])

    def verify_result_length(result, length):
        if len(result) != length:
            raise PySparkRuntimeError(
                errorClass="SCHEMA_MISMATCH_FOR_PANDAS_UDF",
                messageParameters={
                    "expected": str(length),
                    "actual": str(len(result)),
                },
            )
        return result

    return (
        args_kwargs_offsets,
        lambda *a: (verify_result_length(evaluate(*a), len(a[0])), arrow_return_type),
    )


def wrap_pandas_batch_iter_udf(f, return_type):
    arrow_return_type = to_arrow_type(return_type)
    iter_type_label = "pandas.DataFrame" if type(return_type) == StructType else "pandas.Series"

    def verify_result(result):
        if not isinstance(result, Iterator) and not hasattr(result, "__iter__"):
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={
                    "expected": "iterator of {}".format(iter_type_label),
                    "actual": type(result).__name__,
                },
            )
        return result

    def verify_element(elem):
        import pandas as pd

        if not isinstance(elem, pd.DataFrame if type(return_type) == StructType else pd.Series):
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={
                    "expected": "iterator of {}".format(iter_type_label),
                    "actual": "iterator of {}".format(type(elem).__name__),
                },
            )

        verify_pandas_result(
            elem, return_type, assign_cols_by_name=True, truncate_return_schema=True
        )

        return elem

    return lambda *iterator: map(
        lambda res: (res, arrow_return_type), map(verify_element, verify_result(f(*iterator)))
    )


def verify_pandas_result(result, return_type, assign_cols_by_name, truncate_return_schema):
    import pandas as pd

    if type(return_type) == StructType:
        if not isinstance(result, pd.DataFrame):
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={
                    "expected": "pandas.DataFrame",
                    "actual": type(result).__name__,
                },
            )

        # check the schema of the result only if it is not empty or has columns
        if not result.empty or len(result.columns) != 0:
            # if any column name of the result is a string
            # the column names of the result have to match the return type
            #   see create_array in pyspark.sql.pandas.serializers.ArrowStreamPandasSerializer
            field_names = set([field.name for field in return_type.fields])
            # only the first len(field_names) result columns are considered
            # when truncating the return schema
            result_columns = (
                result.columns[: len(field_names)] if truncate_return_schema else result.columns
            )
            column_names = set(result_columns)
            if (
                assign_cols_by_name
                and any(isinstance(name, str) for name in result.columns)
                and column_names != field_names
            ):
                missing = sorted(list(field_names.difference(column_names)))
                missing = f" Missing: {', '.join(missing)}." if missing else ""

                extra = sorted(list(column_names.difference(field_names)))
                extra = f" Unexpected: {', '.join(extra)}." if extra else ""

                raise PySparkRuntimeError(
                    errorClass="RESULT_COLUMNS_MISMATCH_FOR_PANDAS_UDF",
                    messageParameters={
                        "missing": missing,
                        "extra": extra,
                    },
                )
            # otherwise the number of columns of result have to match the return type
            elif len(result_columns) != len(return_type):
                raise PySparkRuntimeError(
                    errorClass="RESULT_LENGTH_MISMATCH_FOR_PANDAS_UDF",
                    messageParameters={
                        "expected": str(len(return_type)),
                        "actual": str(len(result.columns)),
                    },
                )
    else:
        if not isinstance(result, pd.Series):
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={"expected": "pandas.Series", "actual": type(result).__name__},
            )


def wrap_arrow_batch_iter_udf(f, return_type):
    arrow_return_type = to_arrow_type(return_type)

    def verify_result(result):
        if not isinstance(result, Iterator) and not hasattr(result, "__iter__"):
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={
                    "expected": "iterator of pyarrow.RecordBatch",
                    "actual": type(result).__name__,
                },
            )
        return result

    def verify_element(elem):
        import pyarrow as pa

        if not isinstance(elem, pa.RecordBatch):
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={
                    "expected": "iterator of pyarrow.RecordBatch",
                    "actual": "iterator of {}".format(type(elem).__name__),
                },
            )

        return elem

    return lambda *iterator: map(
        lambda res: (res, arrow_return_type), map(verify_element, verify_result(f(*iterator)))
    )


def wrap_cogrouped_map_arrow_udf(f, return_type, argspec, runner_conf):
    _assign_cols_by_name = assign_cols_by_name(runner_conf)

    if _assign_cols_by_name:
        expected_cols_and_types = {
            col.name: to_arrow_type(col.dataType) for col in return_type.fields
        }
    else:
        expected_cols_and_types = [
            (col.name, to_arrow_type(col.dataType)) for col in return_type.fields
        ]

    def wrapped(left_key_table, left_value_table, right_key_table, right_value_table):
        if len(argspec.args) == 2:
            result = f(left_value_table, right_value_table)
        elif len(argspec.args) == 3:
            key_table = left_key_table if left_key_table.num_rows > 0 else right_key_table
            key = tuple(c[0] for c in key_table.columns)
            result = f(key, left_value_table, right_value_table)

        if isinstance(result, Iterator):
            def verify_element(batch):
                verify_arrow_batch(batch, _assign_cols_by_name, expected_cols_and_types)
                return batch

            return map(verify_element, result)
        else:
            verify_arrow_table(result, _assign_cols_by_name, expected_cols_and_types)
            return result.to_batches()

    return lambda kl, vl, kr, vr: (wrapped(kl, vl, kr, vr), to_arrow_type(return_type))


def wrap_cogrouped_map_pandas_udf(f, return_type, argspec, runner_conf):
    _assign_cols_by_name = assign_cols_by_name(runner_conf)

    def wrapped(left_key_series, left_value_series, right_key_series, right_value_series):
        import pandas as pd

        left_df = pd.concat(left_value_series, axis=1)
        right_df = pd.concat(right_value_series, axis=1)

        if len(argspec.args) == 2:
            result = f(left_df, right_df)
        elif len(argspec.args) == 3:
            key_series = left_key_series if not left_df.empty else right_key_series
            key = tuple(s[0] for s in key_series)
            result = f(key, left_df, right_df)
        verify_pandas_result(
            result, return_type, _assign_cols_by_name, truncate_return_schema=False
        )

        return result

    return lambda kl, vl, kr, vr: [(wrapped(kl, vl, kr, vr), to_arrow_type(return_type))]


def verify_arrow_result(result, assign_cols_by_name, expected_cols_and_types):
    # the types of the fields have to be identical to return type
    # an empty table can have no columns; if there are columns, they have to match
    if result.num_columns != 0 or result.num_rows != 0:
        # columns are either mapped by name or position
        if assign_cols_by_name:
            actual_cols_and_types = {
                name: dataType for name, dataType in zip(result.schema.names, result.schema.types)
            }
            missing = sorted(
                list(set(expected_cols_and_types.keys()).difference(actual_cols_and_types.keys()))
            )
            extra = sorted(
                list(set(actual_cols_and_types.keys()).difference(expected_cols_and_types.keys()))
            )

            if missing or extra:
                missing = f" Missing: {', '.join(missing)}." if missing else ""
                extra = f" Unexpected: {', '.join(extra)}." if extra else ""

                raise PySparkRuntimeError(
                    errorClass="RESULT_COLUMNS_MISMATCH_FOR_ARROW_UDF",
                    messageParameters={
                        "missing": missing,
                        "extra": extra,
                    },
                )

            column_types = [
                (name, expected_cols_and_types[name], actual_cols_and_types[name])
                for name in sorted(expected_cols_and_types.keys())
            ]
        else:
            actual_cols_and_types = [
                (name, dataType) for name, dataType in zip(result.schema.names, result.schema.types)
            ]
            column_types = [
                (expected_name, expected_type, actual_type)
                for (expected_name, expected_type), (actual_name, actual_type) in zip(
                    expected_cols_and_types, actual_cols_and_types
                )
            ]

        type_mismatch = [
            (name, expected, actual)
            for name, expected, actual in column_types
            if actual != expected
        ]

        if type_mismatch:
            raise PySparkRuntimeError(
                errorClass="RESULT_TYPE_MISMATCH_FOR_ARROW_UDF",
                messageParameters={
                    "mismatch": ", ".join(
                        "column '{}' (expected {}, actual {})".format(name, expected, actual)
                        for name, expected, actual in type_mismatch
                    )
                },
            )
        
def verify_arrow_table(table, assign_cols_by_name, expected_cols_and_types):
    import pyarrow as pa

    if not isinstance(table, pa.Table):
        raise PySparkTypeError(
            errorClass="UDF_RETURN_TYPE",
            messageParameters={
                "expected": "pyarrow.Table",
                "actual": type(table).__name__,
            },
        )
    
    verify_arrow_result(table, assign_cols_by_name, expected_cols_and_types)

def verify_arrow_batch(batch, assign_cols_by_name, expected_cols_and_types):
    import pyarrow as pa

    if not isinstance(batch, pa.RecordBatch):
        raise PySparkTypeError(
            errorClass="UDF_RETURN_TYPE",
            messageParameters={
                "expected": "pyarrow.RecordBatch",
                "actual": type(batch).__name__,
            },
        )
    
    verify_arrow_result(batch, assign_cols_by_name, expected_cols_and_types)


def wrap_grouped_map_arrow_udf(f, return_type, argspec, runner_conf):
    _assign_cols_by_name = assign_cols_by_name(runner_conf)

    if _assign_cols_by_name:
        expected_cols_and_types = {
            col.name: to_arrow_type(col.dataType) for col in return_type.fields
        }
    else:
        expected_cols_and_types = [
            (col.name, to_arrow_type(col.dataType)) for col in return_type.fields
        ]

    def wrapped(key_table, value_table):
        if len(argspec.args) == 1:
            result = f(value_table)
        elif len(argspec.args) == 2:
            key = tuple(c[0] for c in key_table.columns)
            result = f(key, value_table)

        if isinstance(result, Iterator):
            def verify_element(batch):
                verify_arrow_batch(batch, _assign_cols_by_name, expected_cols_and_types)
                return batch

            return map(verify_element, result)
        else:
            verify_arrow_table(result, _assign_cols_by_name, expected_cols_and_types)
            return result.to_batches()

    return lambda k, v: (wrapped(k, v), to_arrow_type(return_type))


def wrap_grouped_map_pandas_udf(f, return_type, argspec, runner_conf):
    _assign_cols_by_name = assign_cols_by_name(runner_conf)

    def wrapped(key_series, value_series):
        import pandas as pd

        if len(argspec.args) == 1:
            result = f(pd.concat(value_series, axis=1))
        elif len(argspec.args) == 2:
            key = tuple(s[0] for s in key_series)
            result = f(key, pd.concat(value_series, axis=1))
        verify_pandas_result(
            result, return_type, _assign_cols_by_name, truncate_return_schema=False
        )

        return result

    return lambda k, v: [(wrapped(k, v), to_arrow_type(return_type))]


def wrap_grouped_transform_with_state_pandas_udf(f, return_type, runner_conf):
    def wrapped(stateful_processor_api_client, key, value_series_gen):
        import pandas as pd

        values = (pd.concat(x, axis=1) for x in value_series_gen)
        result_iter = f(stateful_processor_api_client, key, values)

        # TODO(SPARK-49100): add verification that elements in result_iter are
        # indeed of type pd.DataFrame and confirm to assigned cols

        return result_iter

    return lambda p, k, v: [(wrapped(p, k, v), to_arrow_type(return_type))]


def wrap_grouped_map_pandas_udf_with_state(f, return_type):
    """
    Provides a new lambda instance wrapping user function of applyInPandasWithState.

    The lambda instance receives (key series, iterator of value series, state) and performs
    some conversion to be adapted with the signature of user function.

    See the function doc of inner function `wrapped` for more details on what adapter does.
    See the function doc of `mapper` function for
    `eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE` for more details on
    the input parameters of lambda function.

    Along with the returned iterator, the lambda instance will also produce the return_type as
    converted to the arrow schema.
    """

    def wrapped(key_series, value_series_gen, state):
        """
        Provide an adapter of the user function performing below:

        - Extract the first value of all columns in key series and produce as a tuple.
        - If the state has timed out, call the user function with empty pandas DataFrame.
        - If not, construct a new generator which converts each element of value series to
          pandas DataFrame (lazy evaluation), and call the user function with the generator
        - Verify each element of returned iterator to check the schema of pandas DataFrame.
        """
        import pandas as pd

        key = tuple(s[0] for s in key_series)

        if state.hasTimedOut:
            # Timeout processing pass empty iterator. Here we return an empty DataFrame instead.
            values = [
                pd.DataFrame(columns=pd.concat(next(value_series_gen), axis=1).columns),
            ]
        else:
            values = (pd.concat(x, axis=1) for x in value_series_gen)

        result_iter = f(key, values, state)

        def verify_element(result):
            if not isinstance(result, pd.DataFrame):
                raise PySparkTypeError(
                    errorClass="UDF_RETURN_TYPE",
                    messageParameters={
                        "expected": "iterator of pandas.DataFrame",
                        "actual": "iterator of {}".format(type(result).__name__),
                    },
                )
            # the number of columns of result have to match the return type
            # but it is fine for result to have no columns at all if it is empty
            if not (
                len(result.columns) == len(return_type)
                or (len(result.columns) == 0 and result.empty)
            ):
                raise PySparkRuntimeError(
                    errorClass="RESULT_LENGTH_MISMATCH_FOR_PANDAS_UDF",
                    messageParameters={
                        "expected": str(len(return_type)),
                        "actual": str(len(result.columns)),
                    },
                )

            return result

        if isinstance(result_iter, pd.DataFrame):
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={
                    "expected": "iterable of pandas.DataFrame",
                    "actual": type(result_iter).__name__,
                },
            )

        try:
            iter(result_iter)
        except TypeError:
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={"expected": "iterable", "actual": type(result_iter).__name__},
            )

        result_iter_with_validation = (verify_element(x) for x in result_iter)

        return (
            result_iter_with_validation,
            state,
        )

    return lambda k, v, s: [(wrapped(k, v, s), to_arrow_type(return_type))]


def wrap_grouped_agg_pandas_udf(f, args_offsets, kwargs_offsets, return_type):
    func, args_kwargs_offsets = wrap_kwargs_support(f, args_offsets, kwargs_offsets)

    arrow_return_type = to_arrow_type(return_type)

    def wrapped(*series):
        import pandas as pd

        result = func(*series)
        return pd.Series([result])

    return (
        args_kwargs_offsets,
        lambda *a: (wrapped(*a), arrow_return_type),
    )


def wrap_window_agg_pandas_udf(
    f, args_offsets, kwargs_offsets, return_type, runner_conf, udf_index
):
    window_bound_types_str = runner_conf.get("pandas_window_bound_types")
    window_bound_type = [t.strip().lower() for t in window_bound_types_str.split(",")][udf_index]
    if window_bound_type == "bounded":
        return wrap_bounded_window_agg_pandas_udf(f, args_offsets, kwargs_offsets, return_type)
    elif window_bound_type == "unbounded":
        return wrap_unbounded_window_agg_pandas_udf(f, args_offsets, kwargs_offsets, return_type)
    else:
        raise PySparkRuntimeError(
            errorClass="INVALID_WINDOW_BOUND_TYPE",
            messageParameters={
                "window_bound_type": window_bound_type,
            },
        )


def wrap_unbounded_window_agg_pandas_udf(f, args_offsets, kwargs_offsets, return_type):
    func, args_kwargs_offsets = wrap_kwargs_support(f, args_offsets, kwargs_offsets)

    # This is similar to grouped_agg_pandas_udf, the only difference
    # is that window_agg_pandas_udf needs to repeat the return value
    # to match window length, where grouped_agg_pandas_udf just returns
    # the scalar value.
    arrow_return_type = to_arrow_type(return_type)

    def wrapped(*series):
        import pandas as pd

        result = func(*series)
        return pd.Series([result]).repeat(len(series[0]))

    return (
        args_kwargs_offsets,
        lambda *a: (wrapped(*a), arrow_return_type),
    )


def wrap_bounded_window_agg_pandas_udf(f, args_offsets, kwargs_offsets, return_type):
    # args_offsets should have at least 2 for begin_index, end_index.
    assert len(args_offsets) >= 2, len(args_offsets)
    func, args_kwargs_offsets = wrap_kwargs_support(f, args_offsets[2:], kwargs_offsets)

    arrow_return_type = to_arrow_type(return_type)

    def wrapped(begin_index, end_index, *series):
        import pandas as pd

        result = []

        # Index operation is faster on np.ndarray,
        # So we turn the index series into np array
        # here for performance
        begin_array = begin_index.values
        end_array = end_index.values

        for i in range(len(begin_array)):
            # Note: Create a slice from a series for each window is
            #       actually pretty expensive. However, there
            #       is no easy way to reduce cost here.
            # Note: s.iloc[i : j] is about 30% faster than s[i: j], with
            #       the caveat that the created slices shares the same
            #       memory with s. Therefore, user are not allowed to
            #       change the value of input series inside the window
            #       function. It is rare that user needs to modify the
            #       input series in the window function, and therefore,
            #       it is be a reasonable restriction.
            # Note: Calling reset_index on the slices will increase the cost
            #       of creating slices by about 100%. Therefore, for performance
            #       reasons we don't do it here.
            series_slices = [s.iloc[begin_array[i] : end_array[i]] for s in series]
            result.append(func(*series_slices))
        return pd.Series(result)

    return (
        args_offsets[:2] + args_kwargs_offsets,
        lambda *a: (wrapped(*a), arrow_return_type),
    )


def wrap_kwargs_support(f, args_offsets, kwargs_offsets):
    if len(kwargs_offsets):
        keys = list(kwargs_offsets.keys())

        len_args_offsets = len(args_offsets)
        if len_args_offsets > 0:

            def func(*args):
                return f(*args[:len_args_offsets], **dict(zip(keys, args[len_args_offsets:])))

        else:

            def func(*args):
                return f(**dict(zip(keys, args)))

        return func, args_offsets + [kwargs_offsets[key] for key in keys]
    else:
        return f, args_offsets


def _supports_profiler(eval_type: int) -> bool:
    return eval_type not in (
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
        PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
        PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
    )


def wrap_perf_profiler(f, result_id):
    import cProfile
    import pstats

    from pyspark.sql.profiler import ProfileResultsParam

    accumulator = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
    )

    def profiling_func(*args, **kwargs):
        with cProfile.Profile() as pr:
            ret = f(*args, **kwargs)
        st = pstats.Stats(pr)
        st.stream = None  # make it picklable
        st.strip_dirs()

        accumulator.add({result_id: (st, None)})

        return ret

    return profiling_func


def wrap_memory_profiler(f, result_id):
    from pyspark.sql.profiler import ProfileResultsParam
    from pyspark.profiler import UDFLineProfilerV2

    accumulator = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
    )

    def profiling_func(*args, **kwargs):
        profiler = UDFLineProfilerV2()

        wrapped = profiler(f)
        ret = wrapped(*args, **kwargs)
        codemap_dict = {
            filename: list(line_iterator) for filename, line_iterator in profiler.code_map.items()
        }
        accumulator.add({result_id: (None, codemap_dict)})
        return ret

    return profiling_func


def read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index, profiler):
    num_arg = read_int(infile)

    if eval_type in (
        PythonEvalType.SQL_BATCHED_UDF,
        PythonEvalType.SQL_ARROW_BATCHED_UDF,
        PythonEvalType.SQL_SCALAR_PANDAS_UDF,
        PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
        PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
        # The below doesn't support named argument, but shares the same protocol.
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
    ):
        args_offsets = []
        kwargs_offsets = {}
        for _ in range(num_arg):
            offset = read_int(infile)
            if read_bool(infile):
                name = utf8_deserializer.loads(infile)
                kwargs_offsets[name] = offset
            else:
                args_offsets.append(offset)
    else:
        args_offsets = [read_int(infile) for i in range(num_arg)]
        kwargs_offsets = {}

    chained_func = None
    for i in range(read_int(infile)):
        f, return_type = read_command(pickleSer, infile)
        if chained_func is None:
            chained_func = f
        else:
            chained_func = chain(chained_func, f)

    if profiler == "perf":
        result_id = read_long(infile)

        if _supports_profiler(eval_type):
            profiling_func = wrap_perf_profiler(chained_func, result_id)
        else:
            profiling_func = chained_func

    elif profiler == "memory":
        result_id = read_long(infile)
        if _supports_profiler(eval_type) and has_memory_profiler:
            profiling_func = wrap_memory_profiler(chained_func, result_id)
        else:
            profiling_func = chained_func
    else:
        profiling_func = chained_func

    if eval_type in (
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
        PythonEvalType.SQL_ARROW_BATCHED_UDF,
    ):
        func = profiling_func
    else:
        # make sure StopIteration's raised in the user code are not ignored
        # when they are processed in a for loop, raise them as RuntimeError's instead
        func = fail_on_stopiteration(profiling_func)

    # the last returnType will be the return type of UDF
    if eval_type == PythonEvalType.SQL_SCALAR_PANDAS_UDF:
        return wrap_scalar_pandas_udf(func, args_offsets, kwargs_offsets, return_type)
    elif eval_type == PythonEvalType.SQL_ARROW_BATCHED_UDF:
        return wrap_arrow_batch_udf(func, args_offsets, kwargs_offsets, return_type)
    elif eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF:
        return args_offsets, wrap_pandas_batch_iter_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF:
        return args_offsets, wrap_pandas_batch_iter_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_MAP_ARROW_ITER_UDF:
        return args_offsets, wrap_arrow_batch_iter_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF:
        argspec = inspect.getfullargspec(chained_func)  # signature was lost when wrapping it
        return args_offsets, wrap_grouped_map_pandas_udf(func, return_type, argspec, runner_conf)
    elif eval_type == PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF:
        argspec = inspect.getfullargspec(chained_func)  # signature was lost when wrapping it
        return args_offsets, wrap_grouped_map_arrow_udf(func, return_type, argspec, runner_conf)
    elif eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE:
        return args_offsets, wrap_grouped_map_pandas_udf_with_state(func, return_type)
    elif eval_type == PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF:
        return args_offsets, wrap_grouped_transform_with_state_pandas_udf(
            func, return_type, runner_conf
        )
    elif eval_type == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
        argspec = inspect.getfullargspec(chained_func)  # signature was lost when wrapping it
        return args_offsets, wrap_cogrouped_map_pandas_udf(func, return_type, argspec, runner_conf)
    elif eval_type == PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF:
        argspec = inspect.getfullargspec(chained_func)  # signature was lost when wrapping it
        return args_offsets, wrap_cogrouped_map_arrow_udf(func, return_type, argspec, runner_conf)
    elif eval_type == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF:
        return wrap_grouped_agg_pandas_udf(func, args_offsets, kwargs_offsets, return_type)
    elif eval_type == PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF:
        return wrap_window_agg_pandas_udf(
            func, args_offsets, kwargs_offsets, return_type, runner_conf, udf_index
        )
    elif eval_type == PythonEvalType.SQL_BATCHED_UDF:
        return wrap_udf(func, args_offsets, kwargs_offsets, return_type)
    else:
        raise ValueError("Unknown eval type: {}".format(eval_type))


# Used by SQL_GROUPED_MAP_PANDAS_UDF, SQL_GROUPED_MAP_ARROW_UDF,
# SQL_COGROUPED_MAP_PANDAS_UDF, SQL_COGROUPED_MAP_ARROW_UDF,
# SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
# SQL_SCALAR_PANDAS_UDF and SQL_ARROW_BATCHED_UDF when
# returning StructType
def assign_cols_by_name(runner_conf):
    return (
        runner_conf.get(
            "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName", "true"
        ).lower()
        == "true"
    )


# Read and process a serialized user-defined table function (UDTF) from a socket.
# It expects the UDTF to be in a specific format and performs various checks to
# ensure the UDTF is valid. This function also prepares a mapper function for applying
# the UDTF logic to input rows.
def read_udtf(pickleSer, infile, eval_type):
    if eval_type == PythonEvalType.SQL_ARROW_TABLE_UDF:
        runner_conf = {}
        # Load conf used for arrow evaluation.
        num_conf = read_int(infile)
        for i in range(num_conf):
            k = utf8_deserializer.loads(infile)
            v = utf8_deserializer.loads(infile)
            runner_conf[k] = v

        # NOTE: if timezone is set here, that implies respectSessionTimeZone is True
        timezone = runner_conf.get("spark.sql.session.timeZone", None)
        safecheck = (
            runner_conf.get("spark.sql.execution.pandas.convertToArrowArraySafely", "false").lower()
            == "true"
        )
        ser = ArrowStreamPandasUDTFSerializer(timezone, safecheck)
    else:
        # Each row is a group so do not batch but send one by one.
        ser = BatchedSerializer(CPickleSerializer(), 1)

    # See 'PythonUDTFRunner.PythonUDFWriterThread.writeCommand'
    num_arg = read_int(infile)
    args_offsets = []
    kwargs_offsets = {}
    for _ in range(num_arg):
        offset = read_int(infile)
        if read_bool(infile):
            name = utf8_deserializer.loads(infile)
            kwargs_offsets[name] = offset
        else:
            args_offsets.append(offset)
    num_partition_child_indexes = read_int(infile)
    partition_child_indexes = [read_int(infile) for i in range(num_partition_child_indexes)]
    has_pickled_analyze_result = read_bool(infile)
    if has_pickled_analyze_result:
        pickled_analyze_result = pickleSer._read_with_length(infile)
    else:
        pickled_analyze_result = None
    # Initially we assume that the UDTF __init__ method accepts the pickled AnalyzeResult,
    # although we may set this to false later if we find otherwise.
    handler = read_command(pickleSer, infile)
    if not isinstance(handler, type):
        raise PySparkRuntimeError(
            f"Invalid UDTF handler type. Expected a class (type 'type'), but "
            f"got an instance of {type(handler).__name__}."
        )

    return_type = _parse_datatype_json_string(utf8_deserializer.loads(infile))
    if not isinstance(return_type, StructType):
        raise PySparkRuntimeError(
            f"The return type of a UDTF must be a struct type, but got {type(return_type)}."
        )
    udtf_name = utf8_deserializer.loads(infile)

    # Update the handler that creates a new UDTF instance to first try calling the UDTF constructor
    # with one argument containing the previous AnalyzeResult. If that fails, then try a constructor
    # with no arguments. In this way each UDTF class instance can decide if it wants to inspect the
    # AnalyzeResult.
    udtf_init_args = inspect.getfullargspec(handler)
    if has_pickled_analyze_result:
        if len(udtf_init_args.args) > 2:
            raise PySparkRuntimeError(
                errorClass="UDTF_CONSTRUCTOR_INVALID_IMPLEMENTS_ANALYZE_METHOD",
                messageParameters={"name": udtf_name},
            )
        elif len(udtf_init_args.args) == 2:
            prev_handler = handler

            def construct_udtf():
                # Here we pass the AnalyzeResult to the UDTF's __init__ method.
                return prev_handler(dataclasses.replace(pickled_analyze_result))

            handler = construct_udtf
    elif len(udtf_init_args.args) > 1:
        raise PySparkRuntimeError(
            errorClass="UDTF_CONSTRUCTOR_INVALID_NO_ANALYZE_METHOD",
            messageParameters={"name": udtf_name},
        )

    class UDTFWithPartitions:
        """
        This implements the logic of a UDTF that accepts an input TABLE argument with one or more
        PARTITION BY expressions.

        For example, let's assume we have a table like:
            CREATE TABLE t (c1 INT, c2 INT) USING delta;
        Then for the following queries:
            SELECT * FROM my_udtf(TABLE (t) PARTITION BY c1, c2);
            The partition_child_indexes will be: 0, 1.
            SELECT * FROM my_udtf(TABLE (t) PARTITION BY c1, c2 + 4);
            The partition_child_indexes will be: 0, 2 (where we add a projection for "c2 + 4").
        """

        def __init__(self, create_udtf: Callable, partition_child_indexes: list):
            """
            Creates a new instance of this class to wrap the provided UDTF with another one that
            checks the values of projected partitioning expressions on consecutive rows to figure
            out when the partition boundaries change.

            Parameters
            ----------
            create_udtf: function
                Function to create a new instance of the UDTF to be invoked.
            partition_child_indexes: list
                List of integers identifying zero-based indexes of the columns of the input table
                that contain projected partitioning expressions. This class will inspect these
                values for each pair of consecutive input rows. When they change, this indicates
                the boundary between two partitions, and we will invoke the 'terminate' method on
                the UDTF class instance and then destroy it and create a new one to implement the
                desired partitioning semantics.
            """
            self._create_udtf: Callable = create_udtf
            self._udtf = create_udtf()
            self._prev_arguments: list = list()
            self._partition_child_indexes: list = partition_child_indexes
            self._eval_raised_skip_rest_of_input_table: bool = False

        def eval(self, *args, **kwargs) -> Iterator:
            changed_partitions = self._check_partition_boundaries(
                list(args) + list(kwargs.values())
            )
            if changed_partitions:
                if self._udtf.terminate is not None:
                    result = self._udtf.terminate()
                    if result is not None:
                        for row in result:
                            yield row
                self._udtf = self._create_udtf()
                self._eval_raised_skip_rest_of_input_table = False
            if self._udtf.eval is not None and not self._eval_raised_skip_rest_of_input_table:
                # Filter the arguments to exclude projected PARTITION BY values added by Catalyst.
                filtered_args = [self._remove_partition_by_exprs(arg) for arg in args]
                filtered_kwargs = {
                    key: self._remove_partition_by_exprs(value) for (key, value) in kwargs.items()
                }
                try:
                    result = self._udtf.eval(*filtered_args, **filtered_kwargs)
                    if result is not None:
                        for row in result:
                            yield row
                except SkipRestOfInputTableException:
                    # If the 'eval' method raised this exception, then we should skip the rest of
                    # the rows in the current partition. Set this field to True here and then for
                    # each subsequent row in the partition, we will skip calling the 'eval' method
                    # until we see a change in the partition boundaries.
                    self._eval_raised_skip_rest_of_input_table = True

        def terminate(self) -> Iterator:
            if self._udtf.terminate is not None:
                return self._udtf.terminate()
            return iter(())

        def cleanup(self) -> None:
            if hasattr(self._udtf, "cleanup"):
                self._udtf.cleanup()

        def _check_partition_boundaries(self, arguments: list) -> bool:
            result = False
            if len(self._prev_arguments) > 0:
                cur_table_arg = self._get_table_arg(arguments)
                prev_table_arg = self._get_table_arg(self._prev_arguments)
                cur_partitions_args = []
                prev_partitions_args = []
                for i in self._partition_child_indexes:
                    cur_partitions_args.append(cur_table_arg[i])
                    prev_partitions_args.append(prev_table_arg[i])
                result = any(k != v for k, v in zip(cur_partitions_args, prev_partitions_args))
            self._prev_arguments = arguments
            return result

        def _get_table_arg(self, inputs: list) -> Row:
            return [x for x in inputs if type(x) is Row][0]

        def _remove_partition_by_exprs(self, arg: Any) -> Any:
            if isinstance(arg, Row):
                new_row_keys = []
                new_row_values = []
                for i, (key, value) in enumerate(zip(arg.__fields__, arg)):
                    if i not in self._partition_child_indexes:
                        new_row_keys.append(key)
                        new_row_values.append(value)
                return _create_row(new_row_keys, new_row_values)
            else:
                return arg

    # Instantiate the UDTF class.
    try:
        if len(partition_child_indexes) > 0:
            udtf = UDTFWithPartitions(handler, partition_child_indexes)
        else:
            udtf = handler()
    except Exception as e:
        raise PySparkRuntimeError(
            errorClass="UDTF_EXEC_ERROR",
            messageParameters={"method_name": "__init__", "error": str(e)},
        )

    # Validate the UDTF
    if not hasattr(udtf, "eval"):
        raise PySparkRuntimeError(
            "Failed to execute the user defined table function because it has not "
            "implemented the 'eval' method. Please add the 'eval' method and try "
            "the query again."
        )

    # Check that the arguments provided to the UDTF call match the expected parameters defined
    # in the 'eval' method signature.
    try:
        inspect.signature(udtf.eval).bind(*args_offsets, **kwargs_offsets)
    except TypeError as e:
        raise PySparkRuntimeError(
            errorClass="UDTF_EVAL_METHOD_ARGUMENTS_DO_NOT_MATCH_SIGNATURE",
            messageParameters={"name": udtf_name, "reason": str(e)},
        ) from None

    def build_null_checker(return_type: StructType) -> Optional[Callable[[Any], None]]:
        def raise_(result_column_index):
            raise PySparkRuntimeError(
                errorClass="UDTF_EXEC_ERROR",
                messageParameters={
                    "method_name": "eval' or 'terminate",
                    "error": f"Column {result_column_index} within a returned row had a "
                    + "value of None, either directly or within array/struct/map "
                    + "subfields, but the corresponding column type was declared as "
                    + "non-nullable; please update the UDTF to return a non-None value at "
                    + "this location or otherwise declare the column type as nullable.",
                },
            )

        def checker(data_type: DataType, result_column_index: int):
            if isinstance(data_type, ArrayType):
                element_checker = checker(data_type.elementType, result_column_index)
                contains_null = data_type.containsNull

                if element_checker is None and contains_null:
                    return None

                def check_array(arr):
                    if isinstance(arr, list):
                        for e in arr:
                            if e is None:
                                if not contains_null:
                                    raise_(result_column_index)
                            elif element_checker is not None:
                                element_checker(e)

                return check_array

            elif isinstance(data_type, MapType):
                key_checker = checker(data_type.keyType, result_column_index)
                value_checker = checker(data_type.valueType, result_column_index)
                value_contains_null = data_type.valueContainsNull

                if value_checker is None and value_contains_null:

                    def check_map(map):
                        if isinstance(map, dict):
                            for k, v in map.items():
                                if k is None:
                                    raise_(result_column_index)
                                elif key_checker is not None:
                                    key_checker(k)

                else:

                    def check_map(map):
                        if isinstance(map, dict):
                            for k, v in map.items():
                                if k is None:
                                    raise_(result_column_index)
                                elif key_checker is not None:
                                    key_checker(k)
                                if v is None:
                                    if not value_contains_null:
                                        raise_(result_column_index)
                                elif value_checker is not None:
                                    value_checker(v)

                return check_map

            elif isinstance(data_type, StructType):
                field_checkers = [checker(f.dataType, result_column_index) for f in data_type]
                nullables = [f.nullable for f in data_type]

                if all(c is None for c in field_checkers) and all(nullables):
                    return None

                def check_struct(struct):
                    if isinstance(struct, tuple):
                        for value, checker, nullable in zip(struct, field_checkers, nullables):
                            if value is None:
                                if not nullable:
                                    raise_(result_column_index)
                            elif checker is not None:
                                checker(value)

                return check_struct

            else:
                return None

        field_checkers = [
            checker(f.dataType, result_column_index=i) for i, f in enumerate(return_type)
        ]
        nullables = [f.nullable for f in return_type]

        if all(c is None for c in field_checkers) and all(nullables):
            return None

        def check(row):
            if isinstance(row, tuple):
                for i, (value, checker, nullable) in enumerate(zip(row, field_checkers, nullables)):
                    if value is None:
                        if not nullable:
                            raise_(i)
                    elif checker is not None:
                        checker(value)

        return check

    check_output_row_against_schema = build_null_checker(return_type)

    if eval_type == PythonEvalType.SQL_ARROW_TABLE_UDF:

        def wrap_arrow_udtf(f, return_type):
            import pandas as pd

            arrow_return_type = to_arrow_type(return_type)
            return_type_size = len(return_type)

            def verify_result(result):
                if not isinstance(result, pd.DataFrame):
                    raise PySparkTypeError(
                        errorClass="INVALID_ARROW_UDTF_RETURN_TYPE",
                        messageParameters={
                            "return_type": type(result).__name__,
                            "value": str(result),
                            "func": f.__name__,
                        },
                    )

                # Validate the output schema when the result dataframe has either output
                # rows or columns. Note that we avoid using `df.empty` here because the
                # result dataframe may contain an empty row. For example, when a UDTF is
                # defined as follows: def eval(self): yield tuple().
                if len(result) > 0 or len(result.columns) > 0:
                    if len(result.columns) != return_type_size:
                        raise PySparkRuntimeError(
                            errorClass="UDTF_RETURN_SCHEMA_MISMATCH",
                            messageParameters={
                                "expected": str(return_type_size),
                                "actual": str(len(result.columns)),
                                "func": f.__name__,
                            },
                        )

                # Verify the type and the schema of the result.
                verify_pandas_result(
                    result, return_type, assign_cols_by_name=False, truncate_return_schema=False
                )
                return result

            # Wrap the exception thrown from the UDTF in a PySparkRuntimeError.
            def func(*a: Any) -> Any:
                try:
                    return f(*a)
                except SkipRestOfInputTableException:
                    raise
                except Exception as e:
                    raise PySparkRuntimeError(
                        errorClass="UDTF_EXEC_ERROR",
                        messageParameters={"method_name": f.__name__, "error": str(e)},
                    )

            def check_return_value(res):
                # Check whether the result of an arrow UDTF is iterable before
                # using it to construct a pandas DataFrame.
                if res is not None:
                    if not isinstance(res, Iterable):
                        raise PySparkRuntimeError(
                            errorClass="UDTF_RETURN_NOT_ITERABLE",
                            messageParameters={
                                "type": type(res).__name__,
                                "func": f.__name__,
                            },
                        )
                    if check_output_row_against_schema is not None:
                        for row in res:
                            if row is not None:
                                check_output_row_against_schema(row)
                            yield row
                    else:
                        yield from res

            def evaluate(*args: pd.Series):
                if len(args) == 0:
                    res = func()
                    yield verify_result(pd.DataFrame(check_return_value(res))), arrow_return_type
                else:
                    # Create tuples from the input pandas Series, each tuple
                    # represents a row across all Series.
                    row_tuples = zip(*args)
                    for row in row_tuples:
                        res = func(*row)
                        yield verify_result(
                            pd.DataFrame(check_return_value(res))
                        ), arrow_return_type

            return evaluate

        eval_func_kwargs_support, args_kwargs_offsets = wrap_kwargs_support(
            getattr(udtf, "eval"), args_offsets, kwargs_offsets
        )
        eval = wrap_arrow_udtf(eval_func_kwargs_support, return_type)

        if hasattr(udtf, "terminate"):
            terminate = wrap_arrow_udtf(getattr(udtf, "terminate"), return_type)
        else:
            terminate = None

        cleanup = getattr(udtf, "cleanup") if hasattr(udtf, "cleanup") else None

        def mapper(_, it):
            try:
                for a in it:
                    # The eval function yields an iterator. Each element produced by this
                    # iterator is a tuple in the form of (pandas.DataFrame, arrow_return_type).
                    yield from eval(*[a[o] for o in args_kwargs_offsets])
                if terminate is not None:
                    yield from terminate()
            except SkipRestOfInputTableException:
                if terminate is not None:
                    yield from terminate()
            finally:
                if cleanup is not None:
                    cleanup()

        return mapper, None, ser, ser

    else:

        def wrap_udtf(f, return_type):
            assert return_type.needConversion()
            toInternal = return_type.toInternal
            return_type_size = len(return_type)

            def verify_and_convert_result(result):
                if result is not None:
                    if hasattr(result, "__len__") and len(result) != return_type_size:
                        raise PySparkRuntimeError(
                            errorClass="UDTF_RETURN_SCHEMA_MISMATCH",
                            messageParameters={
                                "expected": str(return_type_size),
                                "actual": str(len(result)),
                                "func": f.__name__,
                            },
                        )

                    if not (isinstance(result, (list, dict, tuple)) or hasattr(result, "__dict__")):
                        raise PySparkRuntimeError(
                            errorClass="UDTF_INVALID_OUTPUT_ROW_TYPE",
                            messageParameters={
                                "type": type(result).__name__,
                                "func": f.__name__,
                            },
                        )
                    if check_output_row_against_schema is not None:
                        check_output_row_against_schema(result)
                return toInternal(result)

            # Evaluate the function and return a tuple back to the executor.
            def evaluate(*a) -> tuple:
                try:
                    res = f(*a)
                except SkipRestOfInputTableException:
                    raise
                except Exception as e:
                    raise PySparkRuntimeError(
                        errorClass="UDTF_EXEC_ERROR",
                        messageParameters={"method_name": f.__name__, "error": str(e)},
                    )

                if res is None:
                    # If the function returns None or does not have an explicit return statement,
                    # an empty tuple is returned to the executor.
                    # This is because directly constructing tuple(None) results in an exception.
                    return tuple()

                if not isinstance(res, Iterable):
                    raise PySparkRuntimeError(
                        errorClass="UDTF_RETURN_NOT_ITERABLE",
                        messageParameters={
                            "type": type(res).__name__,
                            "func": f.__name__,
                        },
                    )

                # If the function returns a result, we map it to the internal representation and
                # returns the results as a tuple.
                return tuple(map(verify_and_convert_result, res))

            return evaluate

        eval_func_kwargs_support, args_kwargs_offsets = wrap_kwargs_support(
            getattr(udtf, "eval"), args_offsets, kwargs_offsets
        )
        eval = wrap_udtf(eval_func_kwargs_support, return_type)

        if hasattr(udtf, "terminate"):
            terminate = wrap_udtf(getattr(udtf, "terminate"), return_type)
        else:
            terminate = None

        cleanup = getattr(udtf, "cleanup") if hasattr(udtf, "cleanup") else None

        # Return an iterator of iterators.
        def mapper(_, it):
            try:
                for a in it:
                    yield eval(*[a[o] for o in args_kwargs_offsets])
                if terminate is not None:
                    yield terminate()
            except SkipRestOfInputTableException:
                if terminate is not None:
                    yield terminate()
            finally:
                if cleanup is not None:
                    cleanup()

        return mapper, None, ser, ser


def read_udfs(pickleSer, infile, eval_type):
    runner_conf = {}

    state_server_port = None
    key_schema = None
    if eval_type in (
        PythonEvalType.SQL_ARROW_BATCHED_UDF,
        PythonEvalType.SQL_SCALAR_PANDAS_UDF,
        PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
        PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
        PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
        PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
        PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
        PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF,
        PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF,
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF,
    ):
        # Load conf used for pandas_udf evaluation
        num_conf = read_int(infile)
        for i in range(num_conf):
            k = utf8_deserializer.loads(infile)
            v = utf8_deserializer.loads(infile)
            runner_conf[k] = v

        state_object_schema = None
        if eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE:
            state_object_schema = StructType.fromJson(json.loads(utf8_deserializer.loads(infile)))
        elif eval_type == PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF:
            state_server_port = read_int(infile)
            key_schema = StructType.fromJson(json.loads(utf8_deserializer.loads(infile)))

        # NOTE: if timezone is set here, that implies respectSessionTimeZone is True
        timezone = runner_conf.get("spark.sql.session.timeZone", None)
        safecheck = (
            runner_conf.get("spark.sql.execution.pandas.convertToArrowArraySafely", "false").lower()
            == "true"
        )
        _assign_cols_by_name = assign_cols_by_name(runner_conf)

        if eval_type == PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF:
            ser = CogroupArrowUDFSerializer(_assign_cols_by_name)
        elif eval_type == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
            ser = CogroupPandasUDFSerializer(timezone, safecheck, _assign_cols_by_name)
        elif eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE:
            arrow_max_records_per_batch = runner_conf.get(
                "spark.sql.execution.arrow.maxRecordsPerBatch", 10000
            )
            arrow_max_records_per_batch = int(arrow_max_records_per_batch)

            ser = ApplyInPandasWithStateSerializer(
                timezone,
                safecheck,
                _assign_cols_by_name,
                state_object_schema,
                arrow_max_records_per_batch,
            )
        elif eval_type == PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF:
            arrow_max_records_per_batch = runner_conf.get(
                "spark.sql.execution.arrow.maxRecordsPerBatch", 10000
            )
            arrow_max_records_per_batch = int(arrow_max_records_per_batch)

            ser = TransformWithStateInPandasSerializer(
                timezone, safecheck, _assign_cols_by_name, arrow_max_records_per_batch
            )

        elif eval_type == PythonEvalType.SQL_MAP_ARROW_ITER_UDF:
            ser = ArrowStreamUDFSerializer()
        elif eval_type == PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF:
            ser = ArrowStreamGroupUDFSerializer(_assign_cols_by_name)
        else:
            # Scalar Pandas UDF handles struct type arguments as pandas DataFrames instead of
            # pandas Series. See SPARK-27240.
            df_for_struct = (
                eval_type == PythonEvalType.SQL_SCALAR_PANDAS_UDF
                or eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
                or eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF
            )
            # Arrow-optimized Python UDF takes a struct type argument as a Row
            struct_in_pandas = (
                "row" if eval_type == PythonEvalType.SQL_ARROW_BATCHED_UDF else "dict"
            )
            ndarray_as_list = eval_type == PythonEvalType.SQL_ARROW_BATCHED_UDF
            # Arrow-optimized Python UDF uses explicit Arrow cast for type coercion
            arrow_cast = eval_type == PythonEvalType.SQL_ARROW_BATCHED_UDF
            ser = ArrowStreamPandasUDFSerializer(
                timezone,
                safecheck,
                _assign_cols_by_name,
                df_for_struct,
                struct_in_pandas,
                ndarray_as_list,
                arrow_cast,
            )
    else:
        ser = BatchedSerializer(CPickleSerializer(), 100)

    is_profiling = read_bool(infile)
    if is_profiling:
        profiler = utf8_deserializer.loads(infile)
    else:
        profiler = None

    num_udfs = read_int(infile)

    is_scalar_iter = eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
    is_map_pandas_iter = eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF
    is_map_arrow_iter = eval_type == PythonEvalType.SQL_MAP_ARROW_ITER_UDF

    if is_scalar_iter or is_map_pandas_iter or is_map_arrow_iter:
        if is_scalar_iter:
            assert num_udfs == 1, "One SCALAR_ITER UDF expected here."
        if is_map_pandas_iter:
            assert num_udfs == 1, "One MAP_PANDAS_ITER UDF expected here."
        if is_map_arrow_iter:
            assert num_udfs == 1, "One MAP_ARROW_ITER UDF expected here."

        arg_offsets, udf = read_single_udf(
            pickleSer, infile, eval_type, runner_conf, udf_index=0, profiler=profiler
        )

        def func(_, iterator):
            num_input_rows = 0

            def map_batch(batch):
                nonlocal num_input_rows

                udf_args = [batch[offset] for offset in arg_offsets]
                num_input_rows += len(udf_args[0])
                if len(udf_args) == 1:
                    return udf_args[0]
                else:
                    return tuple(udf_args)

            iterator = map(map_batch, iterator)
            result_iter = udf(iterator)

            num_output_rows = 0
            for result_batch, result_type in result_iter:
                num_output_rows += len(result_batch)
                # This check is for Scalar Iterator UDF to fail fast.
                # The length of the entire input can only be explicitly known
                # by consuming the input iterator in user side. Therefore,
                # it's very unlikely the output length is higher than
                # input length.
                if is_scalar_iter and num_output_rows > num_input_rows:
                    raise PySparkRuntimeError(
                        errorClass="PANDAS_UDF_OUTPUT_EXCEEDS_INPUT_ROWS", messageParameters={}
                    )
                yield (result_batch, result_type)

            if is_scalar_iter:
                try:
                    next(iterator)
                except StopIteration:
                    pass
                else:
                    raise PySparkRuntimeError(
                        errorClass="STOP_ITERATION_OCCURRED_FROM_SCALAR_ITER_PANDAS_UDF",
                        messageParameters={},
                    )

                if num_output_rows != num_input_rows:
                    raise PySparkRuntimeError(
                        errorClass="RESULT_LENGTH_MISMATCH_FOR_SCALAR_ITER_PANDAS_UDF",
                        messageParameters={
                            "output_length": str(num_output_rows),
                            "input_length": str(num_input_rows),
                        },
                    )

        # profiling is not supported for UDF
        return func, None, ser, ser

    def extract_key_value_indexes(grouped_arg_offsets):
        """
        Helper function to extract the key and value indexes from arg_offsets for the grouped and
        cogrouped pandas udfs. See BasePandasGroupExec.resolveArgOffsets for equivalent scala code.

        Parameters
        ----------
        grouped_arg_offsets:  list
            List containing the key and value indexes of columns of the
            DataFrames to be passed to the udf. It consists of n repeating groups where n is the
            number of DataFrames.  Each group has the following format:
                group[0]: length of group
                group[1]: length of key indexes
                group[2.. group[1] +2]: key attributes
                group[group[1] +3 group[0]]: value attributes
        """
        parsed = []
        idx = 0
        while idx < len(grouped_arg_offsets):
            offsets_len = grouped_arg_offsets[idx]
            idx += 1
            offsets = grouped_arg_offsets[idx : idx + offsets_len]
            split_index = offsets[0] + 1
            offset_keys = offsets[1:split_index]
            offset_values = offsets[split_index:]
            parsed.append([offset_keys, offset_values])
            idx += offsets_len
        return parsed

    if eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF:
        # We assume there is only one UDF here because grouped map doesn't
        # support combining multiple UDFs.
        assert num_udfs == 1

        # See FlatMapGroupsInPandasExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes
        arg_offsets, f = read_single_udf(
            pickleSer, infile, eval_type, runner_conf, udf_index=0, profiler=profiler
        )
        parsed_offsets = extract_key_value_indexes(arg_offsets)

        # Create function like this:
        #   mapper a: f([a[0]], [a[0], a[1]])
        def mapper(a):
            keys = [a[o] for o in parsed_offsets[0][0]]
            vals = [a[o] for o in parsed_offsets[0][1]]
            return f(keys, vals)

    elif eval_type == PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF:
        # We assume there is only one UDF here because grouped map doesn't
        # support combining multiple UDFs.
        assert num_udfs == 1

        # See TransformWithStateInPandasExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes
        arg_offsets, f = read_single_udf(
            pickleSer, infile, eval_type, runner_conf, udf_index=0, profiler=profiler
        )
        parsed_offsets = extract_key_value_indexes(arg_offsets)
        ser.key_offsets = parsed_offsets[0][0]
        stateful_processor_api_client = StatefulProcessorApiClient(state_server_port, key_schema)

        # Create function like this:
        #   mapper a: f([a[0]], [a[0], a[1]])
        def mapper(a):
            key = a[0]

            def values_gen():
                for x in a[1]:
                    retVal = [x[1][o] for o in parsed_offsets[0][1]]
                    yield retVal

            # This must be generator comprehension - do not materialize.
            return f(stateful_processor_api_client, key, values_gen())

    elif eval_type == PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF:
        import pyarrow as pa

        # We assume there is only one UDF here because grouped map doesn't
        # support combining multiple UDFs.
        assert num_udfs == 1

        # See FlatMapGroupsInPandasExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes
        arg_offsets, f = read_single_udf(
            pickleSer, infile, eval_type, runner_conf, udf_index=0, profiler=profiler
        )
        parsed_offsets = extract_key_value_indexes(arg_offsets)

        def batch_from_offset(batch, offsets):
            return pa.RecordBatch.from_arrays(
                arrays=[batch.columns[o] for o in offsets],
                names=[batch.schema.names[o] for o in offsets],
            )

        def table_from_batches(batches, offsets):
            return pa.Table.from_batches([batch_from_offset(batch, offsets) for batch in batches])

        def mapper(a):
            keys = table_from_batches(a, parsed_offsets[0][0])
            vals = table_from_batches(a, parsed_offsets[0][1])
            return f(keys, vals)

    elif eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE:
        # We assume there is only one UDF here because grouped map doesn't
        # support combining multiple UDFs.
        assert num_udfs == 1

        # See FlatMapGroupsInPandas(WithState)Exec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes
        arg_offsets, f = read_single_udf(
            pickleSer, infile, eval_type, runner_conf, udf_index=0, profiler=profiler
        )
        parsed_offsets = extract_key_value_indexes(arg_offsets)

        def mapper(a):
            """
            The function receives (iterator of data, state) and performs extraction of key and
            value from the data, with retaining lazy evaluation.

            See `load_stream` in `ApplyInPandasWithStateSerializer` for more details on the input
            and see `wrap_grouped_map_pandas_udf_with_state` for more details on how output will
            be used.
            """
            from itertools import tee

            state = a[1]
            data_gen = (x[0] for x in a[0])

            # We know there should be at least one item in the iterator/generator.
            # We want to peek the first element to construct the key, hence applying
            # tee to construct the key while we retain another iterator/generator
            # for values.
            keys_gen, values_gen = tee(data_gen)
            keys_elem = next(keys_gen)
            keys = [keys_elem[o] for o in parsed_offsets[0][0]]

            # This must be generator comprehension - do not materialize.
            vals = ([x[o] for o in parsed_offsets[0][1]] for x in values_gen)

            return f(keys, vals, state)

    elif eval_type == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
        # We assume there is only one UDF here because cogrouped map doesn't
        # support combining multiple UDFs.
        assert num_udfs == 1
        arg_offsets, f = read_single_udf(
            pickleSer, infile, eval_type, runner_conf, udf_index=0, profiler=profiler
        )

        parsed_offsets = extract_key_value_indexes(arg_offsets)

        def mapper(a):
            df1_keys = [a[0][o] for o in parsed_offsets[0][0]]
            df1_vals = [a[0][o] for o in parsed_offsets[0][1]]
            df2_keys = [a[1][o] for o in parsed_offsets[1][0]]
            df2_vals = [a[1][o] for o in parsed_offsets[1][1]]
            return f(df1_keys, df1_vals, df2_keys, df2_vals)

    elif eval_type == PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF:
        import pyarrow as pa

        # We assume there is only one UDF here because cogrouped map doesn't
        # support combining multiple UDFs.
        assert num_udfs == 1
        arg_offsets, f = read_single_udf(
            pickleSer, infile, eval_type, runner_conf, udf_index=0, profiler=profiler
        )

        parsed_offsets = extract_key_value_indexes(arg_offsets)

        def batch_from_offset(batch, offsets):
            return pa.RecordBatch.from_arrays(
                arrays=[batch.columns[o] for o in offsets],
                names=[batch.schema.names[o] for o in offsets],
            )

        def table_from_batches(batches, offsets):
            return pa.Table.from_batches([batch_from_offset(batch, offsets) for batch in batches])

        def mapper(a):
            df1_keys = table_from_batches(a[0], parsed_offsets[0][0])
            df1_vals = table_from_batches(a[0], parsed_offsets[0][1])
            df2_keys = table_from_batches(a[1], parsed_offsets[1][0])
            df2_vals = table_from_batches(a[1], parsed_offsets[1][1])
            return f(df1_keys, df1_vals, df2_keys, df2_vals)

    else:
        udfs = []
        for i in range(num_udfs):
            udfs.append(
                read_single_udf(
                    pickleSer, infile, eval_type, runner_conf, udf_index=i, profiler=profiler
                )
            )

        def mapper(a):
            result = tuple(f(*[a[o] for o in arg_offsets]) for arg_offsets, f in udfs)
            # In the special case of a single UDF this will return a single result rather
            # than a tuple of results; this is the format that the JVM side expects.
            if len(result) == 1:
                return result[0]
            else:
                return result

    def func(_, it):
        return map(mapper, it)

    # profiling is not supported for UDF
    return func, None, ser, ser


def main(infile, outfile):
    faulthandler_log_path = os.environ.get("PYTHON_FAULTHANDLER_DIR", None)
    try:
        if faulthandler_log_path:
            faulthandler_log_path = os.path.join(faulthandler_log_path, str(os.getpid()))
            faulthandler_log_file = open(faulthandler_log_path, "w")
            faulthandler.enable(file=faulthandler_log_file)

        boot_time = time.time()
        split_index = read_int(infile)
        if split_index == -1:  # for unit tests
            sys.exit(-1)

        check_python_version(infile)

        # read inputs only for a barrier task
        isBarrier = read_bool(infile)
        boundPort = read_int(infile)
        secret = UTF8Deserializer().loads(infile)

        memory_limit_mb = int(os.environ.get("PYSPARK_EXECUTOR_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

        # initialize global state
        taskContext = None
        if isBarrier:
            taskContext = BarrierTaskContext._getOrCreate()
            BarrierTaskContext._initialize(boundPort, secret)
            # Set the task context instance here, so we can get it by TaskContext.get for
            # both TaskContext and BarrierTaskContext
            TaskContext._setTaskContext(taskContext)
        else:
            taskContext = TaskContext._getOrCreate()
        # read inputs for TaskContext info
        taskContext._stageId = read_int(infile)
        taskContext._partitionId = read_int(infile)
        taskContext._attemptNumber = read_int(infile)
        taskContext._taskAttemptId = read_long(infile)
        taskContext._cpus = read_int(infile)
        taskContext._resources = {}
        for r in range(read_int(infile)):
            key = utf8_deserializer.loads(infile)
            name = utf8_deserializer.loads(infile)
            addresses = []
            taskContext._resources = {}
            for a in range(read_int(infile)):
                addresses.append(utf8_deserializer.loads(infile))
            taskContext._resources[key] = ResourceInformation(name, addresses)

        taskContext._localProperties = dict()
        for i in range(read_int(infile)):
            k = utf8_deserializer.loads(infile)
            v = utf8_deserializer.loads(infile)
            taskContext._localProperties[k] = v

        shuffle.MemoryBytesSpilled = 0
        shuffle.DiskBytesSpilled = 0
        _accumulatorRegistry.clear()

        setup_spark_files(infile)
        setup_broadcasts(infile)

        _accumulatorRegistry.clear()
        eval_type = read_int(infile)
        if eval_type == PythonEvalType.NON_UDF:
            func, profiler, deserializer, serializer = read_command(pickleSer, infile)
        elif eval_type in (PythonEvalType.SQL_TABLE_UDF, PythonEvalType.SQL_ARROW_TABLE_UDF):
            func, profiler, deserializer, serializer = read_udtf(pickleSer, infile, eval_type)
        else:
            func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)

        init_time = time.time()

        def process():
            iterator = deserializer.load_stream(infile)
            out_iter = func(split_index, iterator)
            try:
                serializer.dump_stream(out_iter, outfile)
            finally:
                if hasattr(out_iter, "close"):
                    out_iter.close()

        if profiler:
            profiler.profile(process)
        else:
            process()

        # Reset task context to None. This is a guard code to avoid residual context when worker
        # reuse.
        TaskContext._setTaskContext(None)
        BarrierTaskContext._setTaskContext(None)
    except BaseException as e:
        handle_worker_exception(e, outfile)
        sys.exit(-1)
    finally:
        if faulthandler_log_path:
            faulthandler.disable()
            faulthandler_log_file.close()
            os.remove(faulthandler_log_path)
    finish_time = time.time()
    report_times(outfile, boot_time, init_time, finish_time)
    write_long(shuffle.MemoryBytesSpilled, outfile)
    write_long(shuffle.DiskBytesSpilled, outfile)

    # Mark the beginning of the accumulators section of the output
    write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
    send_accumulator_updates(outfile)

    # check end of stream
    if read_int(infile) == SpecialLengths.END_OF_STREAM:
        write_int(SpecialLengths.END_OF_STREAM, outfile)
    else:
        # write a different value to tell JVM to not reuse this worker
        write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
        sys.exit(-1)


if __name__ == "__main__":
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    # TODO: Remove the following two lines and use `Process.pid()` when we drop JDK 8.
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
