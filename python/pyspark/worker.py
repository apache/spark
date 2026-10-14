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

import dataclasses
import inspect
import itertools
import json
import os
import sys
import time
import warnings
from collections.abc import Iterator
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Iterable,
    Optional,
    Tuple,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

from pyspark import shuffle
from pyspark.accumulators import (
    SpecialAccumulatorIds,
    _accumulatorRegistry,
    _deserialize_accumulator,
)
from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.eval_handlers._udtf import TableUDTFHandler, UDTFEvalTypeHandler
from pyspark.eval_handlers.utils import extract_key_value_indexes, wrap_kwargs_support
from pyspark.logger.worker_io import capture_outputs
from pyspark.messages import (
    SparkMessageReceiver,
    SparkSocketMessageReceiver,
)
from pyspark.serializers import (
    BatchedSerializer,
    CPickleSerializer,
    SpecialLengths,
    write_int,
    write_with_length,
)
from pyspark.sql.conversion import (
    ArrowBatchTransformer,
    ArrowToPandasConversion,
    PandasToArrowConversion,
)
from pyspark.sql.pandas.serializers import ArrowStreamSerializer
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.sql.streaming.stateful_processor_util import TransformWithStateInPandasFuncMode
from pyspark.sql.types import (
    Row,
    StructField,
    StructType,
    _parse_datatype_json_string,
)
from pyspark.taskcontext import BarrierTaskContext, TaskContext
from pyspark.util import (
    PythonEvalType,
    fail_on_stopiteration,
    handle_worker_exception,
    start_faulthandler_periodic_traceback,
    with_faulthandler,
)
from pyspark.worker_message import UDTFInfo, WorkerInitInfo
from pyspark.worker_util import (
    EvalConf,
    RunnerConf,
    check_python_version,
    get_sock_file_to_executor,
    pickleSer,
    read_command,
    send_accumulator_updates,
    setup_broadcasts,
    setup_memory_limits,
    setup_spark_files,
)


def report_metrics(
    outfile, boot, init, finish, execution_duration_ms, memory_bytes_spilled, disk_bytes_spilled
):
    """Write the worker metrics as a length-prefixed JSON report after METRICS_DATA."""
    payload = json.dumps(
        {
            "bootTimestampMs": int(1000 * boot),
            "initTimestampMs": int(1000 * init),
            "finishTimestampMs": int(1000 * finish),
            "pythonExecutionDurationMs": execution_duration_ms,
            "memoryBytesSpilled": memory_bytes_spilled,
            "diskBytesSpilled": disk_bytes_spilled,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    write_int(SpecialLengths.METRICS_DATA, outfile)
    write_with_length(payload, outfile)


def chain(f, g):
    """chain two functions together"""
    return lambda *a: g(f(*a))


def _is_iter_based(eval_type: int) -> bool:
    return eval_type in (
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
        PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF,
        # Iterator UDFs lifted out of a higher-order function lambda keep the iterator contract:
        # the user function still consumes and produces an iterator of batches; the worker only
        # feeds it the flattened elements and re-nests the results. See ExtractPythonUDFFromLambda.
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF,
        PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF,
        PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
        PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
        PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_ITER_UDF,
    )


def wrap_perf_profiler(f, eval_type, result_id):
    from pyspark.sql.profiler import ProfileResultsParam, ProfileResultsParamV2, WorkerPerfProfiler

    accumulator = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
    )
    accumulator_v2 = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER_V2, {}, ProfileResultsParamV2
    )

    if _is_iter_based(eval_type):

        def profiling_func(*args, **kwargs):
            iterator = iter(f(*args, **kwargs))
            while True:
                try:
                    with WorkerPerfProfiler(accumulator, accumulator_v2, result_id):
                        item = next(iterator)
                    yield item
                except StopIteration:
                    break

    else:

        def profiling_func(*args, **kwargs):
            with WorkerPerfProfiler(accumulator, accumulator_v2, result_id):
                ret = f(*args, **kwargs)
            return ret

    return profiling_func


def wrap_memory_profiler(f, eval_type, result_id):
    import pyspark.memory_profiler_ext
    from pyspark.sql.profiler import (
        ProfileResultsParam,
        ProfileResultsParamV2,
        WorkerMemoryProfiler,
    )

    if not pyspark.memory_profiler_ext.has_memory_profiler:
        return f

    accumulator = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
    )

    accumulator_v2 = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER_V2, {}, ProfileResultsParamV2
    )

    if _is_iter_based(eval_type):

        def profiling_func(*args, **kwargs):
            g = f(*args, **kwargs)
            iterator = iter(g)

            while True:
                try:
                    with WorkerMemoryProfiler(accumulator, accumulator_v2, result_id, g.gi_code):
                        item = next(iterator)
                    yield item
                except StopIteration:
                    break

    else:

        def profiling_func(*args, **kwargs):
            with WorkerMemoryProfiler(accumulator, accumulator_v2, result_id, f):
                ret = f(*args, **kwargs)
            return ret

    return profiling_func


def read_single_udf(pickleSer, udf_info, eval_type, runner_conf, udf_index):
    chained_func = None
    for udf in udf_info.udfs:
        f, return_type = read_command(pickleSer, udf)
        if chained_func is None:
            chained_func = f
        else:
            chained_func = chain(chained_func, f)

    # If chained_func is from pyspark.sql.worker, it is to read/write data source.
    # In this case, we check the data_source_profiler config.
    module = getattr(chained_func, "__module__", "")
    if isinstance(module, str) and module.startswith("pyspark.sql.worker."):
        profiler = runner_conf.data_source_profiler
    else:
        profiler = runner_conf.udf_profiler
    if profiler == "perf":
        profiling_func = wrap_perf_profiler(chained_func, eval_type, udf_info.result_id)
    elif profiler == "memory":
        profiling_func = wrap_memory_profiler(chained_func, eval_type, udf_info.result_id)
    else:
        profiling_func = chained_func

    if eval_type in (
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF,
        PythonEvalType.SQL_ARROW_BATCHED_UDF,
    ):
        func = profiling_func
    else:
        # make sure StopIteration's raised in the user code are not ignored
        # when they are processed in a for loop, raise them as RuntimeError's instead
        func = fail_on_stopiteration(profiling_func)

    args_offsets, kwargs_offsets = udf_info.args, udf_info.kwargs

    # The last returnType will be the return type of UDF. Eval types are grouped below by the
    # shape of the value they return.

    # Incremental Python aggregators: the pickled "function" is the Aggregator object itself, whose
    # zero/reduce/merge/finish methods the worker calls directly. Return it unwrapped (not through
    # fail_on_stopiteration, which would treat it as a plain callable).
    if eval_type in (
        PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_PARTIAL_UDF,
        PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF,
        PythonEvalType.SQL_WINDOW_AGG_ARROW_INCREMENTAL_UDF,
    ):
        return chained_func, args_offsets, kwargs_offsets, return_type

    # Scalar, aggregation and window UDFs: (func, args_offsets, kwargs_offsets, return_type).
    if eval_type in (
        PythonEvalType.SQL_ARROW_BATCHED_UDF,
        PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF,
        PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF,
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF,
        PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF,
        PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF,
        PythonEvalType.SQL_GROUPED_AGG_ARROW_ITER_UDF,
        PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF,
        PythonEvalType.SQL_GROUPED_AGG_PANDAS_ITER_UDF,
        PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
        PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF,
        PythonEvalType.SQL_SCALAR_ARROW_UDF,
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
        PythonEvalType.SQL_SCALAR_PANDAS_UDF,
        PythonEvalType.SQL_WINDOW_AGG_ARROW_UDF,
        PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
    ):
        return func, args_offsets, kwargs_offsets, return_type
    # Grouped-map and cogrouped-map UDFs: (func, args_offsets, return_type, num_udf_args).
    elif eval_type in (
        PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF,
        PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
        PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF,
        PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_ITER_UDF,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
    ):
        # signature was lost when wrapping it
        num_udf_args = len(inspect.getfullargspec(chained_func).args)
        return func, args_offsets, return_type, num_udf_args
    # Grouped-map-with-state and transform-with-state UDFs: (func, args_offsets, return_type).
    elif eval_type in (
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF,
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF,
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_INIT_STATE_UDF,
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_UDF,
    ):
        return func, args_offsets, return_type
    # Map iterator UDFs take no offsets.
    elif eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF:
        return func, None, None, return_type
    elif eval_type == PythonEvalType.SQL_MAP_ARROW_ITER_UDF:
        return func, None, None, None
    # Batched (plain Python) UDFs: (args_kwargs_offsets, eval func); apply kwargs binding and
    # convert each result to the internal representation only when the return type requires it.
    elif eval_type == PythonEvalType.SQL_BATCHED_UDF:
        func, args_kwargs_offsets = wrap_kwargs_support(func, args_offsets, kwargs_offsets)
        if return_type.needConversion():
            toInternal = return_type.toInternal
            return args_kwargs_offsets, lambda *a: toInternal(func(*a))
        else:
            return args_kwargs_offsets, lambda *a: func(*a)
    else:
        raise ValueError("Unknown eval type: {}".format(eval_type))


# Read a serialized user-defined table function (UDTF) and prepare it for evaluation: wrap
# the constructor to receive the AnalyzeResult, instantiate the UDTF (wrapped in
# partition_wrapper when its TABLE argument has PARTITION BY expressions), and check the 'eval'
# signature against the call arguments. Returns the same
# (func, args_offsets, kwargs_offsets, return_type) shape as read_single_udf, with the UDTF
# instance in the func slot.
def read_single_udtf(pickleSer, udtf_info, partition_wrapper):
    if udtf_info.pickled_analyze_result is not None:
        pickled_analyze_result = pickleSer.loads(udtf_info.pickled_analyze_result)
    else:
        pickled_analyze_result = None
    # Initially we assume that the UDTF __init__ method accepts the pickled AnalyzeResult,
    # although we may set this to false later if we find otherwise.
    handler = read_command(pickleSer, udtf_info.handler)
    if not isinstance(handler, type):
        raise PySparkRuntimeError(
            f"Invalid UDTF handler type. Expected a class (type 'type'), but "
            f"got an instance of {type(handler).__name__}."
        )

    return_type = _parse_datatype_json_string(udtf_info.return_type)
    if not isinstance(return_type, StructType):
        raise PySparkRuntimeError(
            f"The return type of a UDTF must be a struct type, but got {type(return_type)}."
        )

    # Update the handler that creates a new UDTF instance to first try calling the UDTF constructor
    # with one argument containing the previous AnalyzeResult. If that fails, then try a constructor
    # with no arguments. In this way each UDTF class instance can decide if it wants to inspect the
    # AnalyzeResult.
    udtf_init_args = inspect.getfullargspec(handler)
    if pickled_analyze_result is not None:
        if len(udtf_init_args.args) > 2:
            raise PySparkRuntimeError(
                errorClass="UDTF_CONSTRUCTOR_INVALID_IMPLEMENTS_ANALYZE_METHOD",
                messageParameters={"name": udtf_info.name},
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
            messageParameters={"name": udtf_info.name},
        )

    # Instantiate the UDTF class.
    try:
        if len(udtf_info.partition_child_indexes) > 0:
            udtf = partition_wrapper(handler, udtf_info.partition_child_indexes)
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
        inspect.signature(udtf.eval).bind(*udtf_info.args, **udtf_info.kwargs)
    except TypeError as e:
        raise PySparkRuntimeError(
            errorClass="UDTF_EVAL_METHOD_ARGUMENTS_DO_NOT_MATCH_SIGNATURE",
            messageParameters={"name": udtf_info.name, "reason": str(e)},
        ) from None

    return udtf, udtf_info.args, udtf_info.kwargs, return_type


def read_udfs(pickleSer, udf_info_list, eval_type, runner_conf, eval_conf):
    if eval_type in (
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF,
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF,
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_UDF,
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_INIT_STATE_UDF,
    ):
        # NOTE: if timezone is set here, that implies respectSessionTimeZone is True
        ser = ArrowStreamSerializer(write_start_stream=True)
    else:
        batch_size = int(os.environ.get("PYTHON_UDF_BATCH_SIZE", "100"))
        ser = BatchedSerializer(CPickleSerializer(), batch_size)

    udfs = [
        read_single_udf(pickleSer, udf_info, eval_type, runner_conf, udf_index=udf_index)
        for udf_index, udf_info in enumerate(udf_info_list)
    ]

    num_udfs = len(udfs)

    if eval_type == PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF:
        import pandas as pd
        import pyarrow as pa

        assert num_udfs == 1, "One TRANSFORM_WITH_STATE_PANDAS UDF expected here."
        udf, arg_offsets, return_type = udfs[0]

        # See TransformWithStateInPandasExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes
        parsed_offsets = extract_key_value_indexes(arg_offsets)
        assert len(parsed_offsets) == 1, (
            "Expected one pair of offsets for TRANSFORM_WITH_STATE_PANDAS UDF."
        )

        key_offsets = parsed_offsets[0][0]
        value_offsets = parsed_offsets[0][1]
        output_schema = StructType([StructField("_0", return_type)])

        stateful_processor_api_client = StatefulProcessorApiClient(
            eval_conf.state_server_socket_port,
            eval_conf.grouping_key_schema,
            auth_secret=eval_conf.state_server_auth_secret,
        )

        arrow_max_records_per_batch = runner_conf.arrow_max_records_per_batch
        arrow_max_records_per_batch = (
            arrow_max_records_per_batch if arrow_max_records_per_batch > 0 else 2**31 - 1
        )
        arrow_max_bytes_per_batch = runner_conf.arrow_max_bytes_per_batch

        def transform_with_state_func(
            split_index: int,
            batches: Iterator[pa.RecordBatch],
        ) -> Iterator[pa.RecordBatch]:
            """Apply transformWithStateInPandas UDF.

            Data chunks for the same grouping key appear sequentially in the
            input batches but may span batch boundaries, so rows are regrouped
            by key and re-chunked into pandas DataFrames bounded by
            arrow_max_records_per_batch and arrow_max_bytes_per_batch. The UDF
            is invoked once per grouping key with a lazy iterator of chunks,
            then once for PROCESS_TIMER and once for COMPLETE.
            """
            total_bytes = 0
            total_rows = 0
            average_arrow_row_size = 0.0

            def row_stream():
                nonlocal total_bytes, total_rows, average_arrow_row_size
                for batch in batches:
                    # Short circuit batch size stats if the batch size is
                    # unlimited as computing batch size is computationally
                    # expensive.
                    if arrow_max_bytes_per_batch != 2**31 - 1 and batch.num_rows > 0:
                        total_bytes += sum(
                            buf.size
                            for col in batch.columns
                            for buf in col.buffers()
                            if buf is not None
                        )
                        total_rows += batch.num_rows
                        average_arrow_row_size = total_bytes / total_rows
                    data_pandas = ArrowToPandasConversion.to_pandas(
                        batch,
                        timezone=runner_conf.timezone,
                        prefer_int_ext_dtype=runner_conf.prefer_int_ext_dtype,
                    )
                    for row in pd.concat(data_pandas, axis=1).itertuples(index=False):
                        batch_key = tuple(row[o] for o in key_offsets)
                        yield (batch_key, row)

            def generate_data_batches():
                """
                Deserialize ArrowRecordBatches and return a generator of
                (grouping key, pandas.DataFrame) chunks.

                This function must avoid materializing multiple Arrow
                RecordBatches into memory at the same time, and data chunks
                from the same grouping key should appear sequentially.
                """
                for batch_key, group_rows in itertools.groupby(row_stream(), key=lambda x: x[0]):
                    rows = []
                    for _, row in group_rows:
                        rows.append(row)
                        if (
                            len(rows) >= arrow_max_records_per_batch
                            or len(rows) * average_arrow_row_size >= arrow_max_bytes_per_batch
                        ):
                            yield (batch_key, pd.DataFrame(rows))
                            rows = []
                    if rows:
                        yield (batch_key, pd.DataFrame(rows))

            def convert_results(result_iter):
                for result in result_iter:
                    if isinstance(return_type, StructType) and not isinstance(result, pd.DataFrame):
                        raise PySparkValueError(
                            "Invalid return type. Please make sure that the UDF returns a "
                            "pandas.DataFrame when the specified return type is StructType."
                        )
                    yield PandasToArrowConversion.from_pandas(
                        [result],
                        output_schema,
                        timezone=runner_conf.timezone,
                        safecheck=runner_conf.safecheck,
                        arrow_cast=True,
                        assign_cols_by_name=runner_conf.assign_cols_by_name,
                        int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
                    )

            for key, group in itertools.groupby(generate_data_batches(), key=lambda x: x[0]):
                # This must be a generator expression - do not materialize.
                values_gen = (df.iloc[:, value_offsets] for _, df in group)
                yield from convert_results(
                    udf(
                        stateful_processor_api_client,
                        TransformWithStateInPandasFuncMode.PROCESS_DATA,
                        key,
                        values_gen,
                    )
                )

            yield from convert_results(
                udf(
                    stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.PROCESS_TIMER,
                    None,
                    iter([]),
                )
            )

            yield from convert_results(
                udf(
                    stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.COMPLETE,
                    None,
                    iter([]),
                )
            )

        return transform_with_state_func, ser

    if eval_type == PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF:
        import pandas as pd
        import pyarrow as pa

        assert num_udfs == 1, "One TRANSFORM_WITH_STATE_PANDAS_INIT_STATE UDF expected here."
        udf, arg_offsets, return_type = udfs[0]

        # See TransformWithStateInPandasExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes.
        # parsed offsets:
        # [
        #     [groupingKeyOffsets, dedupDataOffsets],
        #     [initStateGroupingOffsets, dedupInitDataOffsets]
        # ]
        parsed_offsets = extract_key_value_indexes(arg_offsets)
        key_offsets = parsed_offsets[0][0]
        init_key_offsets = parsed_offsets[1][0]
        output_schema = StructType([StructField("_0", return_type)])

        stateful_processor_api_client = StatefulProcessorApiClient(
            eval_conf.state_server_socket_port,
            eval_conf.grouping_key_schema,
            auth_secret=eval_conf.state_server_auth_secret,
        )

        arrow_max_records_per_batch = runner_conf.arrow_max_records_per_batch
        arrow_max_records_per_batch = (
            arrow_max_records_per_batch if arrow_max_records_per_batch > 0 else 2**31 - 1
        )
        arrow_max_bytes_per_batch = runner_conf.arrow_max_bytes_per_batch

        def func(
            split_index: int,
            data: Iterator[pa.RecordBatch],
        ) -> Iterator[pa.RecordBatch]:
            """Apply transformWithStateInPandas UDF with initial state.

            The input batches carry two struct columns, ``inputData`` and
            ``initState``; each batch holds one or the other but never both.
            Rows are flattened out of whichever struct is present, regrouped by
            grouping key, and re-chunked into pandas DataFrames bounded by
            arrow_max_records_per_batch and arrow_max_bytes_per_batch. The UDF
            is invoked once per grouping key with two separate lazy iterators
            (data DataFrames and init-state DataFrames), then once for
            PROCESS_TIMER and once for COMPLETE.
            """
            total_bytes = 0
            total_rows = 0
            average_arrow_row_size = 0.0

            def flatten_columns(cur_batch: "pa.RecordBatch", col_name: str) -> "pa.Table":
                struct_column = cur_batch.column(cur_batch.schema.get_field_index(col_name))
                # Check if the entire column is null: an empty table (no columns)
                # signals the struct is absent from this batch.
                if struct_column.null_count == len(struct_column):
                    return pa.Table.from_arrays([], names=[])
                field_names = [
                    struct_column.type[i].name for i in range(struct_column.type.num_fields)
                ]
                field_arrays = [
                    struct_column.field(i) for i in range(struct_column.type.num_fields)
                ]
                return pa.Table.from_arrays(field_arrays, names=field_names)

            def to_pandas(table: "pa.Table") -> list:
                return ArrowToPandasConversion.to_pandas(
                    table,
                    timezone=runner_conf.timezone,
                    prefer_int_ext_dtype=runner_conf.prefer_int_ext_dtype,
                )

            def row_stream() -> Iterator[tuple]:
                nonlocal total_bytes, total_rows, average_arrow_row_size
                for batch in data:
                    # Short circuit batch size stats if the batch size is
                    # unlimited as computing batch size is computationally
                    # expensive.
                    if arrow_max_bytes_per_batch != 2**31 - 1 and batch.num_rows > 0:
                        total_bytes += sum(
                            buf.size
                            for col in batch.columns
                            for buf in col.buffers()
                            if buf is not None
                        )
                        total_rows += batch.num_rows
                        average_arrow_row_size = total_bytes / total_rows

                    data_table = flatten_columns(batch, "inputData")
                    init_table = flatten_columns(batch, "initState")

                    # Empty table has no columns. Each batch carries either
                    # input data or init state, never both.
                    has_data = data_table.num_columns > 0
                    has_init = init_table.num_columns > 0
                    assert not (has_data and has_init)

                    if has_data:
                        for row in pd.concat(to_pandas(data_table), axis=1).itertuples(index=False):
                            batch_key = tuple(row[o] for o in key_offsets)
                            yield (batch_key, row, None)
                    elif has_init:
                        for row in pd.concat(to_pandas(init_table), axis=1).itertuples(index=False):
                            batch_key = tuple(row[o] for o in init_key_offsets)
                            yield (batch_key, None, row)

            empty_dataframe = pd.DataFrame()

            def generate_data_batches() -> Iterator[tuple]:
                """
                Deserialize ArrowRecordBatches and return a generator of
                (grouping key, data DataFrame, init-state DataFrame) chunks.

                This function must avoid materializing multiple Arrow
                RecordBatches into memory at the same time, and data chunks
                from the same grouping key should appear sequentially.
                """
                for batch_key, group_rows in itertools.groupby(row_stream(), key=lambda x: x[0]):
                    rows = []
                    init_state_rows = []
                    for _, row, init_state_row in group_rows:
                        if row is not None:
                            rows.append(row)
                        if init_state_row is not None:
                            init_state_rows.append(init_state_row)

                        total_len = len(rows) + len(init_state_rows)
                        if (
                            total_len >= arrow_max_records_per_batch
                            or total_len * average_arrow_row_size >= arrow_max_bytes_per_batch
                        ):
                            yield (
                                batch_key,
                                pd.DataFrame(rows) if rows else empty_dataframe.copy(),
                                (
                                    pd.DataFrame(init_state_rows)
                                    if init_state_rows
                                    else empty_dataframe.copy()
                                ),
                            )
                            rows = []
                            init_state_rows = []
                    if rows or init_state_rows:
                        yield (
                            batch_key,
                            pd.DataFrame(rows) if rows else empty_dataframe.copy(),
                            (
                                pd.DataFrame(init_state_rows)
                                if init_state_rows
                                else empty_dataframe.copy()
                            ),
                        )

            def convert_results(
                result_iter: Iterable["pd.DataFrame"],
            ) -> Iterator["pa.RecordBatch"]:
                # TODO(SPARK-49100): add verification that elements in result_iter are
                # indeed of type pd.DataFrame and conform to assigned cols
                for result in result_iter:
                    if isinstance(return_type, StructType) and not isinstance(result, pd.DataFrame):
                        raise PySparkValueError(
                            "Invalid return type. Please make sure that the UDF returns a "
                            "pandas.DataFrame when the specified return type is StructType."
                        )
                    yield PandasToArrowConversion.from_pandas(
                        [result],
                        output_schema,
                        timezone=runner_conf.timezone,
                        safecheck=runner_conf.safecheck,
                        arrow_cast=True,
                        assign_cols_by_name=runner_conf.assign_cols_by_name,
                        int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
                    )

            for key, group in itertools.groupby(generate_data_batches(), key=lambda x: x[0]):
                # These must be generator expressions - do not materialize. The
                # UDF receives the data and init-state DataFrames as two
                # separate iterators, with empty chunks filtered out.
                group_data, group_init = itertools.tee(group, 2)
                state_values = (data_df for _, data_df, _ in group_data if not data_df.empty)
                init_states = (init_df for _, _, init_df in group_init if not init_df.empty)
                yield from convert_results(
                    udf(
                        stateful_processor_api_client,
                        TransformWithStateInPandasFuncMode.PROCESS_DATA,
                        key,
                        state_values,
                        init_states,
                    )
                )

            yield from convert_results(
                udf(
                    stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.PROCESS_TIMER,
                    None,
                    iter([]),
                    iter([]),
                )
            )

            yield from convert_results(
                udf(
                    stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.COMPLETE,
                    None,
                    iter([]),
                    iter([]),
                )
            )

        return func, ser

    if eval_type == PythonEvalType.SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_UDF:
        import pyarrow as pa

        assert num_udfs == 1, "One TRANSFORM_WITH_STATE_PYTHON_ROW UDF expected here."
        udf, arg_offsets, return_type = udfs[0]

        # See TransformWithStateInPySparkExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes.
        parsed_offsets = extract_key_value_indexes(arg_offsets)
        key_offsets = parsed_offsets[0][0]

        stateful_processor_api_client = StatefulProcessorApiClient(
            eval_conf.state_server_socket_port,
            eval_conf.grouping_key_schema,
            auth_secret=eval_conf.state_server_auth_secret,
        )

        def func(
            split_index: int,
            data: Iterator[pa.RecordBatch],
        ) -> Iterator[pa.RecordBatch]:
            """Apply transformWithStateInPySpark UDF over Row objects.

            Input batches are read row by row without materializing whole
            batches. Rows carrying the same grouping key appear sequentially, so
            they are regrouped by key and the UDF is invoked once per key with a
            lazy iterator of Row objects, then once for PROCESS_TIMER and once
            for COMPLETE. The UDF yields (iterator of Row, Spark type) pairs that
            are converted back into Arrow RecordBatches wrapped in a single
            struct column for the output stream.
            """

            def generate_data_batches() -> Iterator[Tuple[Any, Any]]:
                """
                Deserialize ArrowRecordBatches and return a generator of
                (grouping key, Row) tuples.

                This function must avoid materializing multiple Arrow
                RecordBatches into memory at the same time, and data chunks
                from the same grouping key should appear sequentially.
                """
                for batch in data:
                    DataRow = Row(*batch.schema.names)

                    # Iterate row by row without converting the whole batch.
                    num_cols = batch.num_columns
                    for row_idx in range(batch.num_rows):
                        row_key = tuple(batch[o][row_idx].as_py() for o in key_offsets)
                        row = DataRow(*(batch.column(i)[row_idx].as_py() for i in range(num_cols)))
                        yield row_key, row

            def convert_results(result_rows: Iterable[Any]) -> Iterator["pa.RecordBatch"]:
                # TODO(SPARK-XXXXX): add verification that elements in result_rows
                # are indeed of type Row and conform to assigned cols

                # Convert spark type to arrow type
                # TODO: we need to make this configurable, currently using default values.
                arrow_type = to_arrow_type(
                    return_type,
                    timezone="UTC",
                    prefers_large_types=False,
                )

                rows_as_dict = [row.asDict(True) for row in result_rows]

                pdf_schema = pa.schema(list(arrow_type))
                record_batch = pa.RecordBatch.from_pylist(rows_as_dict, schema=pdf_schema)
                yield ArrowBatchTransformer.wrap_struct(record_batch)

            for key, group in itertools.groupby(generate_data_batches(), key=lambda x: x[0]):
                # This must be a generator expression - do not materialize.
                values_gen = map(lambda x: x[1], group)
                yield from convert_results(
                    udf(
                        stateful_processor_api_client,
                        TransformWithStateInPandasFuncMode.PROCESS_DATA,
                        key,
                        values_gen,
                    )
                )

            yield from convert_results(
                udf(
                    stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.PROCESS_TIMER,
                    None,
                    iter([]),
                )
            )

            yield from convert_results(
                udf(
                    stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.COMPLETE,
                    None,
                    iter([]),
                )
            )

        return func, ser

    if eval_type == PythonEvalType.SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_INIT_STATE_UDF:
        import pyarrow as pa

        assert num_udfs == 1, "One TRANSFORM_WITH_STATE_PYTHON_ROW_INIT_STATE UDF expected here."
        udf, arg_offsets, return_type = udfs[0]

        # See TransformWithStateInPandasExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes.
        # parsed offsets:
        # [
        #     [groupingKeyOffsets, dedupDataOffsets],
        #     [initStateGroupingOffsets, dedupInitDataOffsets]
        # ]
        parsed_offsets = extract_key_value_indexes(arg_offsets)
        key_offsets = parsed_offsets[0][0]
        init_key_offsets = parsed_offsets[1][0]

        stateful_processor_api_client = StatefulProcessorApiClient(
            eval_conf.state_server_socket_port,
            eval_conf.grouping_key_schema,
            auth_secret=eval_conf.state_server_auth_secret,
        )

        arrow_max_records_per_batch = runner_conf.arrow_max_records_per_batch
        arrow_max_records_per_batch = (
            arrow_max_records_per_batch if arrow_max_records_per_batch > 0 else 2**31 - 1
        )

        def func(
            split_index: int,
            data: Iterator[pa.RecordBatch],
        ) -> Iterator[pa.RecordBatch]:
            """Apply transformWithStateInPySpark UDF with initial state over Rows.

            The input batches carry two struct columns, ``inputData`` and
            ``initState``; each batch holds one or the other but never both.
            Rows are flattened out of whichever struct is present, regrouped by
            grouping key, and re-chunked into Row lists bounded by
            arrow_max_records_per_batch. The UDF is invoked once per chunk with
            two separate iterators (data Rows and init-state Rows), then once for
            PROCESS_TIMER and once for COMPLETE. The UDF yields (iterator of Row,
            Spark type) pairs that are converted back into Arrow RecordBatches
            wrapped in a single struct column for the output stream.
            """

            def extract_rows(
                cur_batch: "pa.RecordBatch", col_name: str, offsets: list
            ) -> Optional[Iterator[Tuple[Any, Any]]]:
                data_column = cur_batch.column(cur_batch.schema.get_field_index(col_name))

                # Check if the entire column is null.
                if data_column.null_count == len(data_column):
                    return None

                data_field_names = [
                    data_column.type[i].name for i in range(data_column.type.num_fields)
                ]
                data_field_arrays = [
                    data_column.field(i) for i in range(data_column.type.num_fields)
                ]

                DataRow = Row(*data_field_names)

                table = pa.Table.from_arrays(data_field_arrays, names=data_field_names)

                if table.num_rows == 0:
                    return None

                def row_iterator() -> Iterator[Tuple[Any, Any]]:
                    for row_idx in range(table.num_rows):
                        key = tuple(table.column(o)[row_idx].as_py() for o in offsets)
                        row = DataRow(
                            *(table.column(i)[row_idx].as_py() for i in range(table.num_columns))
                        )
                        yield (key, row)

                return row_iterator()

            def row_stream() -> Iterator[Tuple[Any, Optional[Any], Optional[Any]]]:
                # The arrow batch is written in the schema:
                # schema: StructType = new StructType()
                #     .add("inputData", dataSchema)
                #     .add("initState", initStateSchema)
                # We parse each batch into tuples of (key, inputData, initState).
                # Each batch will have either init_data or input_data, not both.
                for batch in data:
                    input_result = extract_rows(batch, "inputData", key_offsets)
                    init_result = extract_rows(batch, "initState", init_key_offsets)

                    assert not (input_result is not None and init_result is not None)

                    if input_result is not None:
                        for key, input_data_row in input_result:
                            yield (key, input_data_row, None)
                    elif init_result is not None:
                        for key, init_state_row in init_result:
                            yield (key, None, init_state_row)

            def generate_data_batches() -> Iterator[Tuple[Any, Tuple[Any, Any]]]:
                """
                Deserialize ArrowRecordBatches and return a generator of
                (grouping key, (data Rows iterator, init-state Rows iterator))
                chunks bounded by arrow_max_records_per_batch.

                This function must avoid materializing multiple Arrow
                RecordBatches into memory at the same time, and data chunks
                from the same grouping key should appear sequentially.
                """
                for k, group_rows in itertools.groupby(row_stream(), key=lambda x: x[0]):
                    input_rows: list = []
                    init_rows: list = []

                    for _, input_row, init_row in group_rows:
                        if input_row is not None:
                            input_rows.append(input_row)
                        if init_row is not None:
                            init_rows.append(init_row)

                        total_len = len(input_rows) + len(init_rows)
                        if total_len >= arrow_max_records_per_batch:
                            yield (k, (iter(input_rows), iter(init_rows)))
                            input_rows = []
                            init_rows = []

                    if input_rows or init_rows:
                        yield (k, (iter(input_rows), iter(init_rows)))

            def convert_results(result_rows: Iterable[Any]) -> Iterator["pa.RecordBatch"]:
                # TODO(SPARK-XXXXX): add verification that elements in result_rows
                # are indeed of type Row and conform to assigned cols

                # Convert spark type to arrow type
                # TODO: we need to make this configurable, currently using default values.
                arrow_type = to_arrow_type(
                    return_type,
                    timezone="UTC",
                    prefers_large_types=False,
                )

                rows_as_dict = [row.asDict(True) for row in result_rows]

                pdf_schema = pa.schema(list(arrow_type))
                record_batch = pa.RecordBatch.from_pylist(rows_as_dict, schema=pdf_schema)
                yield ArrowBatchTransformer.wrap_struct(record_batch)

            for key, group in itertools.groupby(generate_data_batches(), key=lambda x: x[0]):
                # These must be generator expressions - do not materialize.
                for _, (values_gen, init_states_gen) in group:
                    yield from convert_results(
                        udf(
                            stateful_processor_api_client,
                            TransformWithStateInPandasFuncMode.PROCESS_DATA,
                            key,
                            values_gen,
                            init_states_gen,
                        )
                    )

            yield from convert_results(
                udf(
                    stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.PROCESS_TIMER,
                    None,
                    iter([]),
                    iter([]),
                )
            )

            yield from convert_results(
                udf(
                    stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.COMPLETE,
                    None,
                    iter([]),
                    iter([]),
                )
            )

        return func, ser

    elif eval_type == PythonEvalType.SQL_BATCHED_UDF:
        # Plain Python (pickle) UDFs, the only eval type reaching this branch. read_single_udf
        # prepared each UDF as an (arg_offsets, eval_func) pair. Apply every one to each input
        # row: a single UDF yields its bare result, multiple UDFs yield a tuple of results,
        # which is the shape the JVM side expects. num_udfs is fixed, so the single-result
        # case is handled once here rather than by unwrapping a one-element tuple per row.
        def func(split_index: int, data: Iterator[Any]) -> Iterator[Any]:
            if num_udfs == 1:
                arg_offsets, f = udfs[0]
                return (f(*[row[offset] for offset in arg_offsets]) for row in data)
            return (
                tuple(f(*[row[offset] for offset in arg_offsets]) for arg_offsets, f in udfs)
                for row in data
            )

        return func, ser

    else:
        raise ValueError("Unknown eval type: {}".format(eval_type))


def invoke_udf(message_receiver: SparkMessageReceiver, outfile: BinaryIO):
    """
    This function is the main processing function for worker.py.
    It receives messages from the JVM, processes the data, and sends back results.
    This method goes through three phases:

    Initialization -> Processing -> Finish/Cleanup
    """
    try:
        boot_time = time.time()
        # Initialization
        init_message = message_receiver.get_init_message()
        init_info = WorkerInitInfo.from_stream(init_message)
        del init_message

        start_faulthandler_periodic_traceback()
        check_python_version(init_info.python_version)

        memory_limit_mb = int(os.environ.get("PYSPARK_EXECUTOR_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

        TaskContext._setTaskContext(init_info.task_context.to_task_context())

        shuffle.MemoryBytesSpilled = 0
        shuffle.DiskBytesSpilled = 0

        setup_spark_files(init_info.spark_files_dir, init_info.python_includes)
        setup_broadcasts(
            init_info.broadcast.variables,
            init_info.broadcast.conn_info,
            init_info.broadcast.auth_secret,
        )

        _accumulatorRegistry.clear()
        eval_type = init_info.eval_type
        runner_conf = RunnerConf(init_info.runner_conf)
        eval_conf = EvalConf(init_info.eval_conf)
        # UDF and UDTF runners fold profiling into func at construction time (see
        # read_single_udf); only the classic RDD command carries a profiler of its own.
        profiler = None
        if eval_type == PythonEvalType.NON_UDF:
            assert isinstance(init_info.udf_info, (bytes, memoryview))
            func, profiler, deserializer, serializer = read_command(pickleSer, init_info.udf_info)
        else:
            # UDF and UDTF runners read input and write output through a single serializer.
            # If an eval type has a registered handler, dispatch through it: the handler
            # provides both the function and the serializer.
            handler_cls = get_eval_type_handler(eval_type, runner_conf)
            if handler_cls is not None:
                if isinstance(init_info.udf_info, UDTFInfo):
                    assert issubclass(handler_cls, (UDTFEvalTypeHandler, TableUDTFHandler))
                    udfs = [
                        read_single_udtf(
                            pickleSer, init_info.udf_info, handler_cls.partition_wrapper
                        )
                    ]
                else:
                    assert isinstance(init_info.udf_info, list)
                    udfs = [
                        read_single_udf(
                            pickleSer, udf_info, eval_type, runner_conf, udf_index=udf_index
                        )
                        for udf_index, udf_info in enumerate(init_info.udf_info)
                    ]
                handler = handler_cls(udfs=udfs, runner_conf=runner_conf, eval_conf=eval_conf)
                func, serializer = handler.run, handler.serializer
            else:
                func, serializer = read_udfs(
                    pickleSer, init_info.udf_info, eval_type, runner_conf, eval_conf
                )
            deserializer = serializer

        split_index = init_info.split_index
        del init_info

        init_time = time.time()

        # Processing

        # Fetch the input data stream
        input_data_stream = message_receiver.get_data_stream()

        def process():
            iterator = deserializer.load_stream(input_data_stream)
            out_iter = func(split_index, iterator)
            try:
                serializer.dump_stream(out_iter, outfile)
            finally:
                if hasattr(out_iter, "close"):
                    out_iter.close()

        def pipelined_process():
            """
            Pipelined variant of process() that pre-fetches input batches in a background
            reader thread while the main thread computes the UDF and writes output.
            This allows input deserialization to overlap with UDF computation.
            """
            import queue
            import threading

            queue_depth = int(os.environ.get("SPARK_PIPELINED_UDF_QUEUE_DEPTH", "2"))
            _SENTINEL = object()
            input_queue = queue.Queue(maxsize=queue_depth)
            reader_error = [None]
            # Event to signal the reader thread to stop (set by main thread on
            # exception or completion). The reader checks this after each failed
            # put attempt instead of polling with a timeout.
            stop_event = threading.Event()

            def _reader_thread():
                try:
                    for batch in deserializer.load_stream(input_data_stream):
                        # Some serializers (e.g., ArrowStreamGroupSerializer) yield lazy
                        # iterators that still read from the input stream. Materialize them here so
                        # the main thread can consume them without touching the stream.
                        if hasattr(batch, "__next__"):
                            batch = list(batch)
                        # Block on put, but wake up when stop_event is set.
                        # stop_event.wait() returns immediately if already set.
                        while not stop_event.is_set():
                            try:
                                input_queue.put(batch, timeout=0.1)
                                break
                            except queue.Full:
                                continue
                        if stop_event.is_set():
                            return
                except Exception as e:
                    reader_error[0] = e
                finally:
                    # Enqueue sentinel so the consumer knows we're done.
                    while not stop_event.is_set():
                        try:
                            input_queue.put(_SENTINEL, timeout=0.1)
                            break
                        except queue.Full:
                            continue

            t = threading.Thread(
                target=_reader_thread, name="pyspark-pipelined-reader", daemon=True
            )
            t.start()

            def _queued_iter():
                while True:
                    item = input_queue.get()
                    if item is _SENTINEL:
                        if reader_error[0] is not None:
                            raise reader_error[0]
                        return
                    yield item

            out_iter = func(split_index, _queued_iter())
            try:
                serializer.dump_stream(out_iter, outfile)
            finally:
                if hasattr(out_iter, "close"):
                    out_iter.close()
                # Signal reader thread to stop, drain the queue so it can unblock,
                # then wait for it to finish.
                stop_event.set()
                try:
                    while not input_queue.empty():
                        input_queue.get_nowait()
                except Exception:
                    pass
                # If the reader is still blocked in input_data_stream.read(), the stop_event
                # check only fires between put attempts -- it cannot interrupt a syscall.
                # Force-closing the stream here would break worker reuse (the next task uses
                # the same socket fd), so we settle for a bounded join and a loud warning
                # so an undetected leak shows up in the worker log.
                t.join(timeout=5)
                if t.is_alive():
                    warnings.warn(
                        "pipelined reader thread did not exit within 5s; "
                        "it may still be blocked in input_data_stream.read() and could "
                        "read data intended for a subsequent reused-worker task. "
                        "Consider disabling spark.python.worker.reuse if this recurs.",
                        RuntimeWarning,
                    )

        is_pipelined = os.environ.get("SPARK_PIPELINED_UDF") == "1"
        if is_pipelined and hasattr(serializer, "_flush_per_batch"):
            serializer._flush_per_batch = True
        run_process = pipelined_process if is_pipelined else process

        execution_start_time = time.time()
        with capture_outputs():
            if profiler:
                profiler.profile(run_process)
            else:
                run_process()
        execution_duration_ms = int(1000 * (time.time() - execution_start_time))

        # Cleanup
        # Reset task context to None. This is a guard code to avoid residual context when worker
        # reuse.
        TaskContext._setTaskContext(None)
        BarrierTaskContext._setTaskContext(None)
    except BaseException as e:
        handle_worker_exception(e, outfile)
        sys.exit(-1)
    finish_time = time.time()
    report_metrics(
        outfile,
        boot_time,
        init_time,
        finish_time,
        execution_duration_ms,
        shuffle.MemoryBytesSpilled,
        shuffle.DiskBytesSpilled,
    )

    # Mark the beginning of the accumulators section of the output
    write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
    send_accumulator_updates(outfile)

    # Check end of stream - raises if the finish signal is not received correctly.
    # Note: this call might fail due to other reasons (e.g. channel broke)
    # which will terminate the worker process.
    try:
        message_receiver.get_finish_signal_from_stream()
        write_int(SpecialLengths.END_OF_STREAM, outfile)
    except Exception:
        # Write a different value to tell JVM to not reuse this worker
        write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
        sys.exit(-1)


@with_faulthandler
def main(infile, outfile):
    # Instantiate socket message readers for executing the UDF
    socket_reader = SparkSocketMessageReceiver(infile)

    invoke_udf(socket_reader, outfile)


if __name__ == "__main__":
    with get_sock_file_to_executor() as sock_file:
        main(sock_file, sock_file)
