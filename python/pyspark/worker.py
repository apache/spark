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
import json
import os
import sys
import time
import warnings
from typing import Any, BinaryIO, Optional

from pyspark import shuffle
from pyspark.accumulators import (
    SpecialAccumulatorIds,
    _accumulatorRegistry,
    _deserialize_accumulator,
)
from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import get_eval_type_handler
from pyspark.eval_handlers._udtf import TableUDTFHandler, UDTFEvalTypeHandler
from pyspark.eval_handlers.utils import wrap_kwargs_support
from pyspark.logger.worker_io import capture_outputs
from pyspark.messages import (
    SparkMessageReceiver,
    SparkSocketMessageReceiver,
)
from pyspark.serializers import (
    SpecialLengths,
    write_int,
    write_with_length,
)
from pyspark.sql.types import (
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
from pyspark.worker_metrics import WorkerMetrics
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


def report_metrics(outfile: BinaryIO, metrics: dict[str, int]) -> None:
    """Write the worker metrics as a length-prefixed JSON report after METRICS_DATA."""
    payload = json.dumps(metrics, separators=(",", ":")).encode("utf-8")
    write_int(SpecialLengths.METRICS_DATA, outfile)
    write_with_length(payload, outfile)


def chain(f, g):
    """chain two functions together"""
    return lambda *a: g(f(*a))


def wrap_perf_profiler(f, iter_based, result_id):
    from pyspark.sql.profiler import ProfileResultsParam, ProfileResultsParamV2, WorkerPerfProfiler

    accumulator = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
    )
    accumulator_v2 = _deserialize_accumulator(
        SpecialAccumulatorIds.SQL_UDF_PROFIER_V2, {}, ProfileResultsParamV2
    )

    if iter_based:

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


def wrap_memory_profiler(f, iter_based, result_id):
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

    if iter_based:

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


def read_single_udf(pickleSer, udf_info, handler_cls, runner_conf):
    eval_type = handler_cls.eval_type
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
        profiling_func = wrap_perf_profiler(
            chained_func, handler_cls.is_iter_based, udf_info.result_id
        )
    elif profiler == "memory":
        profiling_func = wrap_memory_profiler(
            chained_func, handler_cls.is_iter_based, udf_info.result_id
        )
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
# the constructor to receive the AnalyzeResult, instantiate the UDTF (wrapped in the handler's
# partition_wrapper when its TABLE argument has PARTITION BY expressions), and check the 'eval'
# signature against the call arguments. Returns the same
# (func, args_offsets, kwargs_offsets, return_type) shape as read_single_udf, with the UDTF
# instance in the func slot.
def read_single_udtf(pickleSer, udtf_info, handler_cls):
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
            udtf = handler_cls.partition_wrapper(handler, udtf_info.partition_child_indexes)
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
            metrics = WorkerMetrics()
            assert isinstance(init_info.udf_info, (bytes, memoryview))
            func, profiler, deserializer, serializer = read_command(pickleSer, init_info.udf_info)
        else:
            # UDF and UDTF runners dispatch through the eval type's registered handler, which
            # provides both the function and the single serializer for input and output.
            handler_cls = get_eval_type_handler(eval_type, runner_conf)
            if handler_cls is None:
                raise ValueError("Unknown eval type: {}".format(eval_type))
            if isinstance(init_info.udf_info, UDTFInfo):
                assert issubclass(handler_cls, (UDTFEvalTypeHandler, TableUDTFHandler))
                udfs = [read_single_udtf(pickleSer, init_info.udf_info, handler_cls)]
            else:
                assert isinstance(init_info.udf_info, list)
                udfs = [
                    read_single_udf(pickleSer, udf_info, handler_cls, runner_conf)
                    for udf_info in init_info.udf_info
                ]
            handler = handler_cls(udfs=udfs, runner_conf=runner_conf, eval_conf=eval_conf)
            metrics = handler.metrics
            func, serializer = handler.run, handler.serializer
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

        with metrics.measure("pythonExecutionDurationMs"), capture_outputs():
            if profiler:
                profiler.profile(run_process)
            else:
                run_process()

        # Cleanup
        # Reset task context to None. This is a guard code to avoid residual context when worker
        # reuse.
        TaskContext._setTaskContext(None)
        BarrierTaskContext._setTaskContext(None)
    except BaseException as e:
        handle_worker_exception(e, outfile)
        sys.exit(-1)
    finish_time = time.time()
    # Lifecycle timestamps are epoch milliseconds, matching the JVM's clock for calculations.
    metrics.set("bootTimestampMs", int(1000 * boot_time))
    metrics.set("initTimestampMs", int(1000 * init_time))
    metrics.set("finishTimestampMs", int(1000 * finish_time))
    # Spill totals are byte counts already expressed in their reporting unit.
    metrics.set("memoryBytesSpilled", shuffle.MemoryBytesSpilled)
    metrics.set("diskBytesSpilled", shuffle.DiskBytesSpilled)
    report_metrics(outfile, metrics.to_dict())

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
