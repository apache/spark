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
Python worker that runs one task at a time. ``invoke_udf`` runs a task against any
``SparkMessageReceiver``, so it does not depend on how the worker was started; ``main`` drives
it over the socket of a worker launched by the JVM's PythonRunner, directly or forked by
pyspark.daemon. A task runs either a classic RDD command (eval type NON_UDF) or SQL UDFs/UDTFs
through the handler registered for their eval type in pyspark.eval_handlers.
"""

import json
import os
import sys
import warnings
from typing import BinaryIO

from pyspark import shuffle
from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import EvalTypeHandler, get_eval_type_handler
from pyspark.eval_handlers._typing import UDF, UDTF
from pyspark.eval_handlers._udtf import UDTFHandlerBase
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
from pyspark.sql.types import StructType, _parse_datatype_json_string
from pyspark.taskcontext import BarrierTaskContext, TaskContext
from pyspark.util import (
    PythonEvalType,
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


def invoke_udf(message_receiver: SparkMessageReceiver, outfile: BinaryIO):
    """
    This function is the main processing function for worker.py.
    It reads the task's messages from message_receiver, processes the data, and writes the
    results to outfile.
    This method goes through three phases:

    Initialization -> Processing -> Finish/Cleanup
    """
    try:
        metrics = WorkerMetrics()
        metrics.reset()
        metrics.set_current_timestamp("bootTimestampMs")
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
        # Only the classic RDD command carries a profiler of its own. UDF runners fold profiling
        # into func at construction time (see EvalTypeHandler.__init__); UDTF runners are not
        # profiled.
        profiler = None
        if eval_type == PythonEvalType.NON_UDF:
            assert isinstance(init_info.udf_info, (bytes, memoryview))
            func, profiler, deserializer, serializer = read_command(pickleSer, init_info.udf_info)
        else:
            # UDF and UDTF runners dispatch through the eval type's registered handler, which
            # provides both the function and the single serializer for input and output.
            handler_cls = get_eval_type_handler(eval_type, runner_conf)
            if handler_cls is None:
                raise ValueError("Unknown eval type: {}".format(eval_type))
            handler: EvalTypeHandler
            if isinstance(init_info.udf_info, UDTFInfo):
                assert issubclass(handler_cls, UDTFHandlerBase)
                udtf_cls = read_command(pickleSer, init_info.udf_info.handler)
                return_type = _parse_datatype_json_string(init_info.udf_info.return_type)
                if not isinstance(return_type, StructType):
                    raise PySparkRuntimeError(
                        "The return type of a UDTF must be a struct type, but got "
                        f"{type(return_type)}."
                    )
                if init_info.udf_info.pickled_analyze_result is not None:
                    analyze_result = pickleSer.loads(init_info.udf_info.pickled_analyze_result)
                else:
                    analyze_result = None
                udtf = UDTF(
                    udtf_cls=udtf_cls,
                    args_offsets=init_info.udf_info.args,
                    kwargs_offsets=init_info.udf_info.kwargs,
                    return_type=return_type,
                    partition_child_indexes=init_info.udf_info.partition_child_indexes,
                    analyze_result=analyze_result,
                    name=init_info.udf_info.name,
                )
                handler = handler_cls(
                    udf_descs=[udtf], runner_conf=runner_conf, eval_conf=eval_conf
                )
            else:
                assert isinstance(init_info.udf_info, list)
                udf_descs = []
                for udf_info in init_info.udf_info:
                    # The functions applied in turn to one column; the last one's return type
                    # is the UDF's.
                    funcs, return_types = zip(*(read_command(pickleSer, c) for c in udf_info.udfs))
                    udf_descs.append(
                        UDF(
                            funcs=list(funcs),
                            args_offsets=udf_info.args,
                            kwargs_offsets=udf_info.kwargs,
                            return_type=return_types[-1],
                            result_id=udf_info.result_id,
                        )
                    )
                handler = handler_cls(
                    udf_descs=udf_descs, runner_conf=runner_conf, eval_conf=eval_conf
                )
            func, serializer = handler.run, handler.serializer
            deserializer = serializer

        split_index = init_info.split_index
        del init_info

        metrics.set_current_timestamp("initTimestampMs")

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
        # Pipelined reads can overlap UDF work, so mark these task reports for filtering.
        metrics.set("pythonNumPipelinedTasks", int(is_pipelined))
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
    metrics.set_current_timestamp("finishTimestampMs")
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
