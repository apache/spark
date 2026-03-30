"""
SPARK-55278: Python UDF Worker Process

Standalone gRPC server that executes UDFs on behalf of a Spark executor.
Spawned by the executor's WorkerManager with auth token via env var.

Lifecycle:
  1. Executor spawns this process with SPARK_UDF_AUTH_TOKEN env var
  2. Worker starts gRPC server on assigned port (or port 0 for OS-assigned)
  3. Worker prints "WORKER_PORT=<port>" to stdout
  4. Executor calls RegisterWorker with the auth token
  5. Executor calls InitializeUdf to load a UDF
  6. Executor streams DataBatches via ExecuteUdf, worker returns results
  7. Executor calls Shutdown when done
"""

import os
import sys
import gc
import time
import signal
import logging
import argparse
import hashlib
import threading
import traceback
from concurrent import futures
from typing import Dict, Optional, Callable

import grpc
import pyarrow as pa
import pyarrow.ipc as ipc

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

from pyspark.sql.connect.proto import udf_protocol_pb2 as proto
from pyspark.sql.connect.proto import udf_protocol_pb2_grpc as proto_grpc

logger = logging.getLogger("spark.udf.worker")


class UdfRegistry:
    """Registry of loaded UDFs, keyed by udf_id."""

    def __init__(self):
        self._udfs: Dict[str, dict] = {}

    def register(self, udf_id: str, func: Callable, mode: int,
                 input_schema: pa.Schema, output_schema: pa.Schema):
        self._udfs[udf_id] = {
            "func": func,
            "mode": mode,
            "input_schema": input_schema,
            "output_schema": output_schema,
        }

    def get(self, udf_id: str) -> Optional[dict]:
        return self._udfs.get(udf_id)

    def remove(self, udf_id: str):
        self._udfs.pop(udf_id, None)

    def clear(self):
        self._udfs.clear()


class UdfWorkerServicer(proto_grpc.UdfWorkerServiceServicer):
    """Implementation of the UdfWorkerService gRPC service."""

    def __init__(self, auth_token: str, worker_id: str):
        self._auth_token = auth_token
        self._worker_id = worker_id
        self._session_token: Optional[str] = None
        self._registered = False
        self._registry = UdfRegistry()
        self._total_batches = 0
        self._total_rows = 0
        self._start_time = time.time()
        self._server: Optional[grpc.Server] = None

    def set_server(self, server: grpc.Server):
        self._server = server

    def RegisterWorker(self, request, context):
        # Validate auth token (SEC-01: one-time use)
        if request.auth_token != self._auth_token:
            return proto.RegisterWorkerResponse(
                status=proto.REJECTED,
                error_message="SEC-01: Invalid auth token",
            )
        if request.worker_id != self._worker_id:
            return proto.RegisterWorkerResponse(
                status=proto.REJECTED,
                error_message="SEC-02: Worker ID mismatch",
            )
        if self._registered:
            return proto.RegisterWorkerResponse(
                status=proto.REJECTED,
                error_message="Worker already registered",
            )

        self._registered = True
        # Clear auth token from memory after use (SEC-01)
        self._auth_token = None

        # Generate a simple session token (in production: use JWT)
        self._session_token = hashlib.sha256(
            f"{self._worker_id}-{time.time()}".encode()
        ).hexdigest()

        logger.info("Worker registered: %s", self._worker_id)
        return proto.RegisterWorkerResponse(
            status=proto.ACCEPTED,
            session_token=self._session_token,
        )

    def InitializeUdf(self, request, context):
        if not self._registered:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Worker not registered")
            return proto.InitializeUdfResponse(
                status=proto.PAYLOAD_ERROR,
                error_message="Worker not registered",
            )

        udf_id = request.udf_id
        udf_name = request.udf_name
        mode = request.mode

        # Deserialize input/output schemas from Arrow IPC bytes
        try:
            input_schema = _deserialize_arrow_schema(request.input_schema)
            output_schema = _deserialize_arrow_schema(request.output_schema)
        except Exception as e:
            return proto.InitializeUdfResponse(
                status=proto.SCHEMA_MISMATCH,
                error_message=f"Failed to deserialize schema: {e}",
            )

        # Verify payload integrity (SEC-22)
        payload = request.payload
        if request.payload_integrity_hash:
            actual_hash = _compute_payload_hash(payload)
            if actual_hash != request.payload_integrity_hash:
                return proto.InitializeUdfResponse(
                    status=proto.PAYLOAD_ERROR,
                    error_message="SEC-22: payload integrity check failed",
                )

        # Load the UDF function
        try:
            func = _load_udf(payload, udf_name)
        except Exception as e:
            return proto.InitializeUdfResponse(
                status=proto.PAYLOAD_ERROR,
                error_message=f"Failed to load UDF '{udf_name}': {e}",
                error_detail=traceback.format_exc(),
            )

        self._registry.register(udf_id, func, mode, input_schema, output_schema)

        # Serialize actual output schema
        actual_output_bytes = _serialize_arrow_schema(output_schema)

        logger.info("Initialized UDF: %s (%s) mode=%s", udf_name, udf_id, mode)
        return proto.InitializeUdfResponse(
            status=proto.OK,
            actual_output_schema=actual_output_bytes,
        )

    def ExecuteUdf(self, request_iterator, context):
        """Bidirectional streaming: receive DataBatches, return DataBatchResults."""
        total_batches = 0
        total_rows = 0
        total_time_ns = 0
        udf_id = None
        udf_entry = None

        for request in request_iterator:
            udf_id = request.udf_id
            msg_id = request.message_id

            if request.HasField("data_batch"):
                batch = request.data_batch

                # Resolve UDF entry once (first batch), reuse for subsequent
                if udf_entry is None:
                    udf_entry = self._registry.get(udf_id)
                    if udf_entry is None:
                        yield proto.ExecuteUdfResponse(
                            udf_id=udf_id,
                            message_id=msg_id,
                            error=proto.UdfError(
                                severity=proto.FATAL,
                                category=proto.UDF_EXECUTION,
                                message=f"UDF '{udf_id}' not initialized",
                                batch_id=batch.batch_id,
                            ),
                        )
                        return

                try:
                    start_ns = time.time_ns()
                    result_bytes, result_rows = _execute_batch(
                        udf_entry, batch.arrow_ipc_bytes
                    )
                    elapsed_ns = time.time_ns() - start_ns

                    total_batches += 1
                    total_rows += result_rows
                    total_time_ns += elapsed_ns

                    yield proto.ExecuteUdfResponse(
                        udf_id=udf_id,
                        message_id=msg_id,
                        data_batch_result=proto.DataBatchResult(
                            batch_id=batch.batch_id,
                            arrow_ipc_bytes=result_bytes,
                            num_rows=result_rows,
                            processing_time_ns=elapsed_ns,
                        ),
                    )
                except Exception as e:
                    logger.error("UDF execution failed: %s", e, exc_info=True)
                    yield proto.ExecuteUdfResponse(
                        udf_id=udf_id,
                        message_id=msg_id,
                        error=proto.UdfError(
                            severity=proto.FATAL,
                            category=proto.UDF_EXECUTION,
                            message=str(e),
                            stack_trace=traceback.format_exc(),
                            batch_id=batch.batch_id,
                            row_offset=-1,
                        ),
                    )
                    return

            elif request.HasField("end_of_data"):
                self._total_batches += total_batches
                self._total_rows += total_rows

                yield proto.ExecuteUdfResponse(
                    udf_id=udf_id,
                    message_id=msg_id,
                    end_of_data_result=proto.EndOfDataResult(
                        total_batches_processed=total_batches,
                        total_rows_processed=total_rows,
                        total_processing_time_ns=total_time_ns,
                    ),
                )
                return

            elif request.HasField("cancel"):
                logger.info("Execution cancelled: %s", request.cancel.reason)
                return

    def Heartbeat(self, request, context):
        if _HAS_PSUTIL:
            process = psutil.Process()
            mem = process.memory_info()
            rss = mem.rss
            vms = mem.vms
            cpu = process.cpu_percent()
        else:
            rss = vms = 0
            cpu = 0.0

        # Determine health status based on memory usage
        if rss > 0.9 * vms and vms > 0:
            status = proto.UNHEALTHY
        elif rss > 0.7 * vms and vms > 0:
            status = proto.DEGRADED
        else:
            status = proto.HEALTHY

        return proto.HeartbeatResponse(
            status=status,
            metrics=proto.WorkerMetrics(
                heap_used_bytes=rss,
                heap_total_bytes=vms,
                cpu_usage_percent=cpu,
                total_batches_processed=self._total_batches,
                total_rows_processed=self._total_rows,
                active_udf_count=len(self._registry._udfs),
            ),
        )

    def CleanupWorker(self, request, context):
        udf_id = request.udf_id
        self._registry.remove(udf_id)

        freed = 0
        if request.force_gc:
            before = _get_heap_size()
            gc.collect()
            after = _get_heap_size()
            freed = max(0, before - after)

        if request.clear_global_state:
            # Clear any module-level state that might have been
            # set by the UDF. In production, this would also
            # clear sys.modules entries loaded by the UDF.
            pass

        return proto.CleanupWorkerResponse(
            success=True,
            heap_bytes_freed=freed,
        )

    def Shutdown(self, request, context):
        logger.info("Shutdown requested: %s", proto.ShutdownReason.Name(request.reason))

        response = proto.ShutdownResponse(
            acknowledged=True,
            final_metrics=proto.WorkerMetrics(
                total_batches_processed=self._total_batches,
                total_rows_processed=self._total_rows,
            ),
        )

        # Schedule server stop after response is sent (avoid shutdown before reply)
        if self._server:
            grace = request.grace_period_ms / 1000.0
            threading.Timer(0.1, lambda: self._server.stop(grace=grace)).start()

        return response


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _deserialize_arrow_schema(schema_bytes: bytes) -> pa.Schema:
    """Deserialize Arrow IPC schema bytes to a PyArrow Schema."""
    if not schema_bytes:
        return pa.schema([])
    reader = ipc.open_stream(schema_bytes)
    return reader.schema


def _serialize_arrow_schema(schema: pa.Schema) -> bytes:
    """Serialize a PyArrow Schema to Arrow IPC bytes."""
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema)
    writer.close()
    return sink.getvalue().to_pybytes()


def _compute_payload_hash(payload) -> bytes:
    """Compute SHA-256 hash of the UDF payload for integrity verification."""
    h = hashlib.sha256()
    payload_type = payload.WhichOneof("payload_type")
    if payload_type == "serialized_function":
        h.update(payload.serialized_function)
    elif payload_type == "source_code":
        h.update(payload.source_code.encode("utf-8"))
    elif payload_type == "entry_point":
        h.update(payload.entry_point.encode("utf-8"))
    elif payload_type == "compiled_artifact":
        h.update(payload.compiled_artifact)
    return h.digest()


def _load_udf(payload, udf_name: str) -> Callable:
    """Load a UDF function from the payload.

    Supports three payload formats:
    - source: Python source code defining a function
    - entry_point: "module:function" reference
    - cloudpickle: Serialized function (deprecated, SEC-23)
    """
    payload_type = payload.WhichOneof("payload_type")
    fmt = payload.payload_format

    if payload_type == "source_code":
        # Execute the source code and extract the named function
        namespace = {}
        exec(payload.source_code, namespace)
        if udf_name not in namespace:
            raise ValueError(
                f"Source code does not define function '{udf_name}'. "
                f"Available: {[k for k in namespace if not k.startswith('_')]}"
            )
        return namespace[udf_name]

    elif payload_type == "entry_point":
        # Import module and get function
        entry = payload.entry_point
        if ":" not in entry:
            raise ValueError(
                f"entry_point must be 'module:function', got '{entry}'"
            )
        module_name, func_name = entry.rsplit(":", 1)
        import importlib
        mod = importlib.import_module(module_name)
        return getattr(mod, func_name)

    elif payload_type == "serialized_function":
        if fmt == "cloudpickle":
            # SEC-23: cloudpickle is deprecated but supported for backward compat
            import cloudpickle
            return cloudpickle.loads(payload.serialized_function)
        else:
            raise ValueError(f"Unsupported serialization format: {fmt}")

    else:
        raise ValueError(f"Unsupported payload type: {payload_type}")


def _execute_batch(udf_entry: dict, arrow_ipc_bytes: bytes) -> tuple:
    """Execute a UDF on an Arrow IPC batch.

    Returns (result_arrow_ipc_bytes, num_result_rows).
    """
    func = udf_entry["func"]
    mode = udf_entry["mode"]
    output_schema = udf_entry["output_schema"]

    # Deserialize input batch
    reader = ipc.open_stream(arrow_ipc_bytes)
    input_table = reader.read_all()

    # Execute UDF based on mode
    if mode == proto.UDF_MODE_SCALAR:
        # Scalar UDF: apply function row-by-row, result is single column
        # The function receives columnar data (as pandas Series) for efficiency
        import pandas as pd
        pdf = input_table.to_pandas()
        result_series = func(*[pdf[col] for col in pdf.columns])
        out_col = output_schema.names[0] if output_schema.names else "result"
        if isinstance(result_series, pd.Series):
            result_table = pa.Table.from_pandas(
                pd.DataFrame({out_col: result_series}),
                schema=output_schema,
            )
        else:
            result_table = pa.Table.from_pandas(
                pd.DataFrame({out_col: [result_series] * len(pdf)}),
                schema=output_schema,
            )

    elif mode == proto.UDF_MODE_VECTORIZED:
        # Vectorized UDF: function receives and returns Arrow RecordBatch
        result = func(input_table)
        if isinstance(result, pa.Table):
            result_table = result
        elif isinstance(result, pa.RecordBatch):
            result_table = pa.Table.from_batches([result])
        else:
            import pandas as pd
            if isinstance(result, pd.DataFrame):
                result_table = pa.Table.from_pandas(result, schema=output_schema)
            else:
                raise TypeError(
                    f"Vectorized UDF must return Table, RecordBatch, or DataFrame, "
                    f"got {type(result)}"
                )

    elif mode == proto.UDF_MODE_GROUPED_MAP:
        # Grouped Map UDF: function receives a pandas DataFrame for one group
        import pandas as pd
        pdf = input_table.to_pandas()
        result_pdf = func(pdf)
        result_table = pa.Table.from_pandas(result_pdf, schema=output_schema)

    elif mode == proto.UDF_MODE_TABLE:
        # Table UDF: function receives full table, returns variable-length output
        import pandas as pd
        pdf = input_table.to_pandas()
        result_pdf = func(pdf)
        result_table = pa.Table.from_pandas(result_pdf, schema=output_schema)

    else:
        raise ValueError(f"Unsupported UDF mode: {mode}")

    # Validate row count invariant for SCALAR and VECTORIZED modes
    if mode in (proto.UDF_MODE_SCALAR, proto.UDF_MODE_VECTORIZED):
        if result_table.num_rows != input_table.num_rows:
            raise ValueError(
                f"Row count mismatch: input had {input_table.num_rows} rows but "
                f"UDF returned {result_table.num_rows} rows. "
                f"SCALAR and VECTORIZED UDFs must preserve row count."
            )

    # Serialize result to Arrow IPC
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, result_table.schema)
    for batch in result_table.to_batches():
        writer.write_batch(batch)
    writer.close()

    return sink.getvalue().to_pybytes(), result_table.num_rows


def _get_heap_size() -> int:
    """Get current heap size in bytes."""
    if _HAS_PSUTIL:
        return psutil.Process().memory_info().rss
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def serve(port: int, worker_id: str) -> None:
    """Start the gRPC server."""
    # Read and clear auth token (SEC-01)
    auth_token = os.environ.get("SPARK_UDF_AUTH_TOKEN")
    if not auth_token:
        logger.error("SEC-01: SPARK_UDF_AUTH_TOKEN not set")
        sys.exit(1)
    os.environ.pop("SPARK_UDF_AUTH_TOKEN", None)

    servicer = UdfWorkerServicer(auth_token, worker_id)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    proto_grpc.add_UdfWorkerServiceServicer_to_server(servicer, server)

    if port == 0:
        # OS-assigned port (SEC-27: eliminates TOCTOU race)
        actual_port = server.add_insecure_port("[::]:0")
    else:
        actual_port = server.add_insecure_port(f"[::]:{port}")

    servicer.set_server(server)
    server.start()

    # Print port for executor to read (SEC-27)
    print(f"WORKER_PORT={actual_port}", flush=True)
    logger.info("Worker %s listening on port %d", worker_id, actual_port)

    # Handle SIGTERM gracefully
    def _handle_sigterm(signum, frame):
        logger.info("Received SIGTERM, shutting down...")
        server.stop(grace=5.0)

    signal.signal(signal.SIGTERM, _handle_sigterm)

    server.wait_for_termination()
    logger.info("Worker %s terminated", worker_id)


def main():
    parser = argparse.ArgumentParser(description="Spark UDF Worker")
    parser.add_argument("--port", type=int, default=0,
                        help="Port to listen on (0 for OS-assigned)")
    parser.add_argument("--worker-id", type=str, required=True,
                        help="Unique worker ID assigned by executor")
    parser.add_argument("--log-level", type=str, default="INFO",
                        help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    serve(args.port, args.worker_id)


if __name__ == "__main__":
    main()
