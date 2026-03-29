"""
Tests for SPARK-55278 Python UDF Worker.

Tests the worker servicer, UDF loading, Arrow IPC serde, and batch execution
without requiring a running gRPC server.
"""

import gc
import hashlib
import unittest
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.ipc as ipc
import pandas as pd

from pyspark.sql.connect.proto import udf_protocol_pb2 as proto
from pyspark.sql.worker.udf_worker import (
    UdfWorkerServicer,
    UdfRegistry,
    _deserialize_arrow_schema,
    _serialize_arrow_schema,
    _compute_payload_hash,
    _load_udf,
    _execute_batch,
)


def _make_arrow_ipc_bytes(table: pa.Table) -> bytes:
    """Helper: serialize a PyArrow Table to Arrow IPC bytes."""
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, table.schema)
    for batch in table.to_batches():
        writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


class TestUdfRegistry(unittest.TestCase):
    def test_register_and_get(self):
        registry = UdfRegistry()
        schema = pa.schema([("x", pa.int64())])
        registry.register("udf1", lambda x: x, proto.UDF_MODE_SCALAR, schema, schema)
        entry = registry.get("udf1")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["mode"], proto.UDF_MODE_SCALAR)

    def test_get_missing(self):
        registry = UdfRegistry()
        self.assertIsNone(registry.get("nonexistent"))

    def test_remove(self):
        registry = UdfRegistry()
        schema = pa.schema([("x", pa.int64())])
        registry.register("udf1", lambda x: x, proto.UDF_MODE_SCALAR, schema, schema)
        registry.remove("udf1")
        self.assertIsNone(registry.get("udf1"))

    def test_clear(self):
        registry = UdfRegistry()
        schema = pa.schema([("x", pa.int64())])
        registry.register("udf1", lambda x: x, proto.UDF_MODE_SCALAR, schema, schema)
        registry.register("udf2", lambda x: x, proto.UDF_MODE_SCALAR, schema, schema)
        registry.clear()
        self.assertIsNone(registry.get("udf1"))
        self.assertIsNone(registry.get("udf2"))


class TestArrowSerde(unittest.TestCase):
    def test_schema_round_trip(self):
        schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.utf8()),
            ("value", pa.float64()),
        ])
        serialized = _serialize_arrow_schema(schema)
        self.assertIsInstance(serialized, bytes)
        self.assertGreater(len(serialized), 0)

        recovered = _deserialize_arrow_schema(serialized)
        self.assertEqual(recovered, schema)

    def test_empty_schema_bytes(self):
        schema = _deserialize_arrow_schema(b"")
        self.assertEqual(len(schema), 0)

    def test_complex_schema_round_trip(self):
        schema = pa.schema([
            ("ids", pa.list_(pa.int64())),
            ("flag", pa.bool_()),
            ("data", pa.binary()),
        ])
        recovered = _deserialize_arrow_schema(_serialize_arrow_schema(schema))
        self.assertEqual(recovered, schema)


class TestPayloadHash(unittest.TestCase):
    def test_source_code_hash(self):
        payload = proto.UdfPayload(
            source_code="def my_func(x): return x * 2",
            payload_format="source",
        )
        h = _compute_payload_hash(payload)
        expected = hashlib.sha256(b"def my_func(x): return x * 2").digest()
        self.assertEqual(h, expected)

    def test_serialized_function_hash(self):
        payload = proto.UdfPayload(
            serialized_function=b"\x80\x04\x95",
            payload_format="cloudpickle",
        )
        h = _compute_payload_hash(payload)
        expected = hashlib.sha256(b"\x80\x04\x95").digest()
        self.assertEqual(h, expected)

    def test_entry_point_hash(self):
        payload = proto.UdfPayload(
            entry_point="my_module:my_func",
            payload_format="entry_point",
        )
        h = _compute_payload_hash(payload)
        expected = hashlib.sha256(b"my_module:my_func").digest()
        self.assertEqual(h, expected)


class TestLoadUdf(unittest.TestCase):
    def test_load_source_code(self):
        payload = proto.UdfPayload(
            source_code="def double_it(x): return x * 2",
            payload_format="source",
        )
        func = _load_udf(payload, "double_it")
        self.assertEqual(func(21), 42)

    def test_load_source_code_missing_function(self):
        payload = proto.UdfPayload(
            source_code="def other_func(x): return x",
            payload_format="source",
        )
        with self.assertRaises(ValueError) as ctx:
            _load_udf(payload, "missing_func")
        self.assertIn("missing_func", str(ctx.exception))

    def test_load_entry_point_invalid_format(self):
        payload = proto.UdfPayload(
            entry_point="no_colon_here",
            payload_format="entry_point",
        )
        with self.assertRaises(ValueError) as ctx:
            _load_udf(payload, "func")
        self.assertIn("module:function", str(ctx.exception))

    def test_load_entry_point_builtin(self):
        """Test loading a function from a real module."""
        payload = proto.UdfPayload(
            entry_point="math:sqrt",
            payload_format="entry_point",
        )
        func = _load_udf(payload, "sqrt")
        self.assertAlmostEqual(func(4.0), 2.0)


class TestExecuteBatch(unittest.TestCase):
    def _make_udf_entry(self, func, mode, input_schema, output_schema):
        return {
            "func": func,
            "mode": mode,
            "input_schema": input_schema,
            "output_schema": output_schema,
        }

    def test_scalar_udf(self):
        """Scalar UDF: doubles each value."""
        def double_col(x):
            return x * 2

        input_schema = pa.schema([("value", pa.int64())])
        output_schema = pa.schema([("result", pa.int64())])

        table = pa.table({"value": [1, 2, 3, 4, 5]})
        ipc_bytes = _make_arrow_ipc_bytes(table)

        entry = self._make_udf_entry(double_col, proto.UDF_MODE_SCALAR, input_schema, output_schema)
        result_bytes, num_rows = _execute_batch(entry, ipc_bytes)

        self.assertEqual(num_rows, 5)
        result = ipc.open_stream(result_bytes).read_all()
        self.assertEqual(result.column("result").to_pylist(), [2, 4, 6, 8, 10])

    def test_vectorized_udf_returns_table(self):
        """Vectorized UDF: receives and returns a PyArrow Table."""
        def add_computed(table):
            col = table.column("x").to_pylist()
            return pa.table({"x": col, "x_squared": [v ** 2 for v in col]})

        input_schema = pa.schema([("x", pa.int64())])
        output_schema = pa.schema([("x", pa.int64()), ("x_squared", pa.int64())])

        table = pa.table({"x": [2, 3, 4]})
        ipc_bytes = _make_arrow_ipc_bytes(table)

        entry = self._make_udf_entry(add_computed, proto.UDF_MODE_VECTORIZED, input_schema, output_schema)
        result_bytes, num_rows = _execute_batch(entry, ipc_bytes)

        self.assertEqual(num_rows, 3)
        result = ipc.open_stream(result_bytes).read_all()
        self.assertEqual(result.column("x_squared").to_pylist(), [4, 9, 16])

    def test_vectorized_udf_returns_dataframe(self):
        """Vectorized UDF returning a pandas DataFrame."""
        def transform(table):
            pdf = table.to_pandas()
            pdf["doubled"] = pdf["val"] * 2
            return pdf

        input_schema = pa.schema([("val", pa.int64())])
        output_schema = pa.schema([("val", pa.int64()), ("doubled", pa.int64())])

        table = pa.table({"val": [10, 20, 30]})
        ipc_bytes = _make_arrow_ipc_bytes(table)

        entry = self._make_udf_entry(transform, proto.UDF_MODE_VECTORIZED, input_schema, output_schema)
        result_bytes, num_rows = _execute_batch(entry, ipc_bytes)

        self.assertEqual(num_rows, 3)
        result = ipc.open_stream(result_bytes).read_all()
        self.assertEqual(result.column("doubled").to_pylist(), [20, 40, 60])

    def test_grouped_map_udf(self):
        """Grouped Map UDF: receives a group as DataFrame, returns DataFrame."""
        def normalize(pdf):
            pdf["normalized"] = pdf["value"] - pdf["value"].mean()
            return pdf

        input_schema = pa.schema([("key", pa.utf8()), ("value", pa.float64())])
        output_schema = pa.schema([
            ("key", pa.utf8()), ("value", pa.float64()), ("normalized", pa.float64()),
        ])

        table = pa.table({"key": ["a", "a", "a"], "value": [1.0, 2.0, 3.0]})
        ipc_bytes = _make_arrow_ipc_bytes(table)

        entry = self._make_udf_entry(normalize, proto.UDF_MODE_GROUPED_MAP, input_schema, output_schema)
        result_bytes, num_rows = _execute_batch(entry, ipc_bytes)

        self.assertEqual(num_rows, 3)
        result = ipc.open_stream(result_bytes).read_all()
        normalized = result.column("normalized").to_pylist()
        self.assertAlmostEqual(normalized[0], -1.0)
        self.assertAlmostEqual(normalized[1], 0.0)
        self.assertAlmostEqual(normalized[2], 1.0)

    def test_table_udf_variable_output(self):
        """Table UDF: output may have different row count than input."""
        def explode_rows(pdf):
            return pd.DataFrame({"value": [v for v in pdf["value"] for _ in range(2)]})

        input_schema = pa.schema([("value", pa.int64())])
        output_schema = pa.schema([("value", pa.int64())])

        table = pa.table({"value": [1, 2, 3]})
        ipc_bytes = _make_arrow_ipc_bytes(table)

        entry = self._make_udf_entry(explode_rows, proto.UDF_MODE_TABLE, input_schema, output_schema)
        result_bytes, num_rows = _execute_batch(entry, ipc_bytes)

        self.assertEqual(num_rows, 6)  # 3 * 2
        result = ipc.open_stream(result_bytes).read_all()
        self.assertEqual(result.column("value").to_pylist(), [1, 1, 2, 2, 3, 3])

    def test_empty_batch(self):
        """Handle an empty input batch."""
        def identity(x):
            return x

        input_schema = pa.schema([("x", pa.int64())])
        output_schema = pa.schema([("result", pa.int64())])

        table = pa.table({"x": pa.array([], type=pa.int64())})
        ipc_bytes = _make_arrow_ipc_bytes(table)

        entry = self._make_udf_entry(identity, proto.UDF_MODE_SCALAR, input_schema, output_schema)
        result_bytes, num_rows = _execute_batch(entry, ipc_bytes)
        self.assertEqual(num_rows, 0)

    def test_large_batch(self):
        """Process a large batch (10K rows)."""
        def double_col(x):
            return x * 2

        n = 10000
        table = pa.table({"value": list(range(n))})
        ipc_bytes = _make_arrow_ipc_bytes(table)

        entry = self._make_udf_entry(
            double_col, proto.UDF_MODE_SCALAR,
            pa.schema([("value", pa.int64())]),
            pa.schema([("result", pa.int64())]),
        )
        result_bytes, num_rows = _execute_batch(entry, ipc_bytes)
        self.assertEqual(num_rows, n)

        result = ipc.open_stream(result_bytes).read_all()
        self.assertEqual(result.column("result")[0].as_py(), 0)
        self.assertEqual(result.column("result")[n - 1].as_py(), (n - 1) * 2)


class TestWorkerServicer(unittest.TestCase):
    """Test the UdfWorkerServicer directly (no gRPC transport)."""

    def _make_servicer(self):
        return UdfWorkerServicer(
            auth_token="test-token-123",
            worker_id="worker-abc",
        )

    def _mock_context(self):
        ctx = MagicMock()
        return ctx

    def test_register_worker_success(self):
        servicer = self._make_servicer()
        request = proto.RegisterWorkerRequest(
            worker_id="worker-abc",
            auth_token="test-token-123",
            capabilities=proto.WorkerCapabilities(
                language="python",
                language_version="3.11",
                sdk_version="1.0.0",
                protocol_version=1,
                supports_arrow_ipc=True,
            ),
        )
        response = servicer.RegisterWorker(request, self._mock_context())
        self.assertEqual(response.status, proto.ACCEPTED)
        self.assertTrue(len(response.session_token) > 0)

    def test_register_worker_wrong_token(self):
        servicer = self._make_servicer()
        request = proto.RegisterWorkerRequest(
            worker_id="worker-abc",
            auth_token="wrong-token",
        )
        response = servicer.RegisterWorker(request, self._mock_context())
        self.assertEqual(response.status, proto.REJECTED)
        self.assertIn("SEC-01", response.error_message)

    def test_register_worker_wrong_id(self):
        servicer = self._make_servicer()
        request = proto.RegisterWorkerRequest(
            worker_id="wrong-worker-id",
            auth_token="test-token-123",
        )
        response = servicer.RegisterWorker(request, self._mock_context())
        self.assertEqual(response.status, proto.REJECTED)
        self.assertIn("SEC-02", response.error_message)

    def test_register_worker_double_registration(self):
        servicer = self._make_servicer()
        request = proto.RegisterWorkerRequest(
            worker_id="worker-abc",
            auth_token="test-token-123",
        )
        servicer.RegisterWorker(request, self._mock_context())
        # Second registration should fail
        response = servicer.RegisterWorker(request, self._mock_context())
        self.assertEqual(response.status, proto.REJECTED)

    def test_initialize_udf_source_code(self):
        servicer = self._make_servicer()
        # Register first
        servicer.RegisterWorker(
            proto.RegisterWorkerRequest(
                worker_id="worker-abc", auth_token="test-token-123"
            ),
            self._mock_context(),
        )

        input_schema = pa.schema([("x", pa.int64())])
        output_schema = pa.schema([("result", pa.int64())])

        request = proto.InitializeUdfRequest(
            udf_id="udf-001",
            udf_name="my_double",
            mode=proto.UDF_MODE_SCALAR,
            input_schema=_serialize_arrow_schema(input_schema),
            output_schema=_serialize_arrow_schema(output_schema),
            payload=proto.UdfPayload(
                source_code="def my_double(x): return x * 2",
                payload_format="source",
            ),
        )
        response = servicer.InitializeUdf(request, self._mock_context())
        self.assertEqual(response.status, proto.OK)

    def test_initialize_udf_bad_payload(self):
        servicer = self._make_servicer()
        servicer.RegisterWorker(
            proto.RegisterWorkerRequest(
                worker_id="worker-abc", auth_token="test-token-123"
            ),
            self._mock_context(),
        )

        request = proto.InitializeUdfRequest(
            udf_id="udf-bad",
            udf_name="missing_func",
            mode=proto.UDF_MODE_SCALAR,
            input_schema=_serialize_arrow_schema(pa.schema([("x", pa.int64())])),
            output_schema=_serialize_arrow_schema(pa.schema([("r", pa.int64())])),
            payload=proto.UdfPayload(
                source_code="def other_name(x): return x",
                payload_format="source",
            ),
        )
        response = servicer.InitializeUdf(request, self._mock_context())
        self.assertEqual(response.status, proto.PAYLOAD_ERROR)
        self.assertIn("missing_func", response.error_message)

    def test_cleanup_worker(self):
        servicer = self._make_servicer()
        servicer.RegisterWorker(
            proto.RegisterWorkerRequest(
                worker_id="worker-abc", auth_token="test-token-123"
            ),
            self._mock_context(),
        )

        request = proto.CleanupWorkerRequest(
            udf_id="udf-001",
            force_gc=True,
            clear_global_state=True,
        )
        response = servicer.CleanupWorker(request, self._mock_context())
        self.assertTrue(response.success)

    def test_shutdown(self):
        import time as _time
        servicer = self._make_servicer()
        mock_server = MagicMock()
        servicer.set_server(mock_server)

        request = proto.ShutdownRequest(
            reason=proto.IDLE_TIMEOUT,
            grace_period_ms=5000,
        )
        response = servicer.Shutdown(request, self._mock_context())
        self.assertTrue(response.acknowledged)
        # stop is deferred via threading.Timer(0.1, ...)
        _time.sleep(0.2)
        mock_server.stop.assert_called_once_with(grace=5.0)


if __name__ == "__main__":
    unittest.main()
