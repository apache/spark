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
import datetime
import json
import os
import select
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from decimal import Decimal, InvalidOperation
from typing import List
from unittest import mock

from pyspark.errors import PySparkException, PySparkValueError
from pyspark.sql import Row
from pyspark.sql.datasource import DataSourceLookupReader
from pyspark.sql.types import (
    BinaryType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.testing.sqlutils import SPARK_HOME, ReusedSQLTestCase
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message

if have_pyarrow:
    import pyarrow as pa

# The events of the lookup readers of the worker tests. The readers are classes of this module,
# which the worker unpickles by reference, so they append to this list in the same process.
events: List[str] = []


class EchoLookupReader(DataSourceLookupReader):
    """
    Returns two rows for each parameter row whose first field is even, a named `Row` for the
    others, and records its calls in `events`.
    """

    def __init__(self) -> None:
        self.calls = 0

    def selectLookupPredicates(self, candidates):
        return range(len(candidates))

    def lookup(self, parameters):
        from pyspark import TaskContext

        self.calls += 1
        events.append(
            f"lookup {self.calls}: {parameters} in partition {TaskContext.get().partitionId()}"
        )
        try:
            for ordinal, (key, *_) in enumerate(parameters):
                if key is None:
                    continue
                if key % 2 == 0:
                    yield ordinal, (key, f"first {key}")
                    yield ordinal, [key, f"second {key}"]
                else:
                    yield ordinal, Row(name=f"named {key}", id=key)
        finally:
            events.append("finally")

    def close(self):
        events.append("close")


class OrdinalLookupReader(DataSourceLookupReader):
    """Returns one row, with the fixed ordinal given to its constructor."""

    def __init__(self, ordinal) -> None:
        self.ordinal = ordinal

    def selectLookupPredicates(self, candidates):
        return range(len(candidates))

    def lookup(self, parameters):
        try:
            yield self.ordinal, (1, "one")
        finally:
            events.append("finally")


class PairLookupReader(DataSourceLookupReader):
    """Returns the given item as the result of a lookup."""

    def __init__(self, item) -> None:
        self.item = item

    def selectLookupPredicates(self, candidates):
        return range(len(candidates))

    def lookup(self, parameters):
        yield self.item


class EndlessLookupReader(DataSourceLookupReader):
    """Returns rows until Spark stops it, then records the number of rows it yielded."""

    def selectLookupPredicates(self, candidates):
        return range(len(candidates))

    def lookup(self, parameters):
        yielded = 0
        try:
            while True:
                yield 0, (yielded, "row")
                yielded += 1
        finally:
            events.append(f"finally {yielded}")


class ListLookupReader(DataSourceLookupReader):
    """Returns a list, which is not an iterator."""

    def selectLookupPredicates(self, candidates):
        return range(len(candidates))

    def lookup(self, parameters):
        return [(0, (1, "one"))]


class BatchLookupReader(DataSourceLookupReader):
    """
    Returns the results that `make_results(parameters)` builds, `pyarrow.RecordBatch`es or
    other values, and records its calls in `events`.
    """

    def __init__(self, make_results) -> None:
        self.make_results = make_results

    def selectLookupPredicates(self, candidates):
        return range(len(candidates))

    def lookup(self, parameters):
        events.append(f"lookup {parameters}")
        try:
            yield from self.make_results(parameters)
        finally:
            events.append("finally")


class ListPairsLookupReader(DataSourceLookupReader):
    """Returns the given items as the results of a lookup."""

    def __init__(self, items) -> None:
        self.items = items

    def selectLookupPredicates(self, candidates):
        return range(len(candidates))

    def lookup(self, parameters):
        yield from self.items


class DocExampleLookupReader(DataSourceLookupReader):
    """The RecordBatch example of `DataSourceLookupReader.lookup`, over a dictionary."""

    def __init__(self, rows) -> None:
        self.rows = rows

    def selectLookupPredicates(self, candidates):
        return range(len(candidates))

    def lookup(self, parameters):
        import pyarrow as pa

        ordinals, user_ids, names = [], [], []
        for ordinal, (user_id,) in enumerate(parameters):
            for row in self.rows.get(user_id, []):
                ordinals.append(ordinal)
                user_ids.append(row[0])
                names.append(row[1])
        yield pa.RecordBatch.from_arrays(
            [
                pa.array(ordinals, pa.int32()),
                pa.array(user_ids, pa.int64()),
                pa.array(names, pa.string()),
            ],
            names=["ordinal", "user_id", "name"],
        )


READ_SCHEMA = StructType([StructField("id", LongType()), StructField("name", StringType())])
USER_SCHEMA = StructType([StructField("user_id", LongType()), StructField("name", StringType())])


class LookupWorkerClient:
    """
    Runs `data_source_lookup._main` in a thread, over a socket pair, and plays the JVM's side of
    the protocol of `PythonExternalLookupProtocol`. A socket pair is needed because the worker
    checks for a cancel with `select`.
    """

    def __init__(
        self,
        reader,
        param_schema: StructType,
        read_schema: StructType = READ_SCHEMA,
        max_arrow_batch_size: int = 1000,
        binary_as_bytes: bool = True,
        tcp: bool = False,
        min_descriptor: int = 0,
    ):
        from pyspark.serializers import CloudPickleSerializer, write_int

        with mock.patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
            from pyspark.sql.worker import data_source_lookup

        self.protocol = data_source_lookup
        self.param_schema = param_schema
        if tcp:
            # A loopback TCP connection, as the JVM and the worker use by default.
            with socket.create_server(("127.0.0.1", 0)) as server:
                self.jvm_socket = socket.create_connection(server.getsockname())
                self.python_socket, _ = server.accept()
        else:
            self.jvm_socket, self.python_socket = socket.socketpair()
        if min_descriptor:
            # The worker's side at the lowest free descriptor from `min_descriptor` up, as a
            # reused worker can get it when earlier tasks left descriptors open.
            import fcntl

            descriptor = fcntl.fcntl(self.python_socket.fileno(), fcntl.F_DUPFD, min_descriptor)
            self.python_socket.close()
            self.python_socket = socket.socket(fileno=descriptor)
        self.jvm_in = self.jvm_socket.makefile("rb")
        self.jvm_out = self.jvm_socket.makefile("wb")
        self.error = None

        task_context = {
            "isBarrier": False,
            "connInfo": -1,
            "secret": None,
            "stageId": 1,
            "partitionId": 3,
            "attemptNumber": 0,
            "taskAttemptId": 7,
            "cpus": 1,
            "cpuAmount": "1",
            "resources": {},
            "localProperties": {},
        }
        self.write_utf8(json.dumps(task_context))
        CloudPickleSerializer()._write_with_length(reader, self.jvm_out)
        self.write_utf8(param_schema.json())
        self.write_utf8(read_schema.json())
        write_int(max_arrow_batch_size, self.jvm_out)
        self.jvm_out.write(b"\x01" if binary_as_bytes else b"\x00")
        self.jvm_out.flush()

        self.thread = threading.Thread(target=self.run_worker, daemon=True)
        self.thread.start()
        try:
            self.initialized = self.read_int() == 0
        except EOFError:
            # The worker failed before its reply.
            self.initialized = False

    def write_utf8(self, value: str) -> None:
        from pyspark.serializers import write_with_length

        write_with_length(value.encode("utf-8"), self.jvm_out)

    def read_int(self) -> int:
        from pyspark.serializers import read_int

        return read_int(self.jvm_in)

    def run_worker(self) -> None:
        infile = self.python_socket.makefile("rb", 65536)
        outfile = self.python_socket.makefile("wb", 65536)
        try:
            self.protocol._main(infile, outfile)
        except BaseException as e:
            self.error = e
        finally:
            try:
                outfile.flush()
            finally:
                self.python_socket.shutdown(socket.SHUT_WR)

    def send_request(self, batch: "pa.RecordBatch") -> None:
        from pyspark.serializers import write_int

        write_int(self.protocol.LOOKUP_REQUEST, self.jvm_out)
        writer = pa.ipc.new_stream(self.jvm_out, batch.schema)
        writer.write_batch(batch)
        writer.close()
        self.jvm_out.flush()

    def send(self, message: int) -> None:
        from pyspark.serializers import write_int

        write_int(message, self.jvm_out)
        self.jvm_out.flush()

    def parameters(self, *rows) -> "pa.RecordBatch":
        from pyspark.sql.pandas.types import to_arrow_schema

        schema = to_arrow_schema(
            StructType([StructField(f.name, f.dataType) for f in self.param_schema.fields]),
            timezone="UTC",
        )
        return pa.RecordBatch.from_pylist(
            [dict(zip(schema.names, row)) for row in rows], schema=schema
        )

    def start_response(self) -> "pa.RecordBatchStreamReader":
        from pyspark.serializers import SpecialLengths

        assert self.read_int() == SpecialLengths.START_ARROW_STREAM
        return pa.ipc.open_stream(self.jvm_in)

    def end_response(self) -> int:
        """Reads the end of a response, and returns its status."""
        assert self.read_int() == self.protocol.LOOKUP_RESPONSE_END
        return self.read_int()

    def lookup(self, *rows):
        """Sends a request and reads its whole response: its batches and its status."""
        self.send_request(self.parameters(*rows))
        batches = list(self.start_response())
        return batches, self.end_response()

    def lookup_error(self, *rows, error_type: type = PySparkException) -> Exception:
        """
        Sends a request whose response fails, and returns the failure, of `error_type`, once the
        response's Arrow stream has ended, as the JVM expects.
        """
        self.send_request(self.parameters(*rows))
        list(self.start_response())
        self.thread.join(30)
        assert isinstance(self.error, error_type), repr(self.error)
        return self.error

    def close(self) -> int:
        self.send(self.protocol.LOOKUP_END)
        reply = self.read_int()
        self.thread.join(30)
        return reply

    def shutdown(self) -> None:
        # The worker then reads the end of its input, and stops.
        self.jvm_socket.shutdown(socket.SHUT_RDWR)
        self.thread.join(30)
        for f in (self.jvm_in, self.jvm_out, self.jvm_socket):
            f.close()


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PythonDataSourceLookupWorkerTests(unittest.TestCase):
    """
    Tests of the executor-side lookup worker, `data_source_lookup`, run without a Spark session.
    """

    def setUp(self) -> None:
        events.clear()
        self.clients: List[LookupWorkerClient] = []

    def tearDown(self) -> None:
        for client in self.clients:
            client.shutdown()

    def client(self, reader, param_schema=None, **kwargs) -> LookupWorkerClient:
        if param_schema is None:
            param_schema = StructType([StructField("param_0", LongType())])
        client = LookupWorkerClient(reader, param_schema, **kwargs)
        self.clients.append(client)
        self.assertTrue(client.initialized, repr(client.error))
        return client

    @staticmethod
    def rows(batches):
        return [row for batch in batches for row in batch.to_pylist()]

    def test_tuple_form_converts_parameters_and_results(self):
        param_schema = StructType(
            [
                StructField("param_0", LongType()),
                StructField("param_1", StringType()),
                StructField("param_2", BinaryType()),
                StructField("param_3", DecimalType(10, 2)),
            ]
        )
        client = self.client(EchoLookupReader(), param_schema, max_arrow_batch_size=2)
        batches, status = client.lookup(
            (2, "a", b"\x00\x01", Decimal("1.50")), (None, None, None, None), (3, "c", b"", None)
        )
        # The parameters arrive with the Python types of a DataFrame's rows.
        self.assertEqual(
            events[0],
            "lookup 1: [(2, 'a', b'\\x00\\x01', Decimal('1.50')), (None, None, None, None), "
            "(3, 'c', b'', None)] in partition 3",
        )
        # Batches of at most two rows, with the ordinal first; a named Row is read by name.
        self.assertEqual([batch.num_rows for batch in batches], [2, 1])
        self.assertEqual(batches[0].schema.names, ["ordinal", "id", "name"])
        self.assertEqual(batches[0].schema.field("ordinal").type, pa.int32())
        self.assertEqual(
            self.rows(batches),
            [
                {"ordinal": 0, "id": 2, "name": "first 2"},
                {"ordinal": 0, "id": 2, "name": "second 2"},
                {"ordinal": 2, "id": 3, "name": "named 3"},
            ],
        )
        self.assertEqual(status, client.protocol.STATUS_COMPLETE)

        # The same reader copy serves the next request.
        client.lookup((4, "d", b"", None))
        self.assertEqual(events[-2], "lookup 2: [(4, 'd', b'', None)] in partition 3")
        self.assertEqual(client.close(), 0)
        self.assertIsNone(client.error)
        self.assertEqual(events[-1], "close")

    def test_tuple_form_passes_binary_as_bytearray_without_binary_as_bytes(self):
        param_schema = StructType(
            [StructField("param_0", LongType()), StructField("param_1", BinaryType())]
        )
        client = self.client(EchoLookupReader(), param_schema, binary_as_bytes=False)
        client.lookup((1, b"\x01"))
        self.assertEqual(events[0], "lookup 1: [(1, bytearray(b'\\x01'))] in partition 3")

    def test_tuple_form_ordinals(self):
        accepted = [(0, 0), (2**40, 2**31 - 1), (-(2**40), -(2**31))]
        accepted.append((pa.scalar(0, pa.int8()), 0))
        try:
            import numpy as np

            accepted.append((np.int64(0), 0))
            rejected = [np.bool_(False)]
        except ImportError:
            rejected = []
        rejected += [False, True, 0.0, None, "0", pa.scalar(None, pa.int64())]

        for ordinal, expected in accepted:
            with self.subTest(ordinal=ordinal):
                client = self.client(OrdinalLookupReader(ordinal))
                batches, _ = client.lookup((1,))
                self.assertEqual(self.rows(batches)[0]["ordinal"], expected)

        for ordinal in rejected:
            with self.subTest(ordinal=ordinal):
                events.clear()
                client = self.client(OrdinalLookupReader(ordinal))
                error = client.lookup_error((1,))
                self.assertEqual(error.getCondition(), "DATA_SOURCE_INVALID_RETURN_TYPE")
                # The generator was closed on the failure.
                self.assertEqual(events, ["finally"])

    def test_tuple_form_rejects_results_that_are_not_pairs(self):
        for item in [(0, (1, "one"), 2), Row(a=0, b=(1, "one")), [0, (1, "one")], 0]:
            with self.subTest(item=item):
                client = self.client(PairLookupReader(item))
                error = client.lookup_error((1,))
                self.assertEqual(error.getCondition(), "DATA_SOURCE_INVALID_RETURN_TYPE")
        # A pair whose row is not a tuple, a list or a `Row`. A lookup cannot return a
        # `pyarrow.RecordBatch` as a row.
        client = self.client(PairLookupReader((0, 5)))
        error = client.lookup_error((1,))
        self.assertEqual(error.getCondition(), "DATA_SOURCE_INVALID_RETURN_TYPE")
        self.assertEqual(
            error.getMessageParameters(),
            {
                "type": "int",
                "name": "PairLookupReader",
                "supported_types": "tuple, list, `pyspark.sql.types.Row`",
            },
        )
        client = self.client(PairLookupReader((0, (1, "one", "extra"))))
        error = client.lookup_error((1,))
        self.assertEqual(error.getCondition(), "DATA_SOURCE_RETURN_SCHEMA_MISMATCH")

    def test_tuple_form_rounds_decimals_and_fails_on_values_that_do_not_fit(self):
        # The worker converts the results, so a value that does not fit fails in Python.
        read_schema = StructType(
            [
                StructField("d", DecimalType(10, 2)),
                StructField("s", StructType([StructField("x", LongType(), False)])),
            ]
        )
        client = self.client(
            PairLookupReader((0, (Decimal("1.005"), (1,)))), read_schema=read_schema
        )
        batches, _ = client.lookup((1,))
        # Rounded to the scale, half to even.
        self.assertEqual(self.rows(batches), [{"ordinal": 0, "d": Decimal("1.00"), "s": {"x": 1}}])
        for row, error_type in [
            ((Decimal("123456789.12"), (1,)), InvalidOperation),
            ((Decimal("1.00"), (None,)), PySparkValueError),
        ]:
            with self.subTest(row=row):
                client = self.client(PairLookupReader((0, row)), read_schema=read_schema)
                client.lookup_error((1,), error_type=error_type)

    def test_first_result_of_neither_form_names_both_forms(self):
        table = pa.table({"o": [0], "id": [1], "name": ["a"]})
        for item in [table, Row(a=0, b=(1, "one")), (0, (1, "one"), 2), 0, None]:
            with self.subTest(item=type(item).__name__):
                client = self.client(PairLookupReader(item))
                error = client.lookup_error((1,))
                self.assertEqual(error.getCondition(), "DATA_SOURCE_INVALID_RETURN_TYPE")
                self.assertEqual(
                    error.getMessageParameters()["supported_types"],
                    "(ordinal, row) tuple, `pyarrow.RecordBatch`",
                )
        client = self.client(ListPairsLookupReader([(0, (1, "one")), table]))
        error = client.lookup_error((1,))
        self.assertEqual(error.getMessageParameters()["supported_types"], "(ordinal, row) tuple")

    def test_record_batch_form_of_the_docstring_example(self):
        # The example of `DataSourceLookupReader.lookup` gives each column its type, so a
        # request without a match returns no row instead of failing the schema check.
        client = self.client(DocExampleLookupReader({1: [(1, "one")]}), read_schema=USER_SCHEMA)
        batches, status = client.lookup((7,), (8,))
        self.assertEqual(self.rows(batches), [])
        self.assertEqual(status, client.protocol.STATUS_COMPLETE)
        batches, _ = client.lookup((7,), (1,))
        self.assertEqual(self.rows(batches), [{"ordinal": 1, "user_id": 1, "name": "one"}])

    @staticmethod
    def echo_batches(parameters):
        """The results of `EchoLookupReader`, as one `pyarrow.RecordBatch` with int64 ordinals."""
        ordinals, ids, names = [], [], []
        for ordinal, (key, *_) in enumerate(parameters):
            if key is None:
                continue
            for name in [f"first {key}", f"second {key}"] if key % 2 == 0 else [f"named {key}"]:
                ordinals.append(ordinal)
                ids.append(key)
                names.append(name)
        yield pa.RecordBatch.from_arrays(
            [pa.array(ordinals, pa.int64()), pa.array(ids, pa.int64()), pa.array(names)],
            names=["ordinal", "id", "name"],
        )

    def test_record_batch_form_gives_the_rows_of_the_tuple_form(self):
        parameters = [(2,), (None,), (3,), (7,)]
        tuple_rows = self.rows(self.client(EchoLookupReader()).lookup(*parameters)[0])
        # A maximum batch size below the size of the reader's batch, which the worker does not
        # apply to the RecordBatch form.
        client = self.client(BatchLookupReader(self.echo_batches), max_arrow_batch_size=2)
        batches, status = client.lookup(*parameters)
        self.assertIn("lookup [(2,), (None,), (3,), (7,)]", events)
        self.assertEqual(sorted(self.rows(batches), key=str), sorted(tuple_rows, key=str))
        # The ordinals are cast to int32, and the batch keeps its size.
        self.assertEqual([batch.num_rows for batch in batches], [4])
        self.assertEqual(batches[0].schema.field("ordinal").type, pa.int32())
        self.assertEqual(status, client.protocol.STATUS_COMPLETE)
        self.assertEqual(client.close(), 0)

    def test_record_batch_form_ordinals(self):
        def batch(ordinals):
            n = len(ordinals)
            return pa.RecordBatch.from_arrays(
                [ordinals, pa.array([1] * n, pa.int64()), pa.array(["a"] * n)],
                names=["o", "id", "name"],
            )

        # Any integer type, clamped to the range of a Java int as in the tuple form.
        for ordinals, expected in [
            (pa.array([0, 1], pa.int8()), [0, 1]),
            (pa.array([2**40], pa.uint64()), [2**31 - 1]),
            (pa.array([2**63, 2**64 - 1], pa.uint64()), [2**31 - 1, 2**31 - 1]),
            (pa.array([2**32 - 1], pa.uint32()), [2**31 - 1]),
            (pa.array([-(2**40)], pa.int64()), [-(2**31)]),
        ]:
            with self.subTest(ordinals=ordinals):
                client = self.client(BatchLookupReader(lambda p, o=ordinals: iter([batch(o)])))
                batches, _ = client.lookup((1,), (2,))
                self.assertEqual([row["ordinal"] for row in self.rows(batches)], expected)

        # The ordinals that the tuple form rejects: a null, a bool or a float.
        for ordinals, rejected_type in [
            (pa.array([0, None], pa.int64()), "NoneType"),
            (pa.array([True]), "bool"),
            (pa.array([0.0]), "double"),
        ]:
            with self.subTest(ordinals=ordinals):
                events.clear()
                client = self.client(BatchLookupReader(lambda p, o=ordinals: iter([batch(o)])))
                error = client.lookup_error((1,))
                self.assertEqual(error.getCondition(), "DATA_SOURCE_INVALID_RETURN_TYPE")
                self.assertEqual(error.getMessageParameters()["type"], rejected_type)
                self.assertEqual(events[-1], "finally")

    def test_record_batch_form_checks_every_batch_as_read_does(self):
        def ok():
            return pa.RecordBatch.from_arrays(
                [pa.array([0]), pa.array([1]), pa.array(["a"])], names=["o", "id", "name"]
            )

        def bad(**columns):
            arrays = {"o": pa.array([0]), "id": pa.array([1]), "name": pa.array(["a"])}
            arrays.update(columns)
            return pa.RecordBatch.from_arrays(list(arrays.values()), names=list(arrays))

        cases = [
            # The errors of `DataSourceReader.read` for a batch that does not match.
            (bad(name=pa.array([1])), "DATA_SOURCE_RETURN_SCHEMA_MISMATCH"),
            (bad(name=pa.array(["a"], pa.large_string())), "DATA_SOURCE_RETURN_SCHEMA_MISMATCH"),
            (bad(extra=pa.array([1])), "DATA_SOURCE_RETURN_SCHEMA_MISMATCH"),
            (
                pa.RecordBatch.from_arrays(
                    [pa.array([0]), pa.array([1]), pa.array(["a"])], names=["o", "id", "label"]
                ),
                "DATA_SOURCE_RETURN_SCHEMA_MISMATCH",
            ),
            # A value of the other form after the first batch.
            ((0, (1, "a")), "DATA_SOURCE_INVALID_RETURN_TYPE"),
        ]
        for second, condition in cases:
            with self.subTest(second=second):
                events.clear()
                client = self.client(BatchLookupReader(lambda p, s=second: iter([ok(), s])))
                error = client.lookup_error((1,))
                self.assertEqual(error.getCondition(), condition)
                self.assertEqual(events[-1], "finally")

        # A batch after a first pair is a value of the other form too.
        client = self.client(BatchLookupReader(lambda p: iter([(0, (1, "a")), ok()])))
        self.assertEqual(
            client.lookup_error((1,)).getCondition(), "DATA_SOURCE_INVALID_RETURN_TYPE"
        )

    def test_record_batch_form_passes_empty_batches(self):
        empty = pa.RecordBatch.from_arrays(
            [pa.array([], pa.int64()), pa.array([], pa.int64()), pa.array([], pa.string())],
            names=["o", "id", "name"],
        )
        client = self.client(BatchLookupReader(lambda p: iter([empty, empty])))
        batches, status = client.lookup((1,))
        self.assertEqual([batch.num_rows for batch in batches], [0, 0])
        self.assertEqual(status, client.protocol.STATUS_COMPLETE)
        # No result at all is an empty response in either form.
        client = self.client(BatchLookupReader(lambda p: iter([])))
        batches, status = client.lookup((1,))
        self.assertEqual(batches, [])

    def test_record_batch_form_timestamps(self):
        import datetime

        read_schema = StructType(
            [StructField("id", LongType()), StructField("ts", TimestampType())]
        )
        instant = datetime.datetime(2026, 3, 29, 1, 30, tzinfo=datetime.timezone.utc)

        def tuples(parameters):
            for ordinal, (key,) in enumerate(parameters):
                yield ordinal, (key, instant)

        def batches(parameters):
            n = len(parameters)
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array(range(n), pa.int64()),
                    pa.array([key for (key,) in parameters], pa.int64()),
                    # The Arrow type of a TIMESTAMP column in `DataSourceReader.read`.
                    pa.array([instant] * n, pa.timestamp("us", tz="UTC")),
                ],
                names=["o", "id", "ts"],
            )

        results = []
        for make_results in (tuples, batches):
            client = self.client(BatchLookupReader(make_results), read_schema=read_schema)
            response, _ = client.lookup((1,), (2,))
            results.append(self.rows(response))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[1][0]["ts"], instant)

    def test_lookup_must_return_an_iterator(self):
        client = self.client(ListLookupReader())
        error = client.lookup_error((1,))
        self.assertEqual(error.getCondition(), "DATA_SOURCE_INVALID_RETURN_TYPE")
        self.assertEqual(error.getMessageParameters()["supported_types"], "iterator")

    def test_cancel_stops_the_response_and_keeps_the_worker(self):
        client = self.client(EndlessLookupReader(), max_arrow_batch_size=1)
        client.send_request(client.parameters((1,)))
        response = client.start_response()
        self.assertEqual(response.read_next_batch().num_rows, 1)
        client.send(client.protocol.LOOKUP_CANCEL)
        # The batches that were in flight, then the end of the stream.
        list(response)
        self.assertEqual(client.end_response(), client.protocol.STATUS_CANCELLED)
        # Closing the generator ran its finally block.
        self.assertEqual(len(events), 1)
        self.assertTrue(events[0].startswith("finally "))

        # A cancel that arrives after its response ended is ignored, and the worker serves the
        # next request.
        events.clear()
        client.send(client.protocol.LOOKUP_CANCEL)
        client.send_request(client.parameters((2,)))
        response = client.start_response()
        self.assertEqual(response.read_next_batch().num_rows, 1)
        client.send(client.protocol.LOOKUP_CANCEL)
        list(response)
        self.assertEqual(client.end_response(), client.protocol.STATUS_CANCELLED)
        self.assertEqual(client.close(), 0)
        self.assertIsNone(client.error)

    @unittest.skipIf(not hasattr(select, "poll"), "Requires select.poll")
    def test_cancel_reaches_a_worker_whose_input_descriptor_is_1024_or_more(self):
        # `select.select` cannot check a descriptor of 1024 or more, which a reused worker can get.
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < 2048:
            if hard != resource.RLIM_INFINITY and hard < 2048:
                self.skipTest(f"The limit of open files, {hard}, is below 2048")
            resource.setrlimit(resource.RLIMIT_NOFILE, (2048, hard))
            self.addCleanup(resource.setrlimit, resource.RLIMIT_NOFILE, (soft, hard))
        client = self.client(EndlessLookupReader(), max_arrow_batch_size=1, min_descriptor=1024)
        self.assertGreaterEqual(client.python_socket.fileno(), 1024)
        client.send_request(client.parameters((1,)))
        response = client.start_response()
        self.assertEqual(response.read_next_batch().num_rows, 1)
        client.send(client.protocol.LOOKUP_CANCEL)
        list(response)
        self.assertEqual(client.end_response(), client.protocol.STATUS_CANCELLED)
        self.assertEqual(client.close(), 0)
        self.assertIsNone(client.error)

    def test_turns_off_nagle_on_a_tcp_socket(self):
        # A response takes several flushes. With Nagle's algorithm, a later flush would wait for
        # the JVM's delayed acknowledgement of an earlier one, about 40 ms per request.
        client = self.client(EchoLookupReader(), tcp=True)
        self.assertNotEqual(
            client.python_socket.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY), 0
        )
        batches, status = client.lookup((1,))
        self.assertEqual(status, client.protocol.STATUS_COMPLETE)
        self.assertEqual([r["name"] for r in self.rows(batches)], ["named 1"])
        self.assertEqual(client.close(), 0)
        self.assertIsNone(client.error)

    def test_turns_off_nagle_without_duplicating_the_socket(self):
        # On Windows, `os.dup` cannot duplicate the socket handle that a worker started without
        # the daemon writes to. The socket stays open and keeps serving, over TCP and over a
        # Unix domain socket.
        for tcp in (True, False):
            with mock.patch("os.dup", side_effect=OSError("os.dup of a socket handle")):
                client = self.client(EchoLookupReader(), tcp=tcp)
                if tcp:
                    self.assertNotEqual(
                        client.python_socket.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY),
                        0,
                    )
                for key in (1, 3):
                    batches, status = client.lookup((key,))
                    self.assertEqual(status, client.protocol.STATUS_COMPLETE)
                    self.assertEqual([r["name"] for r in self.rows(batches)], [f"named {key}"])
                self.assertEqual(client.close(), 0)
                self.assertIsNone(client.error, tcp)

    def test_planned_object_must_be_a_lookup_reader(self):
        client = LookupWorkerClient(42, StructType([StructField("param_0", LongType())]))
        self.clients.append(client)
        client.thread.join(30)
        self.assertEqual(client.error.getCondition(), "DATA_SOURCE_TYPE_MISMATCH")

    def test_runs_without_numpy(self):
        # NumPy is checked for only when it is imported, so a lookup works without it.
        with mock.patch.dict(sys.modules, {"numpy": None}):
            client = self.client(OrdinalLookupReader(0))
            batches, _ = client.lookup((1,))
            self.assertEqual(self.rows(batches)[0]["ordinal"], 0)
            client = self.client(OrdinalLookupReader(True))
            error = client.lookup_error((1,))
            self.assertEqual(error.getCondition(), "DATA_SOURCE_INVALID_RETURN_TYPE")
        # The module does not import NumPy.
        code = (
            "import sys; sys.modules['numpy'] = None; import pyspark.sql.worker.data_source_lookup"
        )
        env = dict(os.environ, SPARK_PYTHON_RUNTIME="PYTHON_WORKER")
        subprocess.run([sys.executable, "-c", code], check=True, env=env)


LOOKUP_JOIN_EXAMPLE = os.path.join(
    SPARK_HOME, "examples/src/main/python/sql/streaming/structured_python_lookup_join.py"
)

# The output of the example, as its docstring shows it.
EXAMPLE_OUTPUT = [
    (1, "gold", 2, "gold-1"),
    (1, "gold", 6, "gold-1"),
    (1, "gold", 6, "gold-5"),
    (2, None, 6, "no-segment"),
    (3, "gold", 6, None),
    (4, "gold", 6, None),
    (5, "gold", 6, None),
]


def load_lookup_join_example():
    """
    Loads the lookup join example as a module. It is not in `sys.modules`, so its classes are
    pickled by value, as the classes of a script that `spark-submit` runs.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("lookup_join_example", LOOKUP_JOIN_EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BasePythonDataSourceLookupTestsMixin:
    """
    End-to-end tests of streaming lookup joins with Python data sources. The lookup source holds
    the rows `(id, "name{id}")` for `id` in [1, 9], keyed by `id`.
    """

    def register_lookup_source(self, lookup_value=None, record_batches=False):
        """
        Registers the lookup source `lookup_test`. `lookup_value(reader, key)` computes the value
        of the column `extra` on the executor; it returns `None` by default. The column `ts` is
        the timestamp of the key's day in 2026. With `record_batches`, `lookup` returns the
        same rows as `pyarrow.RecordBatch`es, with the ordinal first.
        """
        import datetime

        from pyspark.sql.datasource import DataSource, EqualTo

        def timestamp(key):
            return datetime.datetime(2026, 1, key, 12, tzinfo=datetime.timezone.utc)

        class TupleReader(DataSourceLookupReader):
            def selectLookupPredicates(self, candidates):
                return [i for i, c in enumerate(candidates) if isinstance(c, EqualTo)][:1]

            def lookup(self, parameters):
                hits = [(o, key) for o, (key,) in enumerate(parameters) if 1 <= key <= 9]
                extras = [lookup_value(self, key) if lookup_value else None for _, key in hits]
                if not record_batches:
                    for (ordinal, key), extra in zip(hits, extras):
                        yield ordinal, (key, f"name{key}", extra, timestamp(key))
                    return
                import pyarrow as pa

                # One batch per hit, to return several batches.
                for (ordinal, key), extra in zip(hits, extras):
                    yield pa.RecordBatch.from_arrays(
                        [
                            pa.array([ordinal], pa.int64()),
                            pa.array([key], pa.int64()),
                            pa.array([f"name{key}"]),
                            pa.array([extra], pa.string()),
                            pa.array([timestamp(key)], pa.timestamp("us", tz="UTC")),
                        ],
                        names=["ordinal", "id", "name", "extra", "ts"],
                    )

        class LookupTestSource(DataSource):
            @classmethod
            def name(cls):
                return "lookup_test"

            def schema(self):
                return "id BIGINT, name STRING, extra STRING, ts TIMESTAMP"

            def lookupReader(self, schema):
                return TupleReader()

        self.spark.dataSource.register(LookupTestSource)

    def run_lookup_join(self, ids, how="inner", lookup=None):
        """
        Joins a stream of the ids, in one partition and in this order, with the lookup source,
        or with the DataFrame `lookup`, and returns the sorted rows.
        """
        with tempfile.TemporaryDirectory(prefix="lookup_stream") as stream_dir:
            with open(os.path.join(stream_dir, "ids.json"), "w") as f:
                f.writelines(json.dumps({"sid": i}) + "\n" for i in ids)
            stream = self.spark.readStream.schema("sid BIGINT").json(stream_dir)
            if lookup is None:
                lookup = self.spark.read.format("lookup_test").load()
            joined = stream.join(lookup, stream.sid == lookup.id, how)
            name = f"lookup_join_{abs(hash((tuple(ids), how)))}"
            query = (
                joined.writeStream.format("memory")
                .queryName(name)
                .trigger(availableNow=True)
                .start()
            )
            try:
                query.awaitTermination(120)
                return sorted(self.spark.table(name).collect(), key=lambda r: str(r))
            finally:
                query.stop()
                self.spark.catalog.dropTempView(name)

    def test_lookup_join(self):
        with self.sql_conf({"spark.sql.python.externalLookup.enabled": "true"}):
            self.register_lookup_source()
            rows = self.run_lookup_join([1, 3, 12, 3])
            self.assertEqual(
                [(r.sid, r.name) for r in rows],
                [(1, "name1"), (3, "name3"), (3, "name3")],
            )
            rows = self.run_lookup_join([2, 12], how="left_anti")
            self.assertEqual([r.sid for r in rows], [12])

    def test_lookup_join_gives_the_same_rows_in_both_forms(self):
        with self.sql_conf({"spark.sql.python.externalLookup.enabled": "true"}):
            results = {}
            for record_batches in (False, True):
                self.register_lookup_source(record_batches=record_batches)
                results[record_batches] = [
                    self.run_lookup_join([1, 3, 12, 3, 5]),
                    self.run_lookup_join([2, 12, 4], how="left_outer"),
                    self.run_lookup_join([2, 12, 4], how="left_semi"),
                ]
            self.assertEqual(results[True], results[False])
            inner = results[True][0]
            self.assertEqual([r.sid for r in inner], [1, 3, 3, 5])
            # PySpark returns a TIMESTAMP as a naive datetime in the local time zone.
            noon = datetime.datetime(2026, 1, 1, 12, tzinfo=datetime.timezone.utc)
            self.assertEqual(inner[0].ts, noon.astimezone().replace(tzinfo=None))

    def ship_file(self, path):
        """Ships the file to the executors, as `SparkFiles.get` finds it there."""
        self.spark.addArtifacts(path, file=True)

    def test_lookup_reads_a_file_added_as_an_artifact(self):
        with tempfile.TemporaryDirectory(prefix="lookup_artifacts") as files_dir:
            path = os.path.join(files_dir, "lookup_artifact.txt")
            with open(path, "w") as f:
                f.write("from an artifact")
            self.spark.addArtifacts(path, file=True)

            def lookup_value(reader, key):
                from pyspark import SparkFiles

                with open(SparkFiles.get("lookup_artifact.txt")) as f:
                    return f"{f.read()} for {key}"

            with self.sql_conf({"spark.sql.python.externalLookup.enabled": "true"}):
                self.register_lookup_source(lookup_value)
                rows = self.run_lookup_join([4, 5])
        self.assertEqual(
            [r.extra for r in rows], ["from an artifact for 4", "from an artifact for 5"]
        )

    def test_hint_mode_and_the_cache_option_of_the_hint(self):
        from pyspark.sql.datasource import DataSource, DataSourceReader, EqualTo

        class HintLookupReader(DataSourceLookupReader):
            def __init__(self, marker_dir):
                self.marker_dir = marker_dir

            def selectLookupPredicates(self, candidates):
                return [i for i, c in enumerate(candidates) if isinstance(c, EqualTo)][:1]

            def maxParameterRowsPerRequest(self):
                return 1

            def lookup(self, parameters):
                with open(os.path.join(self.marker_dir, "requests"), "a") as f:
                    f.write(f"{parameters[0][0]}\n")
                for ordinal, (key,) in enumerate(parameters):
                    yield ordinal, (key, "lookup")

        class HintBatchReader(DataSourceReader):
            def read(self, partition):
                yield from [(1, "read"), (2, "read")]

        class HintTestSource(DataSource):
            @classmethod
            def name(cls):
                return "lookup_hint_test"

            def schema(self):
                return "id BIGINT, name STRING"

            def reader(self, schema):
                return HintBatchReader()

            def lookupReader(self, schema):
                return HintLookupReader(self.options["markerDir"])

        self.spark.dataSource.register(HintTestSource)

        def run(*hint_parameters):
            with tempfile.TemporaryDirectory(prefix="lookup_markers") as marker_dir:
                lookup = (
                    self.spark.read.format("lookup_hint_test")
                    .option("markerDir", marker_dir)
                    .load()
                )
                if hint_parameters:
                    lookup = lookup.hint(*hint_parameters)
                rows = self.run_lookup_join([1, 2, 1], lookup=lookup)
                requests_file = os.path.join(marker_dir, "requests")
                requests = []
                if os.path.exists(requests_file):
                    with open(requests_file) as f:
                        requests = f.read().split()
                return [(r.sid, r.name) for r in rows], requests

        with self.sql_conf({"spark.sql.python.externalLookup.enabled": "true"}):
            expected = [(1, "lookup"), (1, "lookup"), (2, "lookup")]
            # A source with a batch reader is read by lookups only with the hint.
            self.assertEqual(run(), ([(1, "read"), (1, "read"), (2, "read")], []))
            # The lookup cache of the task serves the second row of key 1.
            self.assertEqual(run("EXTERNAL_LOOKUP"), (expected, ["1", "2"]))
            # The hint option turns the cache off for this join, so key 1 is looked up again.
            self.assertEqual(
                run("EXTERNAL_LOOKUP", "cache_enabled=false"), (expected, ["1", "2", "1"])
            )

    @unittest.skipIf(
        not os.path.exists(LOOKUP_JOIN_EXAMPLE), "The lookup join example is not available."
    )
    def test_sqlite_example(self):
        # examples/src/main/python/sql/streaming/structured_python_lookup_join.py, run in this
        # session with each setting of its options.
        example = load_lookup_join_example()
        with tempfile.TemporaryDirectory(prefix="lookup_example") as root:
            db, events_dir = example.build_demo_files(root)
            self.ship_file(db)
            with self.sql_conf(
                {
                    "spark.sql.python.externalLookup.enabled": "true",
                    "spark.sql.python.filterPushdown.enabled": "true",
                }
            ):
                for select_range in (True, False):
                    for arrow in (False, True):
                        with self.subTest(select_range=select_range, arrow=arrow):
                            rows = example.enrich_events(
                                self.spark, events_dir, select_range=select_range, arrow=arrow
                            ).collect()
                            self.assertEqual([tuple(r) for r in rows], EXAMPLE_OUTPUT)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class PythonDataSourceLookupTests(BasePythonDataSourceLookupTestsMixin, ReusedSQLTestCase):
    def ship_file(self, path):
        self.spark.sparkContext.addFile(path)

    def test_lookup_sees_its_task_files_and_accumulators(self):
        accumulator = self.spark.sparkContext.accumulator(0)
        with tempfile.TemporaryDirectory(prefix="lookup_files") as files_dir:
            path = os.path.join(files_dir, "lookup_names.txt")
            with open(path, "w") as f:
                f.write("from a file")
            self.spark.sparkContext.addFile(path)

            def lookup_value(reader, key):
                from pyspark import SparkFiles, TaskContext

                accumulator.add(1)
                with open(SparkFiles.get("lookup_names.txt")) as f:
                    return f"{f.read()} in partition {TaskContext.get().partitionId()}"

            with self.sql_conf({"spark.sql.python.externalLookup.enabled": "true"}):
                self.register_lookup_source(lookup_value)
                rows = self.run_lookup_join([4, 5])
        self.assertEqual([r.sid for r in rows], [4, 5])
        for row in rows:
            self.assertRegex(row.extra, r"^from a file in partition \d+$")
        # The updates of the accumulator reach the driver when the readers close.
        self.assertEqual(accumulator.value, 2)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
