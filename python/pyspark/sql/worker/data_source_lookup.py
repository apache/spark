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
import operator
import os
import select
import socket
import sys
from itertools import chain, islice
from typing import IO, Any, Iterator, List

import pyarrow as pa
import pyarrow.compute as pc

from pyspark.errors import PySparkAssertionError, PySparkRuntimeError
from pyspark.logger.worker_io import capture_outputs
from pyspark.serializers import (
    SpecialLengths,
    UTF8Deserializer,
    read_bool,
    read_int,
    write_int,
)
from pyspark.sql import Row
from pyspark.sql.conversion import ArrowTableToRowsConversion
from pyspark.sql.datasource import DataSourceLookupReader
from pyspark.sql.types import StructType, _parse_datatype_json_string
from pyspark.sql.worker.plan_data_source_read import RowsToArrowConverter
from pyspark.sql.worker.utils import worker_run
from pyspark.taskcontext import TaskContext
from pyspark.util import local_connect_and_auth
from pyspark.worker_message import TaskContextInfo
from pyspark.worker_util import pickleSer, read_command

utf8_deserializer = UTF8Deserializer()

# The messages of the lookup protocol, after the initialization. They must match
# `PythonExternalLookupProtocol` on the JVM.
# From the JVM: a request, followed by one Arrow stream of the parameter rows.
LOOKUP_REQUEST = 1
# From the JVM: stop the response that is being written. It may arrive after the response ended.
LOOKUP_CANCEL = 2
# From the JVM: close the reader; no request follows.
LOOKUP_END = 3
# To the JVM: the end of a response, followed by its status.
LOOKUP_RESPONSE_END = 4
# The status of a response.
STATUS_COMPLETE = 0
STATUS_CANCELLED = 1

# The bounds of a Java int, which the JVM reads ordinals into. An ordinal out of these bounds is
# clamped to them, keeping its sign, as `plan_data_source_lookup` does for the request size: the
# JVM then reports it as out of range.
INT_MIN = -(2**31)
INT_MAX = 2**31 - 1


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


def _is_numpy_bool(value: Any) -> bool:
    # NumPy is not imported here, so the worker runs without it. A value can be a NumPy bool only
    # if the reader has imported NumPy.
    numpy = sys.modules.get("numpy")
    return numpy is not None and isinstance(value, numpy.bool_)


class LookupServer:
    """
    Serves the requests of one lookup reader, the planned `DataSourceLookupReader` that Spark
    unpickled for one input partition, until the JVM closes it.

    Each request holds one Arrow stream with one batch of parameter rows. The response is one
    Arrow stream whose first column is the ordinal of the parameter row that a result row
    answers, followed by the columns of the read schema. It is written batch by batch, as the
    reader returns its results, and ends with `LOOKUP_RESPONSE_END` and the status. The reader
    returns `(ordinal, row)` pairs, or `pyarrow.RecordBatch`es whose first column is the ordinal,
    as `DataSourceReader.read` returns rows or batches.
    """

    def __init__(
        self,
        reader: DataSourceLookupReader,
        param_schema: StructType,
        read_schema: StructType,
        max_arrow_batch_size: int,
        binary_as_bytes: bool,
        infile: IO,
        outfile: IO,
    ):
        self.reader = reader
        self.reader_name = type(reader).__name__
        self.max_arrow_batch_size = max_arrow_batch_size
        self.infile = infile
        self.outfile = outfile
        self.param_converters = [
            ArrowTableToRowsConversion._create_converter(
                field.dataType, binary_as_bytes=binary_as_bytes
            )
            for field in param_schema.fields
        ]
        self.rows_converter = RowsToArrowConverter(
            read_schema, lambda: self.reader_name, "tuple, list, `pyspark.sql.types.Row`"
        )
        self.response_schema = pa.schema(
            [pa.field("ordinal", pa.int32(), nullable=False)] + list(self.rows_converter.pa_schema)
        )

    def serve(self) -> None:
        while True:
            message = read_int(self.infile)
            if message == LOOKUP_REQUEST:
                self.lookup()
            elif message == LOOKUP_CANCEL:
                # The response that it cancels ended before it arrived.
                continue
            elif message == LOOKUP_END:
                self.reader.close()
                write_int(0, self.outfile)
                return
            else:
                raise AssertionError(f"Unexpected message from the JVM: {message}")

    def lookup(self) -> None:
        parameters = self.read_parameters()
        # Tell the JVM that the request was read: from now on, it may send a cancel.
        write_int(SpecialLengths.START_ARROW_STREAM, self.outfile)
        self.outfile.flush()

        status = STATUS_COMPLETE
        results: Any = None
        writer = pa.ipc.new_stream(self.outfile, self.response_schema)
        try:
            results = self.reader.lookup(self.to_tuples(parameters))
            if not isinstance(results, Iterator):
                raise PySparkRuntimeError(
                    errorClass="DATA_SOURCE_INVALID_RETURN_TYPE",
                    messageParameters={
                        "type": type(results).__name__,
                        "name": self.reader_name,
                        "supported_types": "iterator",
                    },
                )
            for batch in self.result_batches(results):
                if self.cancel_requested():
                    status = STATUS_CANCELLED
                    break
                writer.write_batch(batch)
                self.outfile.flush()
        finally:
            try:
                # Closing a generator raises `GeneratorExit` at its `yield`, so that its `finally`
                # and `with` blocks run, on a cancel and on a failure. A generator that has ended
                # ignores it.
                close = getattr(results, "close", None)
                if close is not None:
                    close()
            finally:
                # End the Arrow stream also on a failure, so that the JVM reads the error that
                # follows it.
                writer.close()
        del parameters, results

        write_int(LOOKUP_RESPONSE_END, self.outfile)
        write_int(status, self.outfile)
        self.outfile.flush()

    def read_parameters(self) -> pa.RecordBatch:
        batches = list(pa.ipc.open_stream(self.infile))
        assert len(batches) == 1 and batches[0].num_rows > 0, (
            "Expected a lookup request to hold one batch of parameter rows, "
            f"but found {len(batches)} batches."
        )
        return batches[0]

    def to_tuples(self, parameters: pa.RecordBatch) -> List[tuple]:
        columns = [
            [convert(value) for value in ArrowTableToRowsConversion._to_pylist(column)]
            for convert, column in zip(self.param_converters, parameters.columns)
        ]
        return list(zip(*columns))

    def cancel_requested(self) -> bool:
        """
        Whether the JVM has cancelled the response, checked without blocking. The JVM sends
        nothing but a cancel after a request until the response ends, and Python reads the whole
        request before it writes the response, so the input buffer of `infile` is empty here, and
        the socket alone tells whether a cancel arrived.
        """
        readable, _, _ = select.select([self.infile], [], [], 0)
        if not readable:
            return False
        message = read_int(self.infile)
        assert message == LOOKUP_CANCEL, f"Unexpected message from the JVM: {message}"
        return True

    def invalid_return_type(self, value: Any, supported_types: str) -> PySparkRuntimeError:
        return PySparkRuntimeError(
            errorClass="DATA_SOURCE_INVALID_RETURN_TYPE",
            messageParameters={
                "type": type(value).__name__,
                "name": self.reader_name,
                "supported_types": supported_types,
            },
        )

    def ordinal(self, value: Any) -> int:
        """The ordinal of a result: an integer that is not a bool."""
        if isinstance(value, pa.Scalar) and pa.types.is_integer(value.type):
            value = value.as_py()
        if value is None or isinstance(value, bool) or _is_numpy_bool(value):
            raise self.invalid_return_type(value, "int")
        try:
            ordinal = operator.index(value)
        except TypeError:
            raise self.invalid_return_type(value, "int") from None
        return _clamp(ordinal, INT_MIN, INT_MAX)

    def result_batches(self, results: Iterator[Any]) -> Iterator[pa.RecordBatch]:
        """
        The response batches of the results of one `lookup` call. As `records_to_arrow_batches`
        does for `DataSourceReader.read`, the first result decides the form for the whole call:
        `pyarrow.RecordBatch`es, which are checked and passed on, or `(ordinal, row)` pairs,
        which are converted to batches.
        """
        try:
            first = next(results)
        except StopIteration:
            return
        if isinstance(first, pa.RecordBatch):
            yield from self.arrow_batches(chain([first], results))
        elif not isinstance(first, tuple) or isinstance(first, Row) or len(first) != 2:
            raise self.invalid_return_type(first, "(ordinal, row) tuple, `pyarrow.RecordBatch`")
        else:
            yield from self.tuple_batches(chain([first], results))

    def tuple_batches(self, results: Iterator[Any]) -> Iterator[pa.RecordBatch]:
        """Converts the `(ordinal, row)` pairs that the reader returns to Arrow batches."""
        while True:
            pairs = list(islice(results, self.max_arrow_batch_size))
            if not pairs:
                return
            ordinals = []
            rows = []
            for pair in pairs:
                if not isinstance(pair, tuple) or isinstance(pair, Row) or len(pair) != 2:
                    raise self.invalid_return_type(pair, "(ordinal, row) tuple")
                ordinals.append(self.ordinal(pair[0]))
                rows.append(pair[1])
            values = self.rows_converter.convert(rows)
            yield pa.RecordBatch.from_arrays(
                [pa.array(ordinals, pa.int32())] + values.columns, schema=self.response_schema
            )

    def arrow_batches(self, results: Iterator[Any]) -> Iterator[pa.RecordBatch]:
        """
        Checks the `pyarrow.RecordBatch`es that the reader returns, each of them: the ordinal
        column first, then the columns of the read schema, as `DataSourceReader.read` returns
        them and with the checks of read. A batch keeps its size.
        """
        num_columns = self.rows_converter.num_cols + 1
        for batch in results:
            if not isinstance(batch, pa.RecordBatch):
                raise self.invalid_return_type(batch, "`pyarrow.RecordBatch`")
            if batch.num_columns != num_columns:
                raise PySparkRuntimeError(
                    errorClass="DATA_SOURCE_RETURN_SCHEMA_MISMATCH",
                    messageParameters={
                        "expected": str(num_columns),
                        "actual": str(batch.num_columns),
                    },
                )
            values = pa.RecordBatch.from_arrays(
                batch.columns[1:], schema=pa.schema(list(batch.schema)[1:])
            )
            self.rows_converter.check_batch(values)
            yield pa.RecordBatch.from_arrays(
                [self.arrow_ordinals(batch.column(0))] + values.columns,
                schema=self.response_schema,
            )

    def arrow_ordinals(self, ordinals: pa.Array) -> pa.Array:
        """
        The ordinal column of a `pyarrow.RecordBatch` result, with the rules of an ordinal of
        the tuple form: an integer, not a bool and not null, clamped to the range of a Java int.
        """
        if not pa.types.is_integer(ordinals.type):
            raise PySparkRuntimeError(
                errorClass="DATA_SOURCE_INVALID_RETURN_TYPE",
                messageParameters={
                    "type": str(ordinals.type),
                    "name": self.reader_name,
                    "supported_types": "int",
                },
            )
        if ordinals.null_count > 0:
            raise self.invalid_return_type(None, "int")
        if pa.types.is_unsigned_integer(ordinals.type):
            # A Python int bound would cast a uint64 column to int64, which fails for 2**63 or
            # more, so unsigned ordinals are clamped against an unsigned bound.
            bound = pa.scalar(INT_MAX, pa.uint64())
            ordinals = pc.min_element_wise(ordinals.cast(pa.uint64()), bound).cast(pa.int32())
        elif ordinals.type != pa.int32():
            ordinals = pc.max_element_wise(pc.min_element_wise(ordinals, INT_MAX), INT_MIN).cast(
                pa.int32()
            )
        return ordinals


def disable_nagle(outfile: IO) -> None:
    """
    Turns off Nagle's algorithm on the TCP socket of `outfile`. A lookup is a request and its
    response, and the response takes several flushes: `START_ARROW_STREAM`, which the JVM needs
    before it may send a cancel, then the results and the end. With Nagle's algorithm, a later
    write waits until the JVM acknowledges the earlier one, and the JVM, which sends nothing while
    it waits for the rest of the response, delays that acknowledgement, by about 40 ms on Linux,
    so each request would take at least that long. A Unix domain socket has no such delay.

    The socket object wraps the descriptor of `outfile` without `os.dup`, which cannot duplicate
    a socket handle on Windows, and is detached so that the socket stays open.
    """
    sock = socket.socket(fileno=outfile.fileno())
    try:
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    finally:
        sock.detach()


def _main(infile: IO, outfile: IO) -> None:
    """
    Main method for running the lookups of a Python data source on an executor.

    This process is invoked from `PythonExternalLookupWorker` in the JVM, by the reader that
    `PythonExternalLookupReaderFactory.createReader` creates for one input partition of an
    external lookup join. It serves every request of that reader, one at a time.

    After the data that `worker_run` reads, the JVM sends, in order:
    - the task context
    - the planned `DataSourceLookupReader`, pickled
    - the schema of a parameter row, as JSON
    - the read schema, as JSON
    - the maximum number of rows of an Arrow batch (int)
    - whether binary values are passed as `bytes` (bool)

    This process replies with 0 once it is ready. It then reads requests and writes their
    responses, as `LookupServer` describes, until the JVM closes the reader. It then calls
    `close()` on the reader and replies with 0.
    """
    disable_nagle(outfile)
    # `from_stream` reads with `utf8_deserializer`, which reads a file too.
    task_context = TaskContextInfo.from_stream(infile).to_task_context()  # type: ignore[arg-type]
    TaskContext._setTaskContext(task_context)
    try:
        reader = read_command(pickleSer, infile)
        if not isinstance(reader, DataSourceLookupReader):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "an instance of DataSourceLookupReader",
                    "actual": f"'{type(reader).__name__}'",
                },
            )
        param_schema = _parse_datatype_json_string(utf8_deserializer.loads(infile))
        read_schema = _parse_datatype_json_string(utf8_deserializer.loads(infile))
        max_arrow_batch_size = read_int(infile)
        binary_as_bytes = read_bool(infile)
        assert isinstance(param_schema, StructType) and isinstance(read_schema, StructType)
        assert max_arrow_batch_size > 0, (
            f"The maximum Arrow batch size must be positive, but is {max_arrow_batch_size}."
        )

        with capture_outputs():
            server = LookupServer(
                reader,
                param_schema,
                read_schema,
                max_arrow_batch_size,
                binary_as_bytes,
                infile,
                outfile,
            )
            write_int(0, outfile)
            outfile.flush()
            server.serve()
    finally:
        TaskContext._setTaskContext(None)


def main(infile: IO, outfile: IO) -> None:
    worker_run(_main, infile, outfile, memory_limit_env="PYSPARK_EXECUTOR_MEMORY_MB")


if __name__ == "__main__":
    # As `get_sock_file_to_executor`, which the other workers use when the JVM starts them without
    # the daemon, but with a file for each direction: the check for a cancel selects on the file
    # of the input, and the one file of both directions has no file descriptor.
    sock_file, sock = local_connect_and_auth(
        os.environ.get(
            "PYTHON_WORKER_FACTORY_SOCK_PATH",
            int(os.environ.get("PYTHON_WORKER_FACTORY_PORT", -1)),
        ),
        os.environ.get("PYTHON_WORKER_FACTORY_SECRET"),
    )
    # The authentication timeout applies to the connection only. Between requests the JVM may
    # send nothing for long, as the streaming source runner and the foreachBatch worker allow.
    sock.settimeout(None)
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
    with (
        sock_file,
        sock.makefile("rb", buffer_size) as infile,
        sock.makefile("wb", buffer_size) as outfile,
    ):
        main(infile, outfile)
