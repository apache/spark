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
Util functions for workers.
"""

import importlib
import importlib.util
import json
import os
import sys
import warnings
import zipimport
from contextlib import contextmanager
from inspect import currentframe, getframeinfo
from typing import IO, Any, Generator, Iterator, Optional, Union, overload

from pyspark.messages import ZeroCopyByteStream

if "SPARK_TESTING" in os.environ:
    assert os.environ.get("SPARK_PYTHON_RUNTIME") == "PYTHON_WORKER", (
        "This module can only be imported in python woker"
    )

# 'resource' is a Unix specific module.
has_resource_module = True
try:
    import resource
except ImportError:
    has_resource_module = False

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkRuntimeError
from pyspark.serializers import (
    CPickleSerializer,
    FramedSerializer,
    UTF8Deserializer,
    read_int,
    read_long,
    write_int,
)
from pyspark.sql.types import DataType, StructType, _parse_datatype_json_string
from pyspark.util import is_remote_only, local_connect_and_auth

pickleSer = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def add_path(path: str) -> bool:
    # worker can be used, so do not add path multiple times
    if path not in sys.path:
        # overwrite system packages
        sys.path.insert(1, path)
        return True
    return False


def _zip_archive_of(module_name: str) -> Optional[str]:
    spec = importlib.util.find_spec(module_name)
    loader = spec.loader if spec is not None else None
    return loader.archive if isinstance(loader, zipimport.zipimporter) else None


# The zip archives this worker's pyspark and py4j were loaded from, such as
# $SPARK_HOME/python/lib/pyspark.zip or the copy that Spark ships to YARN containers. They are
# not replaced while an application runs, so _invalidate_import_caches keeps their cached
# directory listings. A user zip that supplies the worker's pyspark or py4j counts as one of
# these archives and must not be replaced while the application runs either: the worker already
# runs modules from it, so a replaced copy could not be used safely anyway. Other archives,
# including user archives on PYTHONPATH, may be replaced and are not kept.
_SPARK_ARCHIVES = frozenset(
    archive for archive in map(_zip_archive_of, ("pyspark", "py4j")) if archive is not None
)


def _invalidate_import_caches() -> None:
    """
    Like importlib.invalidate_caches(), but keeps the zipimporters of Spark's own archives
    (_SPARK_ARCHIVES).

    Up to Python 3.12, invalidating a zipimporter re-reads the archive's whole directory, and an
    archive has one zipimporter per package imported from it, so pyspark.zip is re-read once for
    every package the worker imported from it. Python 3.13+ re-reads each archive once on its
    next lookup. All other archives, such as user archives on PYTHONPATH and the ones in
    python_includes, are still invalidated.
    """
    if not _SPARK_ARCHIVES:
        importlib.invalidate_caches()
        return
    kept = {
        path: finder
        for path, finder in list(sys.path_importer_cache.items())
        # importlib.invalidate_caches() drops relative paths because the working directory may
        # have changed, so only keep absolute ones.
        if isinstance(finder, zipimport.zipimporter)
        and finder.archive in _SPARK_ARCHIVES
        and os.path.isabs(path)
    }
    for path in kept:
        del sys.path_importer_cache[path]
    try:
        importlib.invalidate_caches()
    finally:
        sys.path_importer_cache.update(kept)


def read_command(serializer: FramedSerializer, file: Union[IO, bytes, memoryview]) -> Any:
    if not is_remote_only():
        from pyspark.core.broadcast import Broadcast

    if isinstance(file, (bytes, memoryview)):
        command = serializer.loads(file)
    else:
        command = serializer._read_with_length(file)
    if not is_remote_only() and isinstance(command, Broadcast):
        command = serializer.loads(command.value)
    return command


def check_python_version(infile_or_version: Union[IO, str]) -> None:
    """
    Check the Python version between the running process and the one used to serialize the command.
    """
    if isinstance(infile_or_version, str):
        version = infile_or_version
    else:
        version = utf8_deserializer.loads(infile_or_version)
    worker_version = "%d.%d" % sys.version_info[:2]
    if version != worker_version:
        raise PySparkRuntimeError(
            errorClass="PYTHON_VERSION_MISMATCH",
            messageParameters={
                "worker_version": worker_version,
                "driver_version": str(version),
            },
        )


def setup_memory_limits(memory_limit_mb: int) -> None:
    """
    Sets up the memory limits.

    If memory_limit_mb > 0 and `resource` module is available, sets the memory limit.
    Windows does not support resource limiting and actual resource is not limited on MacOS.
    """
    if memory_limit_mb > 0 and has_resource_module:
        total_memory = resource.RLIMIT_AS
        try:
            soft_limit, hard_limit = resource.getrlimit(total_memory)
            msg = "Current mem limits: {0} of max {1}\n".format(soft_limit, hard_limit)
            print(msg, file=sys.stderr)

            # convert to bytes
            new_limit = memory_limit_mb * 1024 * 1024

            if soft_limit == resource.RLIM_INFINITY or new_limit < soft_limit:
                msg = "Setting mem limits to {0} of max {1}\n".format(new_limit, new_limit)
                print(msg, file=sys.stderr)
                resource.setrlimit(total_memory, (new_limit, new_limit))

        except (resource.error, OSError, ValueError) as e:
            # not all systems support resource limits, so warn instead of failing
            current = currentframe()
            lineno = getframeinfo(current).lineno + 1 if current is not None else 0
            if "__file__" in globals():
                print(
                    warnings.formatwarning(
                        "Failed to set memory limit: {0}".format(e),
                        ResourceWarning,
                        __file__,
                        lineno,
                    ),
                    file=sys.stderr,
                )


@overload
def setup_spark_files(infile_or_spark_files_dir: IO) -> None: ...
@overload
def setup_spark_files(infile_or_spark_files_dir: str, python_includes: list[str]) -> None: ...
def setup_spark_files(
    infile_or_spark_files_dir: Union[IO, str], python_includes: Optional[list[str]] = None
) -> None:
    """
    Set up Spark files, archives, and pyfiles.
    """
    if isinstance(infile_or_spark_files_dir, str):
        spark_files_dir = infile_or_spark_files_dir
    else:
        spark_files_dir = utf8_deserializer.loads(infile_or_spark_files_dir)

    if not is_remote_only():
        from pyspark.core.files import SparkFiles

        SparkFiles._root_directory = spark_files_dir
        SparkFiles._is_running_on_worker = True

    # fetch names of includes (*.zip and *.egg files) and construct PYTHONPATH
    path_changed = add_path(spark_files_dir)  # *.py files that were added will be copied here
    if not isinstance(infile_or_spark_files_dir, str):
        python_includes = [
            utf8_deserializer.loads(infile_or_spark_files_dir)
            for _ in range(read_int(infile_or_spark_files_dir))
        ]
    assert python_includes is not None

    for filename in python_includes:
        path_changed = add_path(os.path.join(spark_files_dir, filename)) or path_changed

    if path_changed:
        _invalidate_import_caches()


@overload
def setup_broadcasts(infile_or_variables: IO[Any]) -> None: ...
@overload
def setup_broadcasts(infile_or_variables: ZeroCopyByteStream) -> None: ...
@overload
def setup_broadcasts(
    infile_or_variables: list[tuple[int, Union[str, None]]], conn_info: str, auth_secret: None
) -> None: ...
@overload
def setup_broadcasts(
    infile_or_variables: list[tuple[int, Union[str, None]]], conn_info: int, auth_secret: str
) -> None: ...
@overload
def setup_broadcasts(
    infile_or_variables: list[tuple[int, Union[str, None]]],
    conn_info: Optional[Union[str, int]],
    auth_secret: Optional[str],
) -> None: ...
def setup_broadcasts(
    infile_or_variables: Union[ZeroCopyByteStream, IO[Any], list[tuple[int, Union[str, None]]]],
    conn_info: Optional[Union[str, int]] = None,
    auth_secret: Optional[str] = None,
) -> None:
    """
    Set up broadcasted variables.
    """
    if not is_remote_only():
        from pyspark.core.broadcast import Broadcast, _broadcastRegistry

    if isinstance(infile_or_variables, list):
        variables = infile_or_variables
    else:
        from pyspark.worker_message import BroadcastInfo

        broadcast_info = BroadcastInfo.from_stream(infile_or_variables)
        conn_info = broadcast_info.conn_info
        auth_secret = broadcast_info.auth_secret
        variables = broadcast_info.variables

    needs_broadcast_decryption_server = conn_info is not None or auth_secret is not None

    if needs_broadcast_decryption_server:
        broadcast_sock_file, _ = local_connect_and_auth(conn_info, auth_secret)
    else:
        broadcast_sock_file = None

    for bid, path in variables:
        if bid >= 0:
            if path is None:
                read_bid = read_long(broadcast_sock_file)
                assert read_bid == bid
                _broadcastRegistry[bid] = Broadcast(sock_file=broadcast_sock_file)
            else:
                _broadcastRegistry[bid] = Broadcast(path=path)
        else:
            _broadcastRegistry.pop(-bid - 1)

    if broadcast_sock_file is not None:
        broadcast_sock_file.write(b"1")
        broadcast_sock_file.close()


@contextmanager
def get_sock_file_to_executor(timeout: Optional[int] = -1) -> Generator[IO, None, None]:
    # Read information about how to connect back to the JVM from the environment.
    conn_info = os.environ.get(
        "PYTHON_WORKER_FACTORY_SOCK_PATH", int(os.environ.get("PYTHON_WORKER_FACTORY_PORT", -1))
    )
    auth_secret = os.environ.get("PYTHON_WORKER_FACTORY_SECRET")
    sock_file, sock = local_connect_and_auth(conn_info, auth_secret)
    if timeout is None or timeout > 0:
        sock.settimeout(timeout)
    # TODO: Remove the following two lines and use `Process.pid()` when we drop JDK 8.
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    try:
        yield sock_file
    finally:
        sock_file.close()


def send_accumulator_updates(outfile: IO) -> None:
    """
    Send the accumulator updates back to JVM.
    """
    write_int(len(_accumulatorRegistry), outfile)
    for aid, accum in _accumulatorRegistry.items():
        pickleSer._write_with_length((aid, accum._value), outfile)


class _EndOfInput:
    """Last item ``read_in_thread`` queues, carrying the exception that ended the source."""

    __slots__ = ("error",)

    def __init__(self) -> None:
        self.error: Optional[BaseException] = None


@contextmanager
def read_in_thread(
    iterator: Iterator[Any], queue_depth: int
) -> Generator[Iterator[Any], None, None]:
    """
    Read ``iterator`` in a background thread and yield an iterator over the same items. The
    reader runs ahead of the caller by up to ``queue_depth`` queued items plus the one it is
    waiting to enqueue.

    This lets reading and deserializing the input overlap with the caller's work. Items that are
    themselves iterators (e.g. lazy per-group iterators that still read the input stream) are
    materialized into lists on the reader thread, so the caller never reads the stream. An
    exception raised by ``iterator`` is re-raised to the caller once the items before it are
    consumed.

    On exit the reader is stopped and joined for up to 5 seconds. The input is never closed, as
    the stream it reads may be reused by the next task of a reused worker; if the reader is still
    blocked reading it, a RuntimeWarning is emitted instead.
    """
    import queue
    import threading

    item_queue: queue.Queue = queue.Queue(maxsize=queue_depth)
    stop_event = threading.Event()
    end = _EndOfInput()

    def _read() -> None:
        # A put that blocks on a full queue is released by the drain on exit, after which the
        # stop check returns.
        try:
            for item in iterator:
                if hasattr(item, "__next__"):
                    item = list(item)
                item_queue.put(item)
                if stop_event.is_set():
                    return
        except BaseException as e:
            end.error = e
        finally:
            if not stop_event.is_set():
                item_queue.put(end)

    def _consume() -> Iterator[Any]:
        while True:
            item = item_queue.get()
            if item is end:
                if end.error is not None:
                    raise end.error
                return
            yield item

    reader = threading.Thread(target=_read, name="pyspark-pipelined-reader", daemon=True)
    reader.start()
    try:
        yield _consume()
    finally:
        # Stop the reader and drain the queue to unblock a pending put. A reader blocked in a
        # read of the input stream only sees the stop between items, so the join is bounded.
        stop_event.set()
        try:
            while not item_queue.empty():
                item_queue.get_nowait()
        except queue.Empty:
            pass
        reader.join(timeout=5)
        if reader.is_alive():
            warnings.warn(
                "pipelined reader thread did not exit within 5s; "
                "it may still be blocked in input_data_stream.read() and could "
                "read data intended for a subsequent reused-worker task. "
                "Consider disabling spark.python.worker.reuse if this recurs.",
                RuntimeWarning,
            )


class Conf:
    def __init__(self, infile_or_dict: Optional[Union[dict[str, str], IO]] = None) -> None:
        self._conf: dict[str, Any] = {}
        if infile_or_dict is not None:
            self.load(infile_or_dict)

    def load(self, infile_or_dict: Union[dict[str, str], IO]) -> None:
        if isinstance(infile_or_dict, dict):
            self._conf = infile_or_dict
        else:
            num_conf = read_int(infile_or_dict)
            # We do a sanity check here to reduce the possibility to stuck indefinitely
            # due to an invalid messsage. If the numer of configurations is obviously
            # wrong, we just raise an error directly.
            # We hand-pick the configurations to send to the worker so the number should
            # be very small (less than 100).
            if num_conf < 0 or num_conf > 10000:
                raise PySparkRuntimeError(
                    errorClass="PROTOCOL_ERROR",
                    messageParameters={
                        "failure": f"Invalid number of configurations: {num_conf}",
                    },
                )
            for _ in range(num_conf):
                k = utf8_deserializer.loads(infile_or_dict)
                v = utf8_deserializer.loads(infile_or_dict)
                self._conf[k] = v

    def get(self, key: str, default: Any = "", *, lower_str: bool = True) -> Any:
        val = self._conf.get(key, default)
        if isinstance(val, str) and lower_str:
            return val.lower()
        return val


class RunnerConf(Conf):
    @property
    def assign_cols_by_name(self) -> bool:
        return (
            self.get("spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName", "true")
            == "true"
        )

    @property
    def use_large_var_types(self) -> bool:
        return self.get("spark.sql.execution.arrow.useLargeVarTypes", "false") == "true"

    @property
    def use_legacy_pandas_udf_conversion(self) -> bool:
        return (
            self.get("spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled", "false")
            == "true"
        )

    @property
    def detailed_timing_enabled(self) -> bool:
        return self.get("spark.sql.execution.pythonUDF.detailedTiming.enabled", "false") == "true"

    @property
    def use_legacy_pandas_udtf_conversion(self) -> bool:
        return (
            self.get("spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled", "false")
            == "true"
        )

    @property
    def map_in_batch_legacy_accept_any_iterable(self) -> bool:
        return (
            self.get(
                "spark.sql.execution.pythonUDF.mapInBatch.legacy.acceptAnyIterable.enabled",
                "true",
            )
            == "true"
        )

    @property
    def binary_as_bytes(self) -> bool:
        return self.get("spark.sql.execution.pyspark.binaryAsBytes", "true") == "true"

    @property
    def safecheck(self) -> bool:
        return self.get("spark.sql.execution.pandas.convertToArrowArraySafely", "false") == "true"

    @property
    def int_to_decimal_coercion_enabled(self) -> bool:
        return (
            self.get("spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled", "false")
            == "true"
        )

    @property
    def prefer_int_ext_dtype(self) -> bool:
        return (
            self.get("spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype", "false")
            == "true"
        )

    @property
    def timezone(self) -> Optional[str]:
        return self.get("spark.sql.session.timeZone", None, lower_str=False)

    @property
    def window_bound_types(self) -> list[str]:
        # Per-UDF window frame bound type ("unbounded" or "bounded"), one entry per window UDF,
        # sent comma-separated by the window operator.
        return [t.strip().lower() for t in self.get("window_bound_types").split(",")]

    @property
    def arrow_max_records_per_batch(self) -> int:
        return int(self.get("spark.sql.execution.arrow.maxRecordsPerBatch", 10000))

    @property
    def arrow_max_bytes_per_batch(self) -> int:
        return int(self.get("spark.sql.execution.arrow.maxBytesPerBatch", 2**31 - 1))

    @property
    def python_udf_arrow_worker_output_batch_max_bytes(self) -> int:
        # -1 (the default) means no limit; only positive values enable output batch resizing.
        return int(self.get("spark.sql.execution.pythonUDF.arrow.workerOutputBatchMaxBytes", -1))

    @property
    def arrow_concurrency_level(self) -> int:
        return int(self.get("spark.sql.execution.pythonUDF.arrow.concurrency.level", -1))

    @property
    def udf_profiler(self) -> Optional[str]:
        return self.get("spark.sql.pyspark.udf.profiler", None)

    @property
    def udf_profiler_sampling_interval(self) -> int:
        return int(self.get("spark.sql.pyspark.udf.profiler.samplingInterval", 100))

    @property
    def data_source_profiler(self) -> Optional[str]:
        return self.get("spark.sql.pyspark.dataSource.profiler", None)


class EvalConf(Conf):
    @property
    def state_value_schema(self) -> Optional[StructType]:
        schema = self.get("state_value_schema", None)
        if schema is None:
            return None
        return StructType.fromJson(json.loads(schema))

    @property
    def grouping_key_schema(self) -> Optional[StructType]:
        schema = self.get("grouping_key_schema", None)
        if schema is None:
            return None
        return StructType.fromJson(json.loads(schema))

    @property
    def state_server_socket_port(self) -> Optional[int | str]:
        port = self.get("state_server_socket_port", None)
        try:
            return int(port)
        except ValueError:
            return port

    @property
    def state_server_auth_secret(self) -> Optional[str]:
        return self.get("state_server_auth_secret", None, lower_str=False)

    @property
    def input_type(self) -> Optional[DataType]:
        input_type = self.get("input_type", None, lower_str=False)
        if input_type is None:
            return None
        return _parse_datatype_json_string(input_type)

    @property
    def elementwise_nesting(self) -> Optional[list]:
        # Per-UDF nesting depth (parallel to the UDF list) for the element-wise lift: how many
        # ``array`` levels the worker flattens off each argument and re-nests onto the result. A UDF
        # in a single lambda is depth 1; one lifted out of nested lambdas is deeper. Absent/empty
        # means depth 1 for every UDF. See ExtractPythonUDFFromLambda.
        raw = self.get("elementwise_nesting", None)
        if raw is None or raw == "":
            return None
        return [int(x) for x in raw.split(",")]

    @property
    def table_arg_offsets(self) -> Optional[list[int]]:
        offsets = self.get("table_arg_offsets", None)
        if offsets is None:
            return None
        return [int(x) for x in offsets.split(",") if x]
