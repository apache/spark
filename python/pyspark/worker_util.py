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
import json
import os
import sys
import warnings
from contextlib import contextmanager
from inspect import currentframe, getframeinfo
from typing import IO, Any, Generator, Optional, Union, overload

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
        importlib.invalidate_caches()


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
    def arrow_max_records_per_batch(self) -> int:
        return int(self.get("spark.sql.execution.arrow.maxRecordsPerBatch", 10000))

    @property
    def arrow_max_bytes_per_batch(self) -> int:
        return int(self.get("spark.sql.execution.arrow.maxBytesPerBatch", 2**31 - 1))

    @property
    def arrow_concurrency_level(self) -> int:
        return int(self.get("spark.sql.execution.pythonUDF.arrow.concurrency.level", -1))

    @property
    def udf_profiler(self) -> Optional[str]:
        return self.get("spark.sql.pyspark.udf.profiler", None)

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
