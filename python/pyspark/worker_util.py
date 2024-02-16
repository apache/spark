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
from inspect import currentframe, getframeinfo
import os
import sys
from typing import Any, IO
import warnings

# 'resource' is a Unix specific module.
has_resource_module = True
try:
    import resource
except ImportError:
    has_resource_module = False

from pyspark.accumulators import _accumulatorRegistry
from pyspark.core.broadcast import Broadcast, _broadcastRegistry
from pyspark.errors import PySparkRuntimeError
from pyspark.core.files import SparkFiles
from pyspark.util import local_connect_and_auth
from pyspark.serializers import (
    read_bool,
    read_int,
    read_long,
    write_int,
    FramedSerializer,
    UTF8Deserializer,
    CPickleSerializer,
)

pickleSer = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def add_path(path: str) -> None:
    # worker can be used, so do not add path multiple times
    if path not in sys.path:
        # overwrite system packages
        sys.path.insert(1, path)


def read_command(serializer: FramedSerializer, file: IO) -> Any:
    command = serializer._read_with_length(file)
    if isinstance(command, Broadcast):
        command = serializer.loads(command.value)
    return command


def check_python_version(infile: IO) -> None:
    """
    Check the Python version between the running process and the one used to serialize the command.
    """
    version = utf8_deserializer.loads(infile)
    worker_version = "%d.%d" % sys.version_info[:2]
    if version != worker_version:
        raise PySparkRuntimeError(
            error_class="PYTHON_VERSION_MISMATCH",
            message_parameters={
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
            (soft_limit, hard_limit) = resource.getrlimit(total_memory)
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
            curent = currentframe()
            lineno = getframeinfo(curent).lineno + 1 if curent is not None else 0
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


def setup_spark_files(infile: IO) -> None:
    """
    Set up Spark files, archives, and pyfiles.
    """
    # fetch name of workdir
    spark_files_dir = utf8_deserializer.loads(infile)
    SparkFiles._root_directory = spark_files_dir
    SparkFiles._is_running_on_worker = True

    # fetch names of includes (*.zip and *.egg files) and construct PYTHONPATH
    add_path(spark_files_dir)  # *.py files that were added will be copied here
    num_python_includes = read_int(infile)
    for _ in range(num_python_includes):
        filename = utf8_deserializer.loads(infile)
        add_path(os.path.join(spark_files_dir, filename))

    importlib.invalidate_caches()


def setup_broadcasts(infile: IO) -> None:
    """
    Set up broadcasted variables.
    """
    # fetch names and values of broadcast variables
    needs_broadcast_decryption_server = read_bool(infile)
    num_broadcast_variables = read_int(infile)
    if needs_broadcast_decryption_server:
        # read the decrypted data from a server in the jvm
        port = read_int(infile)
        auth_secret = utf8_deserializer.loads(infile)
        (broadcast_sock_file, _) = local_connect_and_auth(port, auth_secret)

    for _ in range(num_broadcast_variables):
        bid = read_long(infile)
        if bid >= 0:
            if needs_broadcast_decryption_server:
                read_bid = read_long(broadcast_sock_file)
                assert read_bid == bid
                _broadcastRegistry[bid] = Broadcast(sock_file=broadcast_sock_file)
            else:
                path = utf8_deserializer.loads(infile)
                _broadcastRegistry[bid] = Broadcast(path=path)

        else:
            bid = -bid - 1
            _broadcastRegistry.pop(bid)

    if needs_broadcast_decryption_server:
        broadcast_sock_file.write(b"1")
        broadcast_sock_file.close()


def send_accumulator_updates(outfile: IO) -> None:
    """
    Send the accumulator updates back to JVM.
    """
    write_int(len(_accumulatorRegistry), outfile)
    for aid, accum in _accumulatorRegistry.items():
        pickleSer._write_with_length((aid, accum._value), outfile)
