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
import os
import sys
from typing import Any, IO

from pyspark.accumulators import _accumulatorRegistry
from pyspark.broadcast import Broadcast, _broadcastRegistry
from pyspark.errors import PySparkRuntimeError
from pyspark.files import SparkFiles
from pyspark.java_gateway import local_connect_and_auth
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
    if version != "%d.%d" % sys.version_info[:2]:
        raise PySparkRuntimeError(
            error_class="PYTHON_VERSION_MISMATCH",
            message_parameters={
                "worker_version": str(sys.version_info[:2]),
                "driver_version": str(version),
            },
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
    for (aid, accum) in _accumulatorRegistry.items():
        pickleSer._write_with_length((aid, accum._value), outfile)
