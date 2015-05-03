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
from __future__ import print_function
import os
import sys
import time
import socket
import traceback

from pyspark.accumulators import _accumulatorRegistry
from pyspark.broadcast import Broadcast, _broadcastRegistry
from pyspark.files import SparkFiles
from pyspark.serializers import write_with_length, write_int, read_long, \
    write_long, read_int, SpecialLengths, UTF8Deserializer, PickleSerializer
from pyspark import shuffle

pickleSer = PickleSerializer()
utf8_deserializer = UTF8Deserializer()


def report_times(outfile, boot, init, finish):
    write_int(SpecialLengths.TIMING_DATA, outfile)
    write_long(int(1000 * boot), outfile)
    write_long(int(1000 * init), outfile)
    write_long(int(1000 * finish), outfile)


def add_path(path):
    # worker can be used, so donot add path multiple times
    if path not in sys.path:
        # overwrite system packages
        sys.path.insert(1, path)


def main(infile, outfile):
    try:
        boot_time = time.time()
        split_index = read_int(infile)
        if split_index == -1:  # for unit tests
            exit(-1)

        # initialize global state
        shuffle.MemoryBytesSpilled = 0
        shuffle.DiskBytesSpilled = 0
        _accumulatorRegistry.clear()

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
        if sys.version > '3':
            import importlib
            importlib.invalidate_caches()

        # fetch names and values of broadcast variables
        num_broadcast_variables = read_int(infile)
        for _ in range(num_broadcast_variables):
            bid = read_long(infile)
            if bid >= 0:
                path = utf8_deserializer.loads(infile)
                _broadcastRegistry[bid] = Broadcast(path=path)
            else:
                bid = - bid - 1
                _broadcastRegistry.pop(bid)

        _accumulatorRegistry.clear()
        command = pickleSer._read_with_length(infile)
        if isinstance(command, Broadcast):
            command = pickleSer.loads(command.value)
        (func, profiler, deserializer, serializer), version = command
        if version != sys.version_info[:2]:
            raise Exception(("Python in worker has different version %s than that in " +
                            "driver %s, PySpark cannot run with different minor versions") %
                            (sys.version_info[:2], version))
        init_time = time.time()

        def process():
            iterator = deserializer.load_stream(infile)
            serializer.dump_stream(func(split_index, iterator), outfile)

        if profiler:
            profiler.profile(process)
        else:
            process()
    except Exception:
        try:
            write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, outfile)
            write_with_length(traceback.format_exc().encode("utf-8"), outfile)
        except IOError:
            # JVM close the socket
            pass
        except Exception:
            # Write the error to stderr if it happened while serializing
            print("PySpark worker failed with exception:", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
        exit(-1)
    finish_time = time.time()
    report_times(outfile, boot_time, init_time, finish_time)
    write_long(shuffle.MemoryBytesSpilled, outfile)
    write_long(shuffle.DiskBytesSpilled, outfile)

    # Mark the beginning of the accumulators section of the output
    write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
    write_int(len(_accumulatorRegistry), outfile)
    for (aid, accum) in _accumulatorRegistry.items():
        pickleSer._write_with_length((aid, accum._value), outfile)

    # check end of stream
    if read_int(infile) == SpecialLengths.END_OF_STREAM:
        write_int(SpecialLengths.END_OF_STREAM, outfile)
    else:
        # write a different value to tell JVM to not reuse this worker
        write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
        exit(-1)


if __name__ == '__main__':
    # Read a local port to connect to from stdin
    java_port = int(sys.stdin.readline())
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", java_port))
    sock_file = sock.makefile("a+", 65536)
    main(sock_file, sock_file)
