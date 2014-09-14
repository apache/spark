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
import os
import sys
import time
import socket
import traceback

from pyspark.accumulators import _accumulatorRegistry
from pyspark.broadcast import Broadcast, _broadcastRegistry
from pyspark.files import SparkFiles
from pyspark.serializers import write_with_length, write_int, read_long, \
    write_long, read_int, SpecialLengths, UTF8Deserializer, PickleSerializer, \
    CompressedSerializer
from pyspark import shuffle

pickleSer = PickleSerializer()
utf8_deserializer = UTF8Deserializer()


def report_times(outfile, boot, init, finish):
    write_int(SpecialLengths.TIMING_DATA, outfile)
    write_long(1000 * boot, outfile)
    write_long(1000 * init, outfile)
    write_long(1000 * finish, outfile)


def main(infile, outfile):
    try:
        boot_time = time.time()
        split_index = read_int(infile)
        if split_index == -1:  # for unit tests
            return

        # initialize global state
        shuffle.MemoryBytesSpilled = 0
        shuffle.DiskBytesSpilled = 0
        _accumulatorRegistry.clear()

        # fetch name of workdir
        spark_files_dir = utf8_deserializer.loads(infile)
        SparkFiles._root_directory = spark_files_dir
        SparkFiles._is_running_on_worker = True

        # fetch names of includes (*.zip and *.egg files) and construct PYTHONPATH
        sys.path.append(spark_files_dir)  # *.py files that were added will be copied here
        num_python_includes = read_int(infile)
        for _ in range(num_python_includes):
            filename = utf8_deserializer.loads(infile)
            sys.path.append(os.path.join(spark_files_dir, filename))

        # fetch names and values of broadcast variables
        num_broadcast_variables = read_int(infile)
        ser = CompressedSerializer(pickleSer)
        for _ in range(num_broadcast_variables):
            bid = read_long(infile)
            if bid >= 0:
                value = ser._read_with_length(infile)
                _broadcastRegistry[bid] = Broadcast(bid, value)
            else:
                bid = - bid - 1
                _broadcastRegistry.remove(bid)

        _accumulatorRegistry.clear()
        command = pickleSer._read_with_length(infile)
        (func, deserializer, serializer) = command
        init_time = time.time()
        iterator = deserializer.load_stream(infile)
        serializer.dump_stream(func(split_index, iterator), outfile)
    except Exception:
        try:
            write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, outfile)
            write_with_length(traceback.format_exc(), outfile)
            outfile.flush()
        except IOError:
            # JVM close the socket
            pass
        except Exception:
            # Write the error to stderr if it happened while serializing
            print >> sys.stderr, "PySpark worker failed with exception:"
            print >> sys.stderr, traceback.format_exc()
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


if __name__ == '__main__':
    # Read a local port to connect to from stdin
    java_port = int(sys.stdin.readline())
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", java_port))
    sock_file = sock.makefile("a+", 65536)
    main(sock_file, sock_file)
