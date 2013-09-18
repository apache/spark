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
from base64 import standard_b64decode
# CloudPickler needs to be imported so that depicklers are registered using the
# copy_reg module.
from pyspark.accumulators import _accumulatorRegistry
from pyspark.broadcast import Broadcast, _broadcastRegistry
from pyspark.cloudpickle import CloudPickler
from pyspark.files import SparkFiles
from pyspark.serializers import write_with_length, read_with_length, write_int, \
    read_long, write_long, read_int, dump_pickle, load_pickle, read_from_pickle_file


def load_obj(infile):
    return load_pickle(standard_b64decode(infile.readline().strip()))


def report_times(outfile, boot, init, finish):
    write_int(-3, outfile)
    write_long(1000 * boot, outfile)
    write_long(1000 * init, outfile)
    write_long(1000 * finish, outfile)


def main(infile, outfile):
    boot_time = time.time()
    split_index = read_int(infile)
    if split_index == -1:  # for unit tests
        return

    # fetch name of workdir
    spark_files_dir = load_pickle(read_with_length(infile))
    SparkFiles._root_directory = spark_files_dir
    SparkFiles._is_running_on_worker = True

    # fetch names and values of broadcast variables
    num_broadcast_variables = read_int(infile)
    for _ in range(num_broadcast_variables):
        bid = read_long(infile)
        value = read_with_length(infile)
        _broadcastRegistry[bid] = Broadcast(bid, load_pickle(value))

    # fetch names of includes (*.zip and *.egg files) and construct PYTHONPATH
    sys.path.append(spark_files_dir) # *.py files that were added will be copied here
    num_python_includes =  read_int(infile)
    for _ in range(num_python_includes):
        sys.path.append(os.path.join(spark_files_dir, load_pickle(read_with_length(infile))))

    # now load function
    func = load_obj(infile)
    bypassSerializer = load_obj(infile)
    if bypassSerializer:
        dumps = lambda x: x
    else:
        dumps = dump_pickle
    init_time = time.time()
    iterator = read_from_pickle_file(infile)
    try:
        for obj in func(split_index, iterator):
            write_with_length(dumps(obj), outfile)
    except Exception as e:
        write_int(-2, outfile)
        write_with_length(traceback.format_exc(), outfile)
        sys.exit(-1)
    finish_time = time.time()
    report_times(outfile, boot_time, init_time, finish_time)
    # Mark the beginning of the accumulators section of the output
    write_int(-1, outfile)
    for aid, accum in _accumulatorRegistry.items():
        write_with_length(dump_pickle((aid, accum._value)), outfile)
    write_int(-1, outfile)


if __name__ == '__main__':
    # Read a local port to connect to from stdin
    java_port = int(sys.stdin.readline())
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", java_port))
    sock_file = sock.makefile("a+", 65536)
    main(sock_file, sock_file)
