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
from pyspark.java_gateway import local_connect_and_auth
from pyspark.taskcontext import TaskContext
from pyspark.files import SparkFiles
from pyspark.rdd import PythonEvalType
from pyspark.serializers import write_with_length, write_int, read_long, read_bool, \
    write_long, read_int, SpecialLengths, UTF8Deserializer, PickleSerializer, \
    BatchedSerializer, ArrowStreamPandasSerializer
from pyspark.sql.types import to_arrow_type
from pyspark.util import fail_on_stopiteration
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


def read_command(serializer, file):
    command = serializer._read_with_length(file)
    if isinstance(command, Broadcast):
        command = serializer.loads(command.value)
    return command


def chain(f, g):
    """chain two functions together """
    return lambda *a: g(f(*a))


def wrap_udf(f, return_type):
    if return_type.needConversion():
        toInternal = return_type.toInternal
        return lambda *a: toInternal(f(*a))
    else:
        return lambda *a: f(*a)


def wrap_scalar_pandas_udf(f, return_type):
    arrow_return_type = to_arrow_type(return_type)

    def verify_result_length(*a):
        result = f(*a)
        if not hasattr(result, "__len__"):
            raise TypeError("Return type of the user-defined function should be "
                            "Pandas.Series, but is {}".format(type(result)))
        if len(result) != len(a[0]):
            raise RuntimeError("Result vector from pandas_udf was not the required length: "
                               "expected %d, got %d" % (len(a[0]), len(result)))
        return result

    return lambda *a: (verify_result_length(*a), arrow_return_type)


def wrap_grouped_map_pandas_udf(f, return_type):
    def wrapped(*series):
        import pandas as pd

        result = f(pd.concat(series, axis=1))
        if not isinstance(result, pd.DataFrame):
            raise TypeError("Return type of the user-defined function should be "
                            "pandas.DataFrame, but is {}".format(type(result)))
        if not len(result.columns) == len(return_type):
            raise RuntimeError(
                "Number of columns of the returned pandas.DataFrame "
                "doesn't match specified schema. "
                "Expected: {} Actual: {}".format(len(return_type), len(result.columns)))
        arrow_return_types = (to_arrow_type(field.dataType) for field in return_type)
        return [(result[result.columns[i]], arrow_type)
                for i, arrow_type in enumerate(arrow_return_types)]

    return wrapped


def read_single_udf(pickleSer, infile, eval_type):
    num_arg = read_int(infile)
    arg_offsets = [read_int(infile) for i in range(num_arg)]
    row_func = None
    for i in range(read_int(infile)):
        f, return_type = read_command(pickleSer, infile)
        if row_func is None:
            row_func = f
        else:
            row_func = chain(row_func, f)

    # make sure StopIteration's raised in the user code are not ignored
    # when they are processed in a for loop, raise them as RuntimeError's instead
    func = fail_on_stopiteration(row_func)

    # the last returnType will be the return type of UDF
    if eval_type == PythonEvalType.SQL_SCALAR_PANDAS_UDF:
        return arg_offsets, wrap_scalar_pandas_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF:
        return arg_offsets, wrap_grouped_map_pandas_udf(func, return_type)
    else:
        return arg_offsets, wrap_udf(func, return_type)


def read_udfs(pickleSer, infile, eval_type):
    num_udfs = read_int(infile)
    udfs = {}
    call_udf = []
    for i in range(num_udfs):
        arg_offsets, udf = read_single_udf(pickleSer, infile, eval_type)
        udfs['f%d' % i] = udf
        args = ["a[%d]" % o for o in arg_offsets]
        call_udf.append("f%d(%s)" % (i, ", ".join(args)))
    # Create function like this:
    #   lambda a: (f0(a0), f1(a1, a2), f2(a3))
    # In the special case of a single UDF this will return a single result rather
    # than a tuple of results; this is the format that the JVM side expects.
    mapper_str = "lambda a: (%s)" % (", ".join(call_udf))
    mapper = eval(mapper_str, udfs)

    func = lambda _, it: map(mapper, it)

    if eval_type in (PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                     PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF):
        timezone = utf8_deserializer.loads(infile)
        ser = ArrowStreamPandasSerializer(timezone)
    else:
        ser = BatchedSerializer(PickleSerializer(), 100)

    # profiling is not supported for UDF
    return func, None, ser, ser


def main(infile, outfile):
    try:
        boot_time = time.time()
        split_index = read_int(infile)
        if split_index == -1:  # for unit tests
            exit(-1)

        version = utf8_deserializer.loads(infile)
        if version != "%d.%d" % sys.version_info[:2]:
            raise Exception(("Python in worker has different version %s than that in " +
                             "driver %s, PySpark cannot run with different minor versions." +
                             "Please check environment variables PYSPARK_PYTHON and " +
                             "PYSPARK_DRIVER_PYTHON are correctly set.") %
                            ("%d.%d" % sys.version_info[:2], version))

        # initialize global state
        taskContext = TaskContext._getOrCreate()
        taskContext._stageId = read_int(infile)
        taskContext._partitionId = read_int(infile)
        taskContext._attemptNumber = read_int(infile)
        taskContext._taskAttemptId = read_long(infile)
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
                    assert(read_bid == bid)
                    _broadcastRegistry[bid] = \
                        Broadcast(sock_file=broadcast_sock_file)
                else:
                    path = utf8_deserializer.loads(infile)
                    _broadcastRegistry[bid] = Broadcast(path=path)

            else:
                bid = - bid - 1
                _broadcastRegistry.pop(bid)

        if needs_broadcast_decryption_server:
            broadcast_sock_file.write(b'1')
            broadcast_sock_file.close()

        _accumulatorRegistry.clear()
        eval_type = read_int(infile)
        if eval_type == PythonEvalType.NON_UDF:
            func, profiler, deserializer, serializer = read_command(pickleSer, infile)
        else:
            func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)

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
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    main(sock_file, sock_file)
