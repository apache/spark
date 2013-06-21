"""
Worker that receives input from Piped RDD.
"""
import os
import sys
import time
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
    spark_files_dir = load_pickle(read_with_length(infile))
    SparkFiles._root_directory = spark_files_dir
    SparkFiles._is_running_on_worker = True
    sys.path.append(spark_files_dir)
    num_broadcast_variables = read_int(infile)
    for _ in range(num_broadcast_variables):
        bid = read_long(infile)
        value = read_with_length(infile)
        _broadcastRegistry[bid] = Broadcast(bid, load_pickle(value))
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
    # Redirect stdout to stderr so that users must return values from functions.
    old_stdout = os.fdopen(os.dup(1), 'w')
    os.dup2(2, 1)
    main(sys.stdin, old_stdout)
