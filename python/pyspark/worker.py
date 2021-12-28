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
from inspect import currentframe, getframeinfo, getfullargspec
import importlib

# 'resource' is a Unix specific module.
has_resource_module = True
try:
    import resource
except ImportError:
    has_resource_module = False
import traceback
import warnings
import faulthandler

from pyspark.accumulators import _accumulatorRegistry
from pyspark.broadcast import Broadcast, _broadcastRegistry
from pyspark.java_gateway import local_connect_and_auth
from pyspark.taskcontext import BarrierTaskContext, TaskContext
from pyspark.files import SparkFiles
from pyspark.resource import ResourceInformation
from pyspark.rdd import PythonEvalType
from pyspark.serializers import (
    write_with_length,
    write_int,
    read_long,
    read_bool,
    write_long,
    read_int,
    SpecialLengths,
    UTF8Deserializer,
    CPickleSerializer,
    BatchedSerializer,
)
from pyspark.sql.pandas.serializers import (
    ArrowStreamPandasUDFSerializer,
    CogroupUDFSerializer,
    ArrowStreamUDFSerializer,
)
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import StructType
from pyspark.util import fail_on_stopiteration, try_simplify_traceback  # type: ignore
from pyspark import shuffle

pickleSer = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def report_times(outfile, boot, init, finish):
    write_int(SpecialLengths.TIMING_DATA, outfile)
    write_long(int(1000 * boot), outfile)
    write_long(int(1000 * init), outfile)
    write_long(int(1000 * finish), outfile)


def add_path(path):
    # worker can be used, so do not add path multiple times
    if path not in sys.path:
        # overwrite system packages
        sys.path.insert(1, path)


def read_command(serializer, file):
    command = serializer._read_with_length(file)
    if isinstance(command, Broadcast):
        command = serializer.loads(command.value)
    return command


def chain(f, g):
    """chain two functions together"""
    return lambda *a: g(f(*a))


def wrap_udf(f, return_type):
    if return_type.needConversion():
        toInternal = return_type.toInternal
        return lambda *a: toInternal(f(*a))
    else:
        return lambda *a: f(*a)


def wrap_scalar_pandas_udf(f, return_type):
    arrow_return_type = to_arrow_type(return_type)

    def verify_result_type(result):
        if not hasattr(result, "__len__"):
            pd_type = "Pandas.DataFrame" if type(return_type) == StructType else "Pandas.Series"
            raise TypeError(
                "Return type of the user-defined function should be "
                "{}, but is {}".format(pd_type, type(result))
            )
        return result

    def verify_result_length(result, length):
        if len(result) != length:
            raise RuntimeError(
                "Result vector from pandas_udf was not the required length: "
                "expected %d, got %d" % (length, len(result))
            )
        return result

    return lambda *a: (
        verify_result_length(verify_result_type(f(*a)), len(a[0])),
        arrow_return_type,
    )


def wrap_batch_iter_udf(f, return_type):
    arrow_return_type = to_arrow_type(return_type)

    def verify_result_type(result):
        if not hasattr(result, "__len__"):
            pd_type = "Pandas.DataFrame" if type(return_type) == StructType else "Pandas.Series"
            raise TypeError(
                "Return type of the user-defined function should be "
                "{}, but is {}".format(pd_type, type(result))
            )
        return result

    return lambda *iterator: map(
        lambda res: (res, arrow_return_type), map(verify_result_type, f(*iterator))
    )


def wrap_cogrouped_map_pandas_udf(f, return_type, argspec):
    def wrapped(left_key_series, left_value_series, right_key_series, right_value_series):
        import pandas as pd

        left_df = pd.concat(left_value_series, axis=1)
        right_df = pd.concat(right_value_series, axis=1)

        if len(argspec.args) == 2:
            result = f(left_df, right_df)
        elif len(argspec.args) == 3:
            key_series = left_key_series if not left_df.empty else right_key_series
            key = tuple(s[0] for s in key_series)
            result = f(key, left_df, right_df)
        if not isinstance(result, pd.DataFrame):
            raise TypeError(
                "Return type of the user-defined function should be "
                "pandas.DataFrame, but is {}".format(type(result))
            )
        if not len(result.columns) == len(return_type):
            raise RuntimeError(
                "Number of columns of the returned pandas.DataFrame "
                "doesn't match specified schema. "
                "Expected: {} Actual: {}".format(len(return_type), len(result.columns))
            )
        return result

    return lambda kl, vl, kr, vr: [(wrapped(kl, vl, kr, vr), to_arrow_type(return_type))]


def wrap_grouped_map_pandas_udf(f, return_type, argspec):
    def wrapped(key_series, value_series):
        import pandas as pd

        if len(argspec.args) == 1:
            result = f(pd.concat(value_series, axis=1))
        elif len(argspec.args) == 2:
            key = tuple(s[0] for s in key_series)
            result = f(key, pd.concat(value_series, axis=1))

        if not isinstance(result, pd.DataFrame):
            raise TypeError(
                "Return type of the user-defined function should be "
                "pandas.DataFrame, but is {}".format(type(result))
            )
        if not len(result.columns) == len(return_type):
            raise RuntimeError(
                "Number of columns of the returned pandas.DataFrame "
                "doesn't match specified schema. "
                "Expected: {} Actual: {}".format(len(return_type), len(result.columns))
            )
        return result

    return lambda k, v: [(wrapped(k, v), to_arrow_type(return_type))]


def wrap_grouped_agg_pandas_udf(f, return_type):
    arrow_return_type = to_arrow_type(return_type)

    def wrapped(*series):
        import pandas as pd

        result = f(*series)
        return pd.Series([result])

    return lambda *a: (wrapped(*a), arrow_return_type)


def wrap_window_agg_pandas_udf(f, return_type, runner_conf, udf_index):
    window_bound_types_str = runner_conf.get("pandas_window_bound_types")
    window_bound_type = [t.strip().lower() for t in window_bound_types_str.split(",")][udf_index]
    if window_bound_type == "bounded":
        return wrap_bounded_window_agg_pandas_udf(f, return_type)
    elif window_bound_type == "unbounded":
        return wrap_unbounded_window_agg_pandas_udf(f, return_type)
    else:
        raise RuntimeError("Invalid window bound type: {} ".format(window_bound_type))


def wrap_unbounded_window_agg_pandas_udf(f, return_type):
    # This is similar to grouped_agg_pandas_udf, the only difference
    # is that window_agg_pandas_udf needs to repeat the return value
    # to match window length, where grouped_agg_pandas_udf just returns
    # the scalar value.
    arrow_return_type = to_arrow_type(return_type)

    def wrapped(*series):
        import pandas as pd

        result = f(*series)
        return pd.Series([result]).repeat(len(series[0]))

    return lambda *a: (wrapped(*a), arrow_return_type)


def wrap_bounded_window_agg_pandas_udf(f, return_type):
    arrow_return_type = to_arrow_type(return_type)

    def wrapped(begin_index, end_index, *series):
        import pandas as pd

        result = []

        # Index operation is faster on np.ndarray,
        # So we turn the index series into np array
        # here for performance
        begin_array = begin_index.values
        end_array = end_index.values

        for i in range(len(begin_array)):
            # Note: Create a slice from a series for each window is
            #       actually pretty expensive. However, there
            #       is no easy way to reduce cost here.
            # Note: s.iloc[i : j] is about 30% faster than s[i: j], with
            #       the caveat that the created slices shares the same
            #       memory with s. Therefore, user are not allowed to
            #       change the value of input series inside the window
            #       function. It is rare that user needs to modify the
            #       input series in the window function, and therefore,
            #       it is be a reasonable restriction.
            # Note: Calling reset_index on the slices will increase the cost
            #       of creating slices by about 100%. Therefore, for performance
            #       reasons we don't do it here.
            series_slices = [s.iloc[begin_array[i] : end_array[i]] for s in series]
            result.append(f(*series_slices))
        return pd.Series(result)

    return lambda *a: (wrapped(*a), arrow_return_type)


def read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index):
    num_arg = read_int(infile)
    arg_offsets = [read_int(infile) for i in range(num_arg)]
    chained_func = None
    for i in range(read_int(infile)):
        f, return_type = read_command(pickleSer, infile)
        if chained_func is None:
            chained_func = f
        else:
            chained_func = chain(chained_func, f)

    if eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF:
        func = chained_func
    else:
        # make sure StopIteration's raised in the user code are not ignored
        # when they are processed in a for loop, raise them as RuntimeError's instead
        func = fail_on_stopiteration(chained_func)

    # the last returnType will be the return type of UDF
    if eval_type == PythonEvalType.SQL_SCALAR_PANDAS_UDF:
        return arg_offsets, wrap_scalar_pandas_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF:
        return arg_offsets, wrap_batch_iter_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF:
        return arg_offsets, wrap_batch_iter_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_MAP_ARROW_ITER_UDF:
        return arg_offsets, wrap_batch_iter_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF:
        argspec = getfullargspec(chained_func)  # signature was lost when wrapping it
        return arg_offsets, wrap_grouped_map_pandas_udf(func, return_type, argspec)
    elif eval_type == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
        argspec = getfullargspec(chained_func)  # signature was lost when wrapping it
        return arg_offsets, wrap_cogrouped_map_pandas_udf(func, return_type, argspec)
    elif eval_type == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF:
        return arg_offsets, wrap_grouped_agg_pandas_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF:
        return arg_offsets, wrap_window_agg_pandas_udf(func, return_type, runner_conf, udf_index)
    elif eval_type == PythonEvalType.SQL_BATCHED_UDF:
        return arg_offsets, wrap_udf(func, return_type)
    else:
        raise ValueError("Unknown eval type: {}".format(eval_type))


def read_udfs(pickleSer, infile, eval_type):
    runner_conf = {}

    if eval_type in (
        PythonEvalType.SQL_SCALAR_PANDAS_UDF,
        PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
        PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
        PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
        PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
        PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
        PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
    ):

        # Load conf used for pandas_udf evaluation
        num_conf = read_int(infile)
        for i in range(num_conf):
            k = utf8_deserializer.loads(infile)
            v = utf8_deserializer.loads(infile)
            runner_conf[k] = v

        # NOTE: if timezone is set here, that implies respectSessionTimeZone is True
        timezone = runner_conf.get("spark.sql.session.timeZone", None)
        safecheck = (
            runner_conf.get("spark.sql.execution.pandas.convertToArrowArraySafely", "false").lower()
            == "true"
        )
        # Used by SQL_GROUPED_MAP_PANDAS_UDF and SQL_SCALAR_PANDAS_UDF when returning StructType
        assign_cols_by_name = (
            runner_conf.get(
                "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName", "true"
            ).lower()
            == "true"
        )

        if eval_type == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
            ser = CogroupUDFSerializer(timezone, safecheck, assign_cols_by_name)
        elif eval_type == PythonEvalType.SQL_MAP_ARROW_ITER_UDF:
            ser = ArrowStreamUDFSerializer()
        else:
            # Scalar Pandas UDF handles struct type arguments as pandas DataFrames instead of
            # pandas Series. See SPARK-27240.
            df_for_struct = (
                eval_type == PythonEvalType.SQL_SCALAR_PANDAS_UDF
                or eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
                or eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF
            )
            ser = ArrowStreamPandasUDFSerializer(
                timezone, safecheck, assign_cols_by_name, df_for_struct
            )
    else:
        ser = BatchedSerializer(CPickleSerializer(), 100)

    num_udfs = read_int(infile)

    is_scalar_iter = eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
    is_map_pandas_iter = eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF
    is_map_arrow_iter = eval_type == PythonEvalType.SQL_MAP_ARROW_ITER_UDF

    if is_scalar_iter or is_map_pandas_iter or is_map_arrow_iter:
        if is_scalar_iter:
            assert num_udfs == 1, "One SCALAR_ITER UDF expected here."
        if is_map_pandas_iter:
            assert num_udfs == 1, "One MAP_PANDAS_ITER UDF expected here."
        if is_map_arrow_iter:
            assert num_udfs == 1, "One MAP_ARROW_ITER UDF expected here."

        arg_offsets, udf = read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=0)

        def func(_, iterator):
            num_input_rows = 0

            def map_batch(batch):
                nonlocal num_input_rows

                udf_args = [batch[offset] for offset in arg_offsets]
                num_input_rows += len(udf_args[0])
                if len(udf_args) == 1:
                    return udf_args[0]
                else:
                    return tuple(udf_args)

            iterator = map(map_batch, iterator)
            result_iter = udf(iterator)

            num_output_rows = 0
            for result_batch, result_type in result_iter:
                num_output_rows += len(result_batch)
                # This assert is for Scalar Iterator UDF to fail fast.
                # The length of the entire input can only be explicitly known
                # by consuming the input iterator in user side. Therefore,
                # it's very unlikely the output length is higher than
                # input length.
                assert (
                    is_map_pandas_iter or is_map_arrow_iter or num_output_rows <= num_input_rows
                ), "Pandas SCALAR_ITER UDF outputted more rows than input rows."
                yield (result_batch, result_type)

            if is_scalar_iter:
                try:
                    next(iterator)
                except StopIteration:
                    pass
                else:
                    raise RuntimeError("pandas iterator UDF should exhaust the input " "iterator.")

                if num_output_rows != num_input_rows:
                    raise RuntimeError(
                        "The length of output in Scalar iterator pandas UDF should be "
                        "the same with the input's; however, the length of output was %d and the "
                        "length of input was %d." % (num_output_rows, num_input_rows)
                    )

        # profiling is not supported for UDF
        return func, None, ser, ser

    def extract_key_value_indexes(grouped_arg_offsets):
        """
        Helper function to extract the key and value indexes from arg_offsets for the grouped and
        cogrouped pandas udfs. See BasePandasGroupExec.resolveArgOffsets for equivalent scala code.

        Parameters
        ----------
        grouped_arg_offsets:  list
            List containing the key and value indexes of columns of the
            DataFrames to be passed to the udf. It consists of n repeating groups where n is the
            number of DataFrames.  Each group has the following format:
                group[0]: length of group
                group[1]: length of key indexes
                group[2.. group[1] +2]: key attributes
                group[group[1] +3 group[0]]: value attributes
        """
        parsed = []
        idx = 0
        while idx < len(grouped_arg_offsets):
            offsets_len = grouped_arg_offsets[idx]
            idx += 1
            offsets = grouped_arg_offsets[idx : idx + offsets_len]
            split_index = offsets[0] + 1
            offset_keys = offsets[1:split_index]
            offset_values = offsets[split_index:]
            parsed.append([offset_keys, offset_values])
            idx += offsets_len
        return parsed

    if eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF:
        # We assume there is only one UDF here because grouped map doesn't
        # support combining multiple UDFs.
        assert num_udfs == 1

        # See FlatMapGroupsInPandasExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes
        arg_offsets, f = read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=0)
        parsed_offsets = extract_key_value_indexes(arg_offsets)

        # Create function like this:
        #   mapper a: f([a[0]], [a[0], a[1]])
        def mapper(a):
            keys = [a[o] for o in parsed_offsets[0][0]]
            vals = [a[o] for o in parsed_offsets[0][1]]
            return f(keys, vals)

    elif eval_type == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
        # We assume there is only one UDF here because cogrouped map doesn't
        # support combining multiple UDFs.
        assert num_udfs == 1
        arg_offsets, f = read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=0)

        parsed_offsets = extract_key_value_indexes(arg_offsets)

        def mapper(a):
            df1_keys = [a[0][o] for o in parsed_offsets[0][0]]
            df1_vals = [a[0][o] for o in parsed_offsets[0][1]]
            df2_keys = [a[1][o] for o in parsed_offsets[1][0]]
            df2_vals = [a[1][o] for o in parsed_offsets[1][1]]
            return f(df1_keys, df1_vals, df2_keys, df2_vals)

    else:
        udfs = []
        for i in range(num_udfs):
            udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))

        def mapper(a):
            result = tuple(f(*[a[o] for o in arg_offsets]) for (arg_offsets, f) in udfs)
            # In the special case of a single UDF this will return a single result rather
            # than a tuple of results; this is the format that the JVM side expects.
            if len(result) == 1:
                return result[0]
            else:
                return result

    func = lambda _, it: map(mapper, it)

    # profiling is not supported for UDF
    return func, None, ser, ser


def main(infile, outfile):
    faulthandler_log_path = os.environ.get("PYTHON_FAULTHANDLER_DIR", None)
    try:
        if faulthandler_log_path:
            faulthandler_log_path = os.path.join(faulthandler_log_path, str(os.getpid()))
            faulthandler_log_file = open(faulthandler_log_path, "w")
            faulthandler.enable(file=faulthandler_log_file)

        boot_time = time.time()
        split_index = read_int(infile)
        if split_index == -1:  # for unit tests
            sys.exit(-1)

        version = utf8_deserializer.loads(infile)
        if version != "%d.%d" % sys.version_info[:2]:
            raise RuntimeError(
                (
                    "Python in worker has different version %s than that in "
                    + "driver %s, PySpark cannot run with different minor versions. "
                    + "Please check environment variables PYSPARK_PYTHON and "
                    + "PYSPARK_DRIVER_PYTHON are correctly set."
                )
                % ("%d.%d" % sys.version_info[:2], version)
            )

        # read inputs only for a barrier task
        isBarrier = read_bool(infile)
        boundPort = read_int(infile)
        secret = UTF8Deserializer().loads(infile)

        # set up memory limits
        memory_limit_mb = int(os.environ.get("PYSPARK_EXECUTOR_MEMORY_MB", "-1"))
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
                lineno = (
                    getframeinfo(currentframe()).lineno + 1 if currentframe() is not None else 0
                )
                print(
                    warnings.formatwarning(
                        "Failed to set memory limit: {0}".format(e),
                        ResourceWarning,
                        __file__,
                        lineno,
                    ),
                    file=sys.stderr,
                )

        # initialize global state
        taskContext = None
        if isBarrier:
            taskContext = BarrierTaskContext._getOrCreate()
            BarrierTaskContext._initialize(boundPort, secret)
            # Set the task context instance here, so we can get it by TaskContext.get for
            # both TaskContext and BarrierTaskContext
            TaskContext._setTaskContext(taskContext)
        else:
            taskContext = TaskContext._getOrCreate()
        # read inputs for TaskContext info
        taskContext._stageId = read_int(infile)
        taskContext._partitionId = read_int(infile)
        taskContext._attemptNumber = read_int(infile)
        taskContext._taskAttemptId = read_long(infile)
        taskContext._cpus = read_int(infile)
        taskContext._resources = {}
        for r in range(read_int(infile)):
            key = utf8_deserializer.loads(infile)
            name = utf8_deserializer.loads(infile)
            addresses = []
            taskContext._resources = {}
            for a in range(read_int(infile)):
                addresses.append(utf8_deserializer.loads(infile))
            taskContext._resources[key] = ResourceInformation(name, addresses)

        taskContext._localProperties = dict()
        for i in range(read_int(infile)):
            k = utf8_deserializer.loads(infile)
            v = utf8_deserializer.loads(infile)
            taskContext._localProperties[k] = v

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

        _accumulatorRegistry.clear()
        eval_type = read_int(infile)
        if eval_type == PythonEvalType.NON_UDF:
            func, profiler, deserializer, serializer = read_command(pickleSer, infile)
        else:
            func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)

        init_time = time.time()

        def process():
            iterator = deserializer.load_stream(infile)
            out_iter = func(split_index, iterator)
            try:
                serializer.dump_stream(out_iter, outfile)
            finally:
                if hasattr(out_iter, "close"):
                    out_iter.close()

        if profiler:
            profiler.profile(process)
        else:
            process()

        # Reset task context to None. This is a guard code to avoid residual context when worker
        # reuse.
        TaskContext._setTaskContext(None)
        BarrierTaskContext._setTaskContext(None)
    except BaseException as e:
        try:
            exc_info = None
            if os.environ.get("SPARK_SIMPLIFIED_TRACEBACK", False):
                tb = try_simplify_traceback(sys.exc_info()[-1])
                if tb is not None:
                    e.__cause__ = None
                    exc_info = "".join(traceback.format_exception(type(e), e, tb))
            if exc_info is None:
                exc_info = traceback.format_exc()

            write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, outfile)
            write_with_length(exc_info.encode("utf-8"), outfile)
        except IOError:
            # JVM close the socket
            pass
        except BaseException:
            # Write the error to stderr if it happened while serializing
            print("PySpark worker failed with exception:", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
        sys.exit(-1)
    finally:
        if faulthandler_log_path:
            faulthandler.disable()
            faulthandler_log_file.close()
            os.remove(faulthandler_log_path)
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
        sys.exit(-1)


if __name__ == "__main__":
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    # TODO: Remove thw following two lines and use `Process.pid()` when we drop JDK 8.
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
