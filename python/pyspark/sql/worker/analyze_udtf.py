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

import inspect
import os
import sys
from textwrap import dedent
from typing import Dict, List, IO, Tuple

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import (
    read_bool,
    read_int,
    write_int,
    write_with_length,
    SpecialLengths,
)
from pyspark.sql.functions import PartitioningColumn
from pyspark.sql.types import _parse_datatype_json_string, StructType
from pyspark.sql.udtf import AnalyzeArgument, AnalyzeResult
from pyspark.util import handle_worker_exception
from pyspark.worker_util import (
    check_python_version,
    read_command,
    pickleSer,
    send_accumulator_updates,
    setup_broadcasts,
    setup_memory_limits,
    setup_spark_files,
    utf8_deserializer,
)


def read_udtf(infile: IO) -> type:
    """Reads the Python UDTF and checks if its valid or not."""
    # Receive Python UDTF
    handler = read_command(pickleSer, infile)
    if not isinstance(handler, type):
        raise PySparkRuntimeError(
            f"Invalid UDTF handler type. Expected a class (type 'type'), but "
            f"got an instance of {type(handler).__name__}."
        )

    if not hasattr(handler, "analyze") or not isinstance(
        inspect.getattr_static(handler, "analyze"), staticmethod
    ):
        raise PySparkRuntimeError(
            "Failed to execute the user defined table function because it has not "
            "implemented the 'analyze' static method or specified a fixed "
            "return type during registration time. "
            "Please add the 'analyze' static method or specify the return type, "
            "and try the query again."
        )
    return handler


def read_arguments(infile: IO) -> Tuple[List[AnalyzeArgument], Dict[str, AnalyzeArgument]]:
    """Reads the arguments for `analyze` static method."""
    # Receive arguments
    num_args = read_int(infile)
    args: List[AnalyzeArgument] = []
    kwargs: Dict[str, AnalyzeArgument] = {}
    for _ in range(num_args):
        dt = _parse_datatype_json_string(utf8_deserializer.loads(infile))
        if read_bool(infile):  # is foldable
            value = pickleSer._read_with_length(infile)
            if dt.needConversion():
                value = dt.fromInternal(value)
        else:
            value = None
        is_table = read_bool(infile)  # is table argument
        argument = AnalyzeArgument(dataType=dt, value=value, isTable=is_table)

        is_named_arg = read_bool(infile)
        if is_named_arg:
            name = utf8_deserializer.loads(infile)
            kwargs[name] = argument
        else:
            args.append(argument)
    return args, kwargs


def main(infile: IO, outfile: IO) -> None:
    """
    Runs the Python UDTF's `analyze` static method.

    This process will be invoked from `UserDefinedPythonTableFunctionAnalyzeRunner.runInPython`
    in JVM and receive the Python UDTF and its arguments for the `analyze` static method,
    and call the `analyze` static method, and send back a AnalyzeResult as a result of the method.
    """
    try:
        check_python_version(infile)

        memory_limit_mb = int(os.environ.get("PYSPARK_PLANNER_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

        setup_spark_files(infile)
        setup_broadcasts(infile)

        _accumulatorRegistry.clear()

        handler = read_udtf(infile)
        args, kwargs = read_arguments(infile)

        error_prefix = f"Failed to evaluate the user-defined table function '{handler.__name__}'"

        def format_error(msg: str) -> str:
            return dedent(msg).replace('\n', ' ')

        # Check invariants about the 'analyze' and 'eval' methods before running them.
        def check_method_invariants_before_running(
            expected: inspect.FullArgSpec, method: str, is_static: bool
        ) -> None:
            num_expected_args = len(expected.args)
            num_provided_args = len(args) + len(kwargs)
            num_provided_non_kw_args = len(args)
            if not is_static:
                num_provided_args += 1
                num_provided_non_kw_args += 1
            if (
                expected.varargs is None
                and expected.varkw is None
                and expected.defaults is None
                and num_expected_args != num_provided_args
            ):
                # The UDTF call provided the wrong number of positional arguments.
                def arguments(num):
                    return f"{num} argument{'' if num == 1 else 's'}"

                raise PySparkValueError(
                    format_error(
                        f"""
                    {error_prefix} because its '{method}' method expects exactly
                    {arguments(num_expected_args)}, but the function call provided
                    {arguments(num_provided_args)} instead. Please update the query so that it
                    provides exactly {arguments(num_expected_args)}, or else update the table
                    function so that its '{method}' method accepts exactly
                    {arguments(num_provided_non_kw_args)}, and then try the query again."""
                    )
                )
            expected_arg_names = set(expected.args)
            provided_positional_arg_names = set(expected.args[: len(args)])
            for arg_name in kwargs.keys():
                if expected.varkw is None and arg_name not in expected_arg_names:
                    # The UDTF call provided a keyword argument whose name was not expected.
                    raise PySparkValueError(
                        format_error(
                            f"""
                        {error_prefix} because its '{method}' method expects arguments whose names
                        appear in the set ({', '.join(expected.args)}), but the function call
                        provided a keyword argument with unexpected name '{arg_name}' instead.
                        Please update the query so that it provides only keyword arguments whose
                        names appear in this set, or else update the table function so that its
                        '{method}' method accepts argument names including '{arg_name}', and then
                        try the query again."""
                        )
                    )
                elif arg_name in provided_positional_arg_names:
                    # The UDTF call provided a duplicate keyword argument when a value for that
                    # argument was already specified positionally.
                    raise PySparkValueError(
                        format_error(
                            f"""
                        {error_prefix} because the function call provided keyword argument
                        '{arg_name}' whose corresponding value was already specified positionally.
                        Please update the query so that it provides this argument's value exactly
                        once instead, and then try the query again."""
                        )
                    )

        check_method_invariants_before_running(
            inspect.getfullargspec(handler.analyze), "static analyze", is_static = True)
        if hasattr(handler, "eval"):
            check_method_invariants_before_running(
                inspect.getfullargspec(handler.eval), "eval", is_static = False)

        # Invoke the UDTF's 'analyze' method.
        result = handler.analyze(*args, **kwargs)  # type: ignore[attr-defined]

        # Check invariants about the 'analyze' method after running it.
        if not isinstance(result, AnalyzeResult):
            raise PySparkValueError(
                format_error(
                    f"""
                {error_prefix} because the static 'analyze' method expects a result of type
                pyspark.sql.udtf.AnalyzeResult, but instead this method returned a value of
                type: {type(result)}"""
                )
            )
        elif not isinstance(result.schema, StructType):
            raise PySparkValueError(
                format_error(
                    f"""
                {error_prefix} because the static 'analyze' method expects a result of type
                pyspark.sql.udtf.AnalyzeResult with a 'schema' field comprising a StructType,
                but the 'schema' field had the wrong type: {type(result.schema)}"""
                )
            )
        has_table_arg = (
            len([arg for arg in args if arg.isTable])
            + len([arg for arg in kwargs.items() if arg[-1].isTable])
        ) > 0
        if not has_table_arg and result.withSinglePartition:
            raise PySparkValueError(
                format_error(
                    f"""
                {error_prefix} because the static 'analyze' method returned an 'AnalyzeResult'
                object with the 'withSinglePartition' field set to 'true', but the function call
                did not provide any table argument. Please update the query so that it provides
                a table argument, or else update the table function so that its
                'analyze' method returns an 'AnalyzeResult' object with the
                'withSinglePartition' field set to 'false', and then try the query again."""
                )
            )
        elif not has_table_arg and len(result.partitionBy) > 0:
            raise PySparkValueError(
                format_error(
                    f"""
                {error_prefix} because the static 'analyze' method returned an 'AnalyzeResult'
                object with the 'partitionBy' list set to non-empty, but the function call
                did not provide any table argument. Please update the query so that it provides
                a table argument, or else update the table function so that its
                'analyze' method returns an 'AnalyzeResult' object with the
                'partitionBy' list set to empty, and then try the query again."""
                )
            )
        elif (
            hasattr(result, "partitionBy")
            and isinstance(result.partitionBy, (list, tuple))
            and (
                len(result.partitionBy) > 0
                and not all([isinstance(val, PartitioningColumn) for val in result.partitionBy])
            )
        ):
            raise PySparkValueError(
                format_error(
                    f"""
                {error_prefix} because the static 'analyze' method returned an 'AnalyzeResult'
                object with the 'partitionBy' field set to a value besides a list or tuple of
                'PartitioningColumn' objects. Please update the table function and then try the
                query again."""
                )
            )

        # Return the analyzed schema.
        write_with_length(result.schema.json().encode("utf-8"), outfile)
        # Return the pickled 'AnalyzeResult' class instance.
        pickleSer._write_with_length(result, outfile)
        # Return whether the "with single partition" property is requested.
        write_int(1 if result.withSinglePartition else 0, outfile)
        # Return the list of partitioning columns, if any.
        write_int(len(result.partitionBy), outfile)
        for partitioning_col in result.partitionBy:
            write_with_length(partitioning_col.name.encode("utf-8"), outfile)
        # Return the requested input table ordering, if any.
        write_int(len(result.orderBy), outfile)
        for ordering_col in result.orderBy:
            write_with_length(ordering_col.name.encode("utf-8"), outfile)
            write_int(1 if ordering_col.ascending else 0, outfile)
            if ordering_col.overrideNullsFirst is None:
                write_int(0, outfile)
            elif ordering_col.overrideNullsFirst:
                write_int(1, outfile)
            else:
                write_int(2, outfile)

    except BaseException as e:
        handle_worker_exception(e, outfile)
        sys.exit(-1)

    send_accumulator_updates(outfile)

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
    main(sock_file, sock_file)
