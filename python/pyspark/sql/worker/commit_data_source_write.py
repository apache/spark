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
import faulthandler
import os
import sys
from typing import IO

from pyspark.accumulators import _accumulatorRegistry
from pyspark.errors import PySparkAssertionError
from pyspark.serializers import (
    read_bool,
    read_int,
    write_int,
    SpecialLengths,
)
from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.util import handle_worker_exception, local_connect_and_auth
from pyspark.worker_util import (
    check_python_version,
    pickleSer,
    send_accumulator_updates,
    setup_broadcasts,
    setup_memory_limits,
    setup_spark_files,
)


def main(infile: IO, outfile: IO) -> None:
    """
    Main method for committing or aborting a data source write operation.

    This process is invoked from the `UserDefinedPythonDataSourceCommitRunner.runInPython`
    method in the BatchWrite implementation of the PythonTableProvider. It is
    responsible for invoking either the `commit` or the `abort` method on a data source
    writer instance, given a list of commit messages.
    """
    faulthandler_log_path = os.environ.get("PYTHON_FAULTHANDLER_DIR", None)
    try:
        if faulthandler_log_path:
            faulthandler_log_path = os.path.join(faulthandler_log_path, str(os.getpid()))
            faulthandler_log_file = open(faulthandler_log_path, "w")
            faulthandler.enable(file=faulthandler_log_file)

        check_python_version(infile)

        memory_limit_mb = int(os.environ.get("PYSPARK_PLANNER_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

        setup_spark_files(infile)
        setup_broadcasts(infile)

        _accumulatorRegistry.clear()

        # Receive the data source writer instance.
        writer = pickleSer._read_with_length(infile)
        assert isinstance(writer, DataSourceWriter)

        # Receive the commit messages.
        num_messages = read_int(infile)
        commit_messages = []
        for _ in range(num_messages):
            message = pickleSer._read_with_length(infile)
            if message is not None and not isinstance(message, WriterCommitMessage):
                raise PySparkAssertionError(
                    errorClass="DATA_SOURCE_TYPE_MISMATCH",
                    messageParameters={
                        "expected": "an instance of WriterCommitMessage",
                        "actual": f"'{type(message).__name__}'",
                    },
                )
            commit_messages.append(message)

        # Receive a boolean to indicate whether to invoke `abort`.
        abort = read_bool(infile)

        # Commit or abort the Python data source write.
        # Note the commit messages can be None if there are failed tasks.
        if abort:
            writer.abort(commit_messages)
        else:
            writer.commit(commit_messages)

        # Send a status code back to JVM.
        write_int(0, outfile)

    except BaseException as e:
        handle_worker_exception(e, outfile)
        sys.exit(-1)
    finally:
        if faulthandler_log_path:
            faulthandler.disable()
            faulthandler_log_file.close()
            os.remove(faulthandler_log_path)

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
    conn_info = os.environ.get(
        "PYTHON_WORKER_FACTORY_SOCK_PATH", int(os.environ.get("PYTHON_WORKER_FACTORY_PORT", -1))
    )
    auth_secret = os.environ.get("PYTHON_WORKER_FACTORY_SECRET")
    (sock_file, _) = local_connect_and_auth(conn_info, auth_secret)
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
