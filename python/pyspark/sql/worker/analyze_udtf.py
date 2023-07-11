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
import traceback
from typing import IO

from pyspark.errors import PySparkRuntimeError
from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import (
    read_bool,
    read_int,
    write_int,
    write_with_length,
    CPickleSerializer,
    SpecialLengths,
    UTF8Deserializer,
)
from pyspark.sql.types import _parse_datatype_json_string
from pyspark.util import try_simplify_traceback
from pyspark.worker import read_command

pickleSer = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


def main(infile: IO, outfile: IO) -> None:
    try:
        # Check Python version
        version = utf8_deserializer.loads(infile)
        if version != "%d.%d" % sys.version_info[:2]:
            raise PySparkRuntimeError(
                error_class="PYTHON_VERSION_MISMATCH",
                message_parameters={
                    "worker_version": str(sys.version_info[:2]),
                    "driver_version": str(version),
                },
            )

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
                "implemented the 'analyze' static function. "
                "Please add the 'analyze' static function and try the query again."
            )

        # receive arguments
        num_args = read_int(infile)
        args = []
        for _ in range(num_args):
            dt = _parse_datatype_json_string(utf8_deserializer.loads(infile))
            if read_bool(infile):  # is foldable
                literal = pickleSer._read_with_length(infile)
                if dt.needConversion():
                    literal = dt.fromInternal(literal)
            else:
                literal = None
            is_table = read_bool(infile)  # is table argument
            args.append(dict(data_type=dt, literal=literal, is_table=is_table))

        schema = handler.analyze(*args)  # type: ignore[attr-defined]
        write_with_length(schema.json().encode("utf-8"), outfile)
    except BaseException as e:
        try:
            exc_info = None
            if os.environ.get("SPARK_SIMPLIFIED_TRACEBACK", False):
                tb = try_simplify_traceback(sys.exc_info()[-1])  # type: ignore[arg-type]
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
    # TODO: Remove the following two lines and use `Process.pid()` when we drop JDK 8.
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
