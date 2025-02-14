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
from functools import wraps
import os
import sys
from typing import IO, Callable

from pyspark.accumulators import _accumulatorRegistry
from pyspark.serializers import (
    read_int,
    write_int,
    SpecialLengths,
)
from pyspark.util import handle_worker_exception, local_connect_and_auth
from pyspark.worker_util import (
    check_python_version,
    send_accumulator_updates,
    setup_broadcasts,
    setup_memory_limits,
    setup_spark_files,
)


F = Callable[[IO, IO], None]


def worker_main(func: F) -> F:
    @wraps(func)
    def main(infile: IO, outfile: IO) -> None:
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

            func(infile, outfile)
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
        java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
        auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
        (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
        main(sock_file, sock_file)

    return main
