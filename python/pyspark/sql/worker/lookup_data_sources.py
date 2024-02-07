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
from importlib import import_module
from pkgutil import iter_modules
import os
import sys
from typing import IO

from pyspark.accumulators import _accumulatorRegistry
from pyspark.serializers import (
    read_int,
    write_int,
    write_with_length,
    SpecialLengths,
)
from pyspark.sql.datasource import DataSource
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
    Main method for looking up the available Python Data Sources in Python path.

    This process is invoked from the `UserDefinedPythonDataSourceLookupRunner.runInPython`
    method in `UserDefinedPythonDataSource.lookupAllDataSourcesInPython` when the first
    call related to Python Data Source happens via `DataSourceManager`.

    This is responsible for searching the available Python Data Sources so they can be
    statically registered automatically.
    """
    try:
        check_python_version(infile)

        memory_limit_mb = int(os.environ.get("PYSPARK_PLANNER_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

        setup_spark_files(infile)
        setup_broadcasts(infile)

        _accumulatorRegistry.clear()

        infos = {}
        for info in iter_modules():
            if info.name.startswith("pyspark_"):
                mod = import_module(info.name)
                if hasattr(mod, "DefaultSource") and issubclass(mod.DefaultSource, DataSource):
                    infos[mod.DefaultSource.name()] = mod.DefaultSource

        # Writes name -> pickled data source to JVM side to be registered
        # as a Data Source.
        write_int(len(infos), outfile)
        for name, dataSource in infos.items():
            write_with_length(name.encode("utf-8"), outfile)
            pickleSer._write_with_length(dataSource, outfile)

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
