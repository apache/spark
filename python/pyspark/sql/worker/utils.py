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

import os
import sys
from typing import Callable, IO, Optional

from pyspark.accumulators import (
    _accumulatorRegistry,
    _deserialize_accumulator,
    SpecialAccumulatorIds,
)
from pyspark.sql.profiler import ProfileResultsParam, WorkerPerfProfiler, WorkerMemoryProfiler
from pyspark.serializers import (
    read_int,
    write_int,
    SpecialLengths,
)
from pyspark.util import (
    start_faulthandler_periodic_traceback,
    handle_worker_exception,
    with_faulthandler,
)
from pyspark.worker_util import (
    check_python_version,
    send_accumulator_updates,
    setup_memory_limits,
    setup_spark_files,
    setup_broadcasts,
    Conf,
)


class RunnerConf(Conf):
    @property
    def profiler(self) -> Optional[str]:
        return self.get("spark.sql.pyspark.dataSource.profiler", None)


@with_faulthandler
def worker_run(main: Callable, infile: IO, outfile: IO) -> None:
    try:
        check_python_version(infile)

        start_faulthandler_periodic_traceback()

        memory_limit_mb = int(os.environ.get("PYSPARK_PLANNER_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

        setup_spark_files(infile)
        setup_broadcasts(infile)
        conf = RunnerConf(infile)

        _accumulatorRegistry.clear()
        accumulator = _deserialize_accumulator(
            SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
        )

        worker_module = main.__module__.split(".")[-1]
        if conf.profiler == "perf":
            with WorkerPerfProfiler(accumulator, worker_module):
                main(infile, outfile)
        elif conf.profiler == "memory":
            with WorkerMemoryProfiler(accumulator, worker_module, main):
                main(infile, outfile)
        else:
            main(infile, outfile)
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
