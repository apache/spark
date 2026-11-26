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
from typing import IO

from pyspark.errors import PySparkAssertionError
from pyspark.serializers import write_int
from pyspark.sql.datasource import DataSource
from pyspark.sql.worker.utils import worker_run
from pyspark.worker_util import (
    get_sock_file_to_executor,
    pickleSer,
    read_command,
)


def _main(infile: IO, outfile: IO) -> None:
    """
    Main method for finding out whether a Python data source class serves lookups.

    This process is invoked from `UserDefinedPythonDataSource.lookupSupport` in the JVM, once
    for a registered data source class, when Spark needs the capabilities of a table of it.

    The JVM sends the `DataSource` class. This process replies with two flags (1 or 0): whether
    the class overrides `lookupReader()`, and whether it overrides `reader()`. Both are found on
    the class itself: no data source is created, so no code of the data source runs, and Spark
    creates the data source in Python when it did before lookups existed.
    """
    data_source_cls = read_command(pickleSer, infile)
    if not (isinstance(data_source_cls, type) and issubclass(data_source_cls, DataSource)):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "a subclass of DataSource",
                "actual": f"'{type(data_source_cls).__name__}'",
            },
        )

    write_int(int(data_source_cls.lookupReader is not DataSource.lookupReader), outfile)
    write_int(int(data_source_cls.reader is not DataSource.reader), outfile)


def main(infile: IO, outfile: IO) -> None:
    worker_run(_main, infile, outfile)


if __name__ == "__main__":
    with get_sock_file_to_executor() as sock_file:
        main(sock_file, sock_file)
