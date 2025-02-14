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

from typing import IO, List

from pyspark.errors import PySparkAssertionError, PySparkValueError
from pyspark.serializers import (
    UTF8Deserializer,
    read_int,
    write_int,
)
from pyspark.sql.datasource import DataSourceReader, EqualTo, Filter
from pyspark.sql.worker.internal.data_source_reader_info import DataSourceReaderInfo
from pyspark.sql.worker.internal.data_source_worker import worker_main
from pyspark.worker_util import (
    read_command,
    pickleSer,
)

utf8_deserializer = UTF8Deserializer()


@worker_main
def main(infile: IO, outfile: IO) -> None:
    # Receive the data source reader instance.
    reader_info = read_command(pickleSer, infile)
    if not isinstance(reader_info, DataSourceReaderInfo):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "a Python data source reader info of type 'DataSourceReaderInfo'",
                "actual": f"'{type(reader_info).__name__}'",
            },
        )

    reader = reader_info.reader
    if not isinstance(reader, DataSourceReader):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "a Python data source reader of type 'DataSourceReader'",
                "actual": f"'{type(reader_info).__name__}'",
            },
        )

    # Receive the pushdown filters.
    num_filters = read_int(infile)
    filters: List[Filter] = []
    for _ in range(num_filters):
        name = utf8_deserializer.loads(infile)
        if name == "EqualTo":
            num_parts = read_int(infile)
            column_path = tuple(utf8_deserializer.loads(infile) for _ in range(num_parts))
            value = read_int(infile)
            filter = EqualTo(column_path, value)
            filters.append(filter)
        else:
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_UNSUPPORTED_FILTER",
                messageParameters={
                    "name": name,
                },
            )

    # Push down the filters and get the indices of the unsupported filters.
    unsupported_filters = set(reader.pushdownFilters(list(filters)))
    supported_filter_indices = []
    for i, filter in enumerate(filters):
        if filter in unsupported_filters:
            unsupported_filters.remove(filter)
        else:
            supported_filter_indices.append(i)

    # If it returned any filters that are not in the original filters, raise an error.
    if len(unsupported_filters) > 0:
        raise PySparkValueError(
            errorClass="DATA_SOURCE_EXTRANEOUS_FILTERS",
            messageParameters={
                "type": type(reader).__name__,
                "input": str(list(filters)),
                "extraneous": str(list(unsupported_filters)),
            },
        )

    # pushdownFilters may mutate the reader, so we need to serialize it again.
    pickleSer._write_with_length(reader_info, outfile)

    # Return the supported filter indices.
    write_int(len(supported_filter_indices), outfile)
    for index in supported_filter_indices:
        write_int(index, outfile)
