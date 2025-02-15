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

from typing import IO, Union

from pyspark.errors import PySparkAssertionError
from pyspark.serializers import (
    read_bool,
)
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
)
from pyspark.sql.datasource_internal import _streamReader
from pyspark.sql.types import (
    _parse_datatype_json_string,
    BinaryType,
    StructType,
)
from pyspark.sql.worker.internal.data_source_reader_info import DataSourceReaderInfo
from pyspark.sql.worker.internal.data_source_worker import worker_main
from pyspark.worker_util import (
    read_command,
    pickleSer,
    utf8_deserializer,
)


@worker_main
def main(infile: IO, outfile: IO) -> None:
    # Receive the data source instance.
    data_source = read_command(pickleSer, infile)
    if not isinstance(data_source, DataSource):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "a Python data source instance of type 'DataSource'",
                "actual": f"'{type(data_source).__name__}'",
            },
        )

    # Receive the data source output schema.
    schema_json = utf8_deserializer.loads(infile)
    schema = _parse_datatype_json_string(schema_json)
    if not isinstance(schema, StructType):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "an output schema of type 'StructType'",
                "actual": f"'{type(schema).__name__}'",
            },
        )

    is_streaming = read_bool(infile)

    # Instantiate data source reader.
    if is_streaming:
        reader: Union[DataSourceReader, DataSourceStreamReader] = _streamReader(data_source, schema)
    else:
        reader = data_source.reader(schema=schema)
        # Validate the reader.
        if not isinstance(reader, DataSourceReader):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "an instance of DataSourceReader",
                    "actual": f"'{type(reader).__name__}'",
                },
            )

    reader_info = DataSourceReaderInfo(reader=reader, data_source_name=data_source.name)
    pickleSer._write_with_length(reader_info, outfile)
