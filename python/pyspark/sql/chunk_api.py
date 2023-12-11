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

import pyarrow as pa

from pyspark.rdd import _create_local_socket
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.serializers import write_with_length
from pyspark.sql.pandas.serializers import ArrowStreamSerializer


def persist_dataframe_as_chunks(dataframe: DataFrame) -> list[str]:
    """
    Persist the spark dataframe as chunks, each chunk is an arrow batch.
    Return the list of chunk ids.
    This function is only available when it is called from spark driver process.
    """
    sc = SparkSession.getActiveSession().sparkContext
    return list(
        sc._jvm.org.apache.spark.sql.api.python.ChunkReadUtils
        .persistDataFrameAsArrowBatchChunks(dataframe._jdf)
    )


def unpersist_chunks(chunk_ids: list[str]) -> None:
    """
    Remove chunks by chunk ids.
    This function is only available when it is called from spark driver process.
    """
    sc = SparkSession.getActiveSession().sparkContext
    sc._jvm.org.apache.spark.sql.api.python.ChunkReadUtils.unpersistChunks(chunk_ids)


def read_chunk(chunk_id):
    """
    Read chunk by id, return arrow batch data of this chunk.
    You can call this function from spark driver, spark python UDF python,
    descendant process of spark driver, or descendant process of spark python UDF worker.
    """

    port = int(os.environ["PYSPARK_EXECUTOR_CACHED_ARROW_BATCH_SERVER_PORT"])
    auth_secret = os.environ["PYSPARK_EXECUTOR_CACHED_ARROW_BATCH_SERVER_SECRET"]

    sockfile = _create_local_socket((port, auth_secret))

    try:
        write_with_length(chunk_id.encode("utf-8"), sockfile)
        sockfile.flush()

        arrow_serializer = ArrowStreamSerializer()

        batch_stream = arrow_serializer.load_stream(sockfile)

        arrow_batch = list(batch_stream)[0]

        arrow_batch = pa.RecordBatch.from_arrays(
            [
                # This call actually reallocates the array
                pa.concat_arrays([array])
                for array in arrow_batch
            ],
            schema=arrow_batch.schema,
        )

        arrow_table = pa.Table.from_batches([arrow_batch])

        return arrow_table
    finally:
        sockfile.close()



