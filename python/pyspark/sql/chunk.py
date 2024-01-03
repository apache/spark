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
from collections import namedtuple

import pyarrow as pa

from pyspark.rdd import _create_local_socket
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.serializers import read_with_length, write_with_length
from pyspark.sql.pandas.serializers import ArrowStreamSerializer
from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
from pyspark.errors import PySparkRuntimeError


ChunkMeta = namedtuple("ChunkMeta", ["id", "row_count", "byte_count"])

require_minimum_pyarrow_version()


def persistDataFrameAsChunks(dataframe: DataFrame, max_records_per_chunk: int) -> list[ChunkMeta]:
    """Persist and materialize the spark dataframe as chunks, each chunk is an arrow batch.
    It tries to persist data to spark worker memory firstly, if memory is not sufficient,
    then it fallbacks to persist spilled data to spark worker local disk.
    Return the list of tuple (chunk_id, chunk_row_count, chunk_byte_count).
    This function is only available when it is called from spark driver process.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    dataframe : DataFrame
        the spark DataFrame to be persisted as chunks
    max_records_per_chunk : int
        an integer representing max records per chunk

    Notes
    -----
    This API is a developer API.
    """
    spark = dataframe.sparkSession
    if spark is None:
        raise PySparkRuntimeError("Active spark session is required.")

    sc = spark.sparkContext
    if sc.getConf().get("spark.python.dataFrameChunkRead.enabled", "false").lower() != "true":
        raise PySparkRuntimeError(
            "In order to use 'persistDataFrameAsChunks' API, you must set spark "
            "cluster config 'spark.python.dataFrameChunkRead.enabled' to 'true'."
        )
    chunk_meta_list = list(
        sc._jvm.org.apache.spark.sql.api.python.ChunkReadUtils.persistDataFrameAsArrowBatchChunks(  # type: ignore[union-attr]
            dataframe._jdf, max_records_per_chunk
        )
    )
    return [
        ChunkMeta(java_chunk_meta.id(), java_chunk_meta.rowCount(), java_chunk_meta.byteCount())
        for java_chunk_meta in chunk_meta_list
    ]


def unpersistChunks(chunk_ids: list[str]) -> None:
    """Unpersist chunks by chunk ids.
    This function is only available when it is called from spark driver process.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    chunk_ids : list[str]
        A list of chunk ids

    Notes
    -----
    This API is a developer API.
    """
    sc = SparkSession.getActiveSession().sparkContext  # type: ignore[union-attr]
    (
        sc._jvm.org.apache.spark.sql.api.python.ChunkReadUtils.unpersistChunks(  # type: ignore[union-attr]
            chunk_ids
        )
    )


def readChunk(chunk_id: str) -> pa.Table:
    """Read chunk by id, return this chunk as an arrow table.
    You can call this function from spark driver, spark python UDF python,
    descendant process of spark driver, or descendant process of spark python UDF worker.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    chunk_id : str
        a string of chunk id

    Notes
    -----
    This API is a developer API.
    """

    if "PYSPARK_EXECUTOR_CACHED_ARROW_BATCH_SERVER_PORT" not in os.environ:
        raise PySparkRuntimeError(
            "In order to use dataframe chunk read API, you must set spark "
            "cluster config 'spark.python.dataFrameChunkRead.enabled' to 'true',"
            "and you must call 'readChunk' API in pyspark driver, pyspark UDF,"
            "descendant process of pyspark driver, or descendant process of pyspark "
            "UDF worker."
        )

    port = int(os.environ["PYSPARK_EXECUTOR_CACHED_ARROW_BATCH_SERVER_PORT"])
    auth_secret = os.environ["PYSPARK_EXECUTOR_CACHED_ARROW_BATCH_SERVER_SECRET"]

    sockfile = _create_local_socket((port, auth_secret))

    try:
        write_with_length(chunk_id.encode("utf-8"), sockfile)
        sockfile.flush()
        err_message = read_with_length(sockfile).decode("utf-8")

        if err_message != "ok":
            raise PySparkRuntimeError(f"Read chunk '{chunk_id}' failed (error: {err_message}).")

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
