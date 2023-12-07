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

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


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
