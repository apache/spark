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
from pathlib import Path

from pyspark.errors import PySparkTypeError
from pyspark.sql import SparkSession
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.pipelines.block_connect_access import block_spark_connect_execution_and_analysis
from pyspark.pipelines.dataset import (
    Dataset,
    MaterializedView,
    Table,
    StreamingTable,
    TemporaryView,
)
from pyspark.pipelines.flow import Flow
from pyspark.pipelines.graph_element_registry import GraphElementRegistry
from typing import Any, cast
import pyspark.sql.connect.proto as pb2


class SparkConnectGraphElementRegistry(GraphElementRegistry):
    """Registers datasets and flows in a dataflow graph held in a Spark Connect server."""

    def __init__(self, spark: SparkSession, dataflow_graph_id: str) -> None:
        # Cast because mypy seems to think `spark`` is a function, not an object. Likely related to
        # SPARK-47544.
        self._client = cast(Any, spark).client
        self._dataflow_graph_id = dataflow_graph_id

    def register_dataset(self, dataset: Dataset) -> None:
        if isinstance(dataset, Table):
            table_properties = dataset.table_properties
            partition_cols = dataset.partition_cols
            schema = None  # TODO
            format = dataset.format

            if isinstance(dataset, MaterializedView):
                dataset_type = pb2.DatasetType.MATERIALIZED_VIEW
            elif isinstance(dataset, StreamingTable):
                dataset_type = pb2.DatasetType.TABLE
            else:
                raise PySparkTypeError(
                    errorClass="UNSUPPORTED_PIPELINES_DATASET_TYPE",
                    messageParameters={"dataset_type": type(dataset).__name__},
                )
        elif isinstance(dataset, TemporaryView):
            table_properties = None
            partition_cols = None
            schema = None
            format = None
            dataset_type = pb2.DatasetType.TEMPORARY_VIEW
        else:
            raise PySparkTypeError(
                errorClass="UNSUPPORTED_PIPELINES_DATASET_TYPE",
                messageParameters={"dataset_type": type(dataset).__name__},
            )

        inner_command = pb2.PipelineCommand.DefineDataset(
            dataflow_graph_id=self._dataflow_graph_id,
            dataset_name=dataset.name,
            dataset_type=dataset_type,
            comment=dataset.comment,
            table_properties=table_properties,
            partition_cols=partition_cols,
            schema=schema,
            format=format,
        )
        command = pb2.Command()
        command.pipeline_command.define_dataset.CopyFrom(inner_command)
        self._client.execute_command(command)

    def register_flow(self, flow: Flow) -> None:
        with block_spark_connect_execution_and_analysis():
            df = flow.func()
        relation = cast(ConnectDataFrame, df)._plan.plan(self._client)

        inner_command = pb2.PipelineCommand.DefineFlow(
            dataflow_graph_id=self._dataflow_graph_id,
            flow_name=flow.name,
            target_dataset_name=flow.target,
            relation=relation,
            sql_conf=flow.spark_conf,
            once=flow.once,
        )
        command = pb2.Command()
        command.pipeline_command.define_flow.CopyFrom(inner_command)
        self._client.execute_command(command)

    def register_sql(self, sql_text: str, file_path: Path) -> None:
        inner_command = pb2.PipelineCommand.DefineSqlGraphElements(
            dataflow_graph_id=self._dataflow_graph_id,
            sql_text=sql_text,
            sql_file_path=str(file_path),
        )
        command = pb2.Command()
        command.pipeline_command.define_sql_graph_elements.CopyFrom(inner_command)
        self._client.execute_command(command)
