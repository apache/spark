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
from pyspark.pipelines.output import (
    Output,
    MaterializedView,
    Table,
    Sink,
    StreamingTable,
    TemporaryView,
)
from pyspark.pipelines.flow import Flow
from pyspark.pipelines.graph_element_registry import GraphElementRegistry
from pyspark.pipelines.source_code_location import SourceCodeLocation
from pyspark.sql.connect.types import pyspark_types_to_proto_types
from pyspark.sql.types import StructType
from typing import Any, cast
import pyspark.sql.connect.proto as pb2
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context


class SparkConnectGraphElementRegistry(GraphElementRegistry):
    """Registers outputs and flows in a dataflow graph held in a Spark Connect server."""

    def __init__(self, spark: SparkSession, dataflow_graph_id: str) -> None:
        # Cast because mypy seems to think `spark`` is a function, not an object. Likely related to
        # SPARK-47544.
        self._spark = spark
        self._client = cast(Any, spark).client
        self._dataflow_graph_id = dataflow_graph_id

    def register_output(self, output: Output) -> None:
        table_details = None
        sink_details = None
        if isinstance(output, Table):
            if isinstance(output.schema, str):
                schema_string = output.schema
                schema_data_type = None
            elif isinstance(output.schema, StructType):
                schema_string = None
                schema_data_type = pyspark_types_to_proto_types(output.schema)
            else:
                schema_string = None
                schema_data_type = None

            table_details = pb2.PipelineCommand.DefineOutput.TableDetails(
                table_properties=output.table_properties,
                partition_cols=output.partition_cols,
                clustering_columns=output.cluster_by,
                format=output.format,
                # Even though schema_string is not required, the generated Python code seems to
                # erroneously think it is required.
                schema_string=schema_string,  # type: ignore[arg-type]
                schema_data_type=schema_data_type,
            )

            if isinstance(output, MaterializedView):
                output_type = pb2.OutputType.MATERIALIZED_VIEW
            elif isinstance(output, StreamingTable):
                output_type = pb2.OutputType.TABLE
            else:
                raise PySparkTypeError(
                    errorClass="UNSUPPORTED_PIPELINES_DATASET_TYPE",
                    messageParameters={"output_type": type(output).__name__},
                )
        elif isinstance(output, TemporaryView):
            output_type = pb2.OutputType.TEMPORARY_VIEW
            table_details = None
        elif isinstance(output, Sink):
            output_type = pb2.OutputType.SINK
            sink_details = pb2.PipelineCommand.DefineOutput.SinkDetails(
                options=output.options,
                format=output.format,
            )
        else:
            raise PySparkTypeError(
                errorClass="UNSUPPORTED_PIPELINES_DATASET_TYPE",
                messageParameters={"output_type": type(output).__name__},
            )

        inner_command = pb2.PipelineCommand.DefineOutput(
            dataflow_graph_id=self._dataflow_graph_id,
            output_name=output.name,
            output_type=output_type,
            comment=output.comment,
            sink_details=sink_details,
            table_details=table_details,
            source_code_location=source_code_location_to_proto(output.source_code_location),
        )

        command = pb2.Command()
        command.pipeline_command.define_output.CopyFrom(inner_command)
        self._client.execute_command(command)

    def register_flow(self, flow: Flow) -> None:
        with add_pipeline_analysis_context(
            spark=self._spark, dataflow_graph_id=self._dataflow_graph_id, flow_name=flow.name
        ):
            with block_spark_connect_execution_and_analysis():
                df = flow.func()
        relation = cast(ConnectDataFrame, df)._plan.plan(self._client)

        relation_flow_details = pb2.PipelineCommand.DefineFlow.WriteRelationFlowDetails(
            relation=relation,
        )

        inner_command = pb2.PipelineCommand.DefineFlow(
            dataflow_graph_id=self._dataflow_graph_id,
            flow_name=flow.name,
            target_dataset_name=flow.target,
            relation_flow_details=relation_flow_details,
            sql_conf=flow.spark_conf,
            source_code_location=source_code_location_to_proto(flow.source_code_location),
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


def source_code_location_to_proto(
    source_code_location: SourceCodeLocation,
) -> pb2.SourceCodeLocation:
    return pb2.SourceCodeLocation(
        file_name=source_code_location.filename, line_number=source_code_location.line_number
    )
