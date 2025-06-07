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
from typing import Any, Dict, Mapping, Iterator, Optional, cast

import pyspark.sql.connect.proto as pb2
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.base import PySparkValueError


def create_dataflow_graph(
    spark: SparkSession,
    default_catalog: Optional[str],
    default_database: Optional[str],
    sql_conf: Optional[Mapping[str, str]],
) -> str:
    """Create a dataflow graph in in the Spark Connect server.

    :returns: The ID of the created dataflow graph.
    """
    inner_command = pb2.PipelineCommand.CreateDataflowGraph(
        default_catalog=default_catalog,
        default_database=default_database,
        sql_conf=sql_conf,
    )
    command = pb2.Command()
    command.pipeline_command.create_dataflow_graph.CopyFrom(inner_command)
    # Cast because mypy seems to think `spark`` is a function, not an object. Likely related to
    # SPARK-47544.
    (_, properties, _) = cast(Any, spark).client.execute_command(command)
    return properties["pipeline_command_result"].create_dataflow_graph_result.dataflow_graph_id


def handle_pipeline_events(iter: Iterator[Dict[str, Any]]) -> None:
    """
    Prints out the pipeline events received from the Spark Connect server.
    """
    for result in iter:
        if "pipeline_command_result" in result.keys():
            # We expect to get a pipeline_command_result back in response to the initial StartRun
            # command.
            continue
        elif "pipeline_events_result" not in result.keys():
            raise PySparkValueError(
                "Pipeline logs stream handler received an unexpected result: " f"{result}"
            )
        else:
            for e in result["pipeline_events_result"].events:
                print(f"{e.timestamp}: {e.message}")


def start_run(spark: SparkSession, dataflow_graph_id: str) -> Iterator[Dict[str, Any]]:
    """Start a run of the dataflow graph in the Spark Connect server.

    :param dataflow_graph_id: The ID of the dataflow graph to start.
    """
    inner_command = pb2.PipelineCommand.StartRun(dataflow_graph_id=dataflow_graph_id)
    command = pb2.Command()
    command.pipeline_command.start_run.CopyFrom(inner_command)
    # Cast because mypy seems to think `spark`` is a function, not an object. Likely related to
    # SPARK-47544.
    return cast(Any, spark).client.execute_command_as_iterator(command)
