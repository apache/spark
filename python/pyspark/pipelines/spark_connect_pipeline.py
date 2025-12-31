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
from datetime import timezone
from typing import Any, Dict, Mapping, Iterator, Optional, cast, Sequence

import pyspark.sql.connect.proto as pb2
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.base import PySparkValueError
from pyspark.pipelines.logging_utils import log_with_provided_timestamp


def create_dataflow_graph(
    spark: SparkSession,
    default_catalog: Optional[str],
    default_database: Optional[str],
    sql_conf: Optional[Mapping[str, str]],
) -> str:
    """Create a dataflow graph in the Spark Connect server.

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
        elif "pipeline_event_result" not in result.keys():
            raise PySparkValueError(
                f"Pipeline logs stream handler received an unexpected result: {result}"
            )
        else:
            event = result["pipeline_event_result"].event
            dt = event.timestamp.ToDatetime().replace(tzinfo=timezone.utc)
            log_with_provided_timestamp(event.message, dt)


def start_run(
    spark: SparkSession,
    dataflow_graph_id: str,
    full_refresh: Optional[Sequence[str]],
    full_refresh_all: bool,
    refresh: Optional[Sequence[str]],
    dry: bool,
    storage: str,
) -> Iterator[Dict[str, Any]]:
    """Start a run of the dataflow graph in the Spark Connect server.

    :param spark: SparkSession.
    :param dataflow_graph_id: The ID of the dataflow graph to start.
    :param full_refresh: List of datasets to reset and recompute.
    :param full_refresh_all: Perform a full graph reset and recompute.
    :param refresh: List of datasets to update.
    :param dry: If true, the run will not actually execute any flows, but only validate the graph.
    :param storage: The storage location to store metadata such as streaming checkpoints.
    """
    inner_command = pb2.PipelineCommand.StartRun(
        dataflow_graph_id=dataflow_graph_id,
        full_refresh_selection=full_refresh or [],
        full_refresh_all=full_refresh_all,
        refresh_selection=refresh or [],
        dry=dry,
        storage=storage,
    )
    command = pb2.Command()
    command.pipeline_command.start_run.CopyFrom(inner_command)
    # Cast because mypy seems to think `spark`` is a function, not an object. Likely related to
    # SPARK-47544.
    return cast(Any, spark).client.execute_command_as_iterator(command)
