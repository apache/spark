/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.pipelines.graph

import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.SparkSession

sealed trait SystemMetadata {}

/**
 * Represents the system metadata associated with a [[Flow]].
 */
case class FlowSystemMetadata(
    context: PipelineUpdateContext,
    flow: Flow,
    graph: DataflowGraph
) extends SystemMetadata with Logging {

  /**
   * Returns the checkpoint root directory for a given flow
   * which is storage/_checkpoints/flow_destination_table/flow_name.
   * @return the checkpoint root directory for `flow`
   */
  def flowCheckpointsDirOpt(): Option[Path] = {
    Option(if (graph.table.contains(flow.destinationIdentifier) ||
      graph.sink.contains(flow.destinationIdentifier)) {
      val checkpointRoot = new Path(context.storageRoot, "_checkpoints")
      // Different tables in the pipeline can have flows with the same name, so we include
      // the table's fully qualified identifier in the path to avoid collisions.
      val flowTableId = tableIdentifierToPathString(flow.destinationIdentifier)
      val flowName = flow.identifier.table
      val checkpointDir = new Path(
        new Path(checkpointRoot, flowTableId),
        flowName
      )
      logInfo(
        log"Flow ${MDC(LogKeys.FLOW_NAME, flowName)} using checkpoint " +
          log"directory: ${MDC(LogKeys.CHECKPOINT_PATH, checkpointDir)}"
      )
      checkpointDir
    } else {
      throw new IllegalArgumentException(
        s"Flow ${flow.identifier} does not have a valid destination for checkpoints."
      )
    })
  }

  /**
   * Converts a TableIdentifier to a path string by joining its name parts with the path separator.
   */
  private def tableIdentifierToPathString(tableIdentifier: TableIdentifier): String = {
    tableIdentifier.nameParts.mkString(Path.SEPARATOR)
  }

  /** Returns the location for the most recent checkpoint of a given flow. */
  def latestCheckpointLocation: String = {
    val checkpointsDir = flowCheckpointsDirOpt().get
    SystemMetadata.getLatestCheckpointDir(checkpointsDir)
  }

  /**
   * Same as [[latestCheckpointLocation]] but returns None if the flow checkpoints directory
   * does not exist.
   */
  def latestCheckpointLocationOpt(): Option[String] = {
    flowCheckpointsDirOpt().map { flowCheckpointsDir =>
      SystemMetadata.getLatestCheckpointDir(flowCheckpointsDir)
    }
  }
}

object SystemMetadata {
  private def spark = SparkSession.getActiveSession.get

  /**
   * Finds the largest checkpoint version subdirectory path within a checkpoint directory, or
   * creates and returns a version 0 subdirectory path if no versions exist.
   * @param rootDir The root/parent directory where all the numbered checkpoint subdirectories are
   *                stored
   * @param createNewCheckpointDir If true, a new latest numbered checkpoint directory should be
   *                               created and returned
   * @return The string URI path to the latest checkpoint directory
   */
  def getLatestCheckpointDir(
      rootDir: Path,
      createNewCheckpointDir: Boolean = false
  ): String = {
    val fs = rootDir.getFileSystem(spark.sessionState.newHadoopConf())
    val defaultDir = new Path(rootDir, "0")
    val checkpoint = if (fs.exists(rootDir)) {
      val availableCheckpoints =
        fs.listStatus(rootDir)
          .toSeq
          .sortBy(fs => Try(fs.getPath.getName.toInt).getOrElse(-1))
      availableCheckpoints.lastOption
        .filter(fs => Try(fs.getPath.getName.toInt).isSuccess)
        .map(
          latestCheckpoint =>
            if (createNewCheckpointDir) {
              val incrementedLatestCheckpointDir =
                new Path(rootDir, Math.max(latestCheckpoint.getPath.getName.toInt + 1, 0).toString)
              fs.mkdirs(incrementedLatestCheckpointDir)
              incrementedLatestCheckpointDir
            } else {
              latestCheckpoint.getPath
            }
        )
        .getOrElse {
          fs.mkdirs(defaultDir)
          defaultDir
        }
    } else {
      defaultDir
    }
    checkpoint.toUri.toString
  }
}
