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
package org.apache.spark.sql.execution.streaming.utils

import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.streaming.StreamingSourceIdentifyingName

object StreamingUtils {
  /**
   * Resolves a checkpoint location to a fully qualified path.
   *
   * Converts relative or unqualified paths to fully qualified paths using the
   * file system's URI and working directory.
   *
   * @param hadoopConf Hadoop configuration to access the file system
   * @param checkpointLocation The checkpoint location path (may be relative or unqualified)
   * @return Fully qualified checkpoint location URI as a string
   */
  def resolvedCheckpointLocation(hadoopConf: Configuration, checkpointLocation: String): String = {
    val checkpointPath = new Path(checkpointLocation)
    val fs = checkpointPath.getFileSystem(hadoopConf)
    checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
  }

  /**
   * Computes the metadata path for a streaming source based on its identifying name.
   *
   * Named sources (UserProvided/FlowAssigned) use stable name-based paths, enabling
   * source evolution (reordering, adding, or removing sources without breaking state).
   * Unassigned sources use sequential IDs for backward compatibility.
   *
   * Examples:
   *   - UserProvided("mySource") => "$checkpointRoot/sources/mySource"
   *   - FlowAssigned("source_1") => "$checkpointRoot/sources/source_1"
   *   - Unassigned => "$checkpointRoot/sources/0" (increments nextSourceId)
   *
   * @param sourceIdentifyingName The source's identifying name
   *                              (UserProvided, FlowAssigned, or Unassigned)
   * @param nextSourceId AtomicLong tracking the next positional source ID for Unassigned sources
   * @param resolvedCheckpointRoot The resolved checkpoint root path
   * @return The computed metadata path string
   */
  def getMetadataPath(
      sourceIdentifyingName: StreamingSourceIdentifyingName,
      nextSourceId: AtomicLong,
      resolvedCheckpointRoot: String): String = {
    sourceIdentifyingName.nameOpt match {
      case Some(name) =>
        // User-provided and flow-assigned names use named paths
        s"$resolvedCheckpointRoot/sources/$name"
      case None =>
        // Unassigned sources get sequential IDs assigned here
        s"$resolvedCheckpointRoot/sources/${nextSourceId.getAndIncrement()}"
    }
  }
}
