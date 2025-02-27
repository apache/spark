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

package org.apache.spark.sql.execution.streaming

import java.util.UUID

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.LogKeys.{CHECKPOINT_LOCATION, CHECKPOINT_ROOT, CONFIG, PATH}
import org.apache.spark.internal.MDC
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.{WriteToStream, WriteToStreamStatement}
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Replaces logical [[WriteToStreamStatement]] operator with an [[WriteToStream]] operator.
 */
object ResolveWriteToStream extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case s: WriteToStreamStatement =>
      val (resolvedCheckpointLocation, deleteCheckpointOnStop) = resolveCheckpointLocation(s)

      if (conf.adaptiveExecutionEnabled) {
        logWarning(log"${MDC(CONFIG, SQLConf.ADAPTIVE_EXECUTION_ENABLED.key)} " +
          log"is not supported in streaming DataFrames/Datasets and will be disabled.")
      }

      if (conf.isUnsupportedOperationCheckEnabled) {
        if (s.sink.isInstanceOf[SupportsWrite] && s.isContinuousTrigger) {
          UnsupportedOperationChecker.checkForContinuous(s.inputQuery, s.outputMode)
        } else {
          UnsupportedOperationChecker.checkForStreaming(s.inputQuery, s.outputMode)
        }
      }

      WriteToStream(
        s.userSpecifiedName.orNull,
        resolvedCheckpointLocation,
        s.sink,
        s.outputMode,
        deleteCheckpointOnStop,
        s.inputQuery,
        s.catalogAndIdent,
        s.catalogTable)
  }

  def resolveCheckpointLocation(s: WriteToStreamStatement): (String, Boolean) = {
    var deleteCheckpointOnStop = false
    val checkpointLocation = s.userSpecifiedCheckpointLocation.map { userSpecified =>
      new Path(userSpecified).toString
    }.orElse {
      conf.checkpointLocation.map { location =>
        new Path(location, s.userSpecifiedName.getOrElse(UUID.randomUUID().toString)).toString
      }
    }.getOrElse {
      if (s.useTempCheckpointLocation) {
        deleteCheckpointOnStop = true
        val tempDir = Utils.createTempDir(namePrefix = "temporary").getCanonicalPath
        logWarning(log"Temporary checkpoint location created which is deleted normally when" +
          log" the query didn't fail: ${MDC(PATH, tempDir)}. If it's required to delete " +
          log"it under any circumstances, please set " +
          log"${MDC(CONFIG, SQLConf.FORCE_DELETE_TEMP_CHECKPOINT_LOCATION.key)} to" +
          log" true. Important to know deleting temp checkpoint folder is best effort.")
        // SPARK-42676 - Write temp checkpoints for streaming queries to local filesystem
        // even if default FS is set differently.
        // This is a band-aid fix. Ideally we should convert `tempDir` to URIs, but there
        // are many legacy behaviors related to this.
        if (Utils.isWindows) {
          // For Windows local path, we can't treat that as a URI with file scheme.
          tempDir
        } else {
          "file://" + tempDir
        }
      } else {
        throw QueryCompilationErrors.checkpointLocationNotSpecifiedError()
      }
    }
    val fileManager = CheckpointFileManager.create(new Path(checkpointLocation), s.hadoopConf)

    // If offsets have already been created, we trying to resume a query.
    if (!s.recoverFromCheckpointLocation) {
      val checkpointPath = new Path(checkpointLocation, "offsets")
      if (fileManager.exists(checkpointPath)) {
        throw QueryCompilationErrors.recoverQueryFromCheckpointUnsupportedError(checkpointPath)
      }
    }

    val resolvedCheckpointRoot = {
      val checkpointPath = new Path(checkpointLocation)
      if (conf.getConf(SQLConf.STREAMING_CHECKPOINT_ESCAPED_PATH_CHECK_ENABLED)
        && StreamExecution.containsSpecialCharsInPath(checkpointPath)) {
        // In Spark 2.4 and earlier, the checkpoint path is escaped 3 times (3 `Path.toUri.toString`
        // calls). If this legacy checkpoint path exists, we will throw an error to tell the user
        // how to migrate.
        val legacyCheckpointDir =
        new Path(new Path(checkpointPath.toUri.toString).toUri.toString).toUri.toString
        val legacyCheckpointDirExists =
          try {
            fileManager.exists(new Path(legacyCheckpointDir))
          } catch {
            case NonFatal(e) =>
              // We may not have access to this directory. Don't fail the query if that happens.
              logWarning(e.getMessage, e)
              false
          }
        if (legacyCheckpointDirExists) {
          throw QueryExecutionErrors.legacyCheckpointDirectoryExistsError(
            checkpointPath, legacyCheckpointDir)
        }
      }
      val checkpointDir = fileManager.createCheckpointDirectory()
      checkpointDir.toString
    }
    logInfo(log"Checkpoint root ${MDC(CHECKPOINT_LOCATION, checkpointLocation)} " +
      log"resolved to ${MDC(CHECKPOINT_ROOT, resolvedCheckpointRoot)}.")
    (resolvedCheckpointRoot, deleteCheckpointOnStop)
  }
}

