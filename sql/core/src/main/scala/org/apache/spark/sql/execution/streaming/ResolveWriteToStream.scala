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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.{WriteToStream, WriteToStreamStatement}
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Replaces logical [[WriteToStreamStatement]] operator with an [[WriteToStream]] operator.
 */
object ResolveWriteToStream extends Rule[LogicalPlan] with SQLConfHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case s: WriteToStreamStatement =>
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
          val tempDir = Utils.createTempDir(namePrefix = s"temporary").getCanonicalPath
          logWarning("Temporary checkpoint location created which is deleted normally when" +
            s" the query didn't fail: $tempDir. If it's required to delete it under any" +
            s" circumstances, please set ${SQLConf.FORCE_DELETE_TEMP_CHECKPOINT_LOCATION.key} to" +
            s" true. Important to know deleting temp checkpoint folder is best effort.")
          tempDir
        } else {
          throw new AnalysisException(
            "checkpointLocation must be specified either " +
              """through option("checkpointLocation", ...) or """ +
              s"""SparkSession.conf.set("${SQLConf.CHECKPOINT_LOCATION.key}", ...)""")
        }
      }

      // If offsets have already been created, we trying to resume a query.
      if (!s.recoverFromCheckpointLocation) {
        val checkpointPath = new Path(checkpointLocation, "offsets")
        val fs = checkpointPath.getFileSystem(s.hadoopConf)
        if (fs.exists(checkpointPath)) {
          throw new AnalysisException(
            s"This query does not support recovering from checkpoint location. " +
              s"Delete $checkpointPath to start over.")
        }
      }

      if (conf.adaptiveExecutionEnabled) {
        logWarning(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} " +
          "is not supported in streaming DataFrames/Datasets and will be disabled.")
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
        checkpointLocation,
        s.sink,
        s.outputMode,
        deleteCheckpointOnStop,
        s.inputQuery)
  }
}

