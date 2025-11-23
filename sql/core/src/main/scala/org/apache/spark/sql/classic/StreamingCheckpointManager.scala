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

package org.apache.spark.sql.classic

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.state.{OfflineStateRepartitionErrors, OfflineStateRepartitionRunner}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming

/** @inheritdoc */
private[spark] class StreamingCheckpointManager(
    sparkSession: SparkSession,
    sqlConf: SQLConf) extends streaming.StreamingCheckpointManager with Logging {

  /** @inheritdoc */
  override private[spark] def repartition(
      checkpointLocation: String,
      numPartitions: Int,
      enforceExactlyOnceSink: Boolean = true): Unit = {
    checkpointLocation match {
      case null =>
        throw OfflineStateRepartitionErrors.parameterIsNullError("checkpointLocation")
      case "" =>
        throw OfflineStateRepartitionErrors.parameterIsEmptyError("checkpointLocation")
      case _ => // Valid case, no action needed
    }

    if (numPartitions <= 0) {
      throw OfflineStateRepartitionErrors.parameterIsNotGreaterThanZeroError("numPartitions")
    }

    val runner = new OfflineStateRepartitionRunner(
      sparkSession,
      checkpointLocation,
      numPartitions,
      enforceExactlyOnceSink
    )
    runner.run()
  }
}
