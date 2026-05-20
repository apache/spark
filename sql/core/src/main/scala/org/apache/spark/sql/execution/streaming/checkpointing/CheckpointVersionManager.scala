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

package org.apache.spark.sql.execution.streaming.checkpointing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
 * Central place for resolving streaming checkpoint component versions (offset log, etc.) for a
 * streaming query, based on the user-configured version and any feature-driven minimum required
 * version (e.g. streaming source evolution requires offset log VERSION_2).
 */
object CheckpointVersionManager {

  /**
   * Determines the offset log format version to use for a new streaming query.
   *
   * Returns the max of:
   *  - the version configured via [[SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION]]
   *  - the minimum required version implied by enabled features
   *
   * For existing queries, callers should instead read the version from the most recently written
   * offset log entry; this helper is only consulted when no prior entry exists.
   */
  def getOffsetLogVersion(sparkSessionForStream: SparkSession): Int = {
    val configuredVersion =
      sparkSessionForStream.conf.get(SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION)
    val minRequiredVersion = getMinRequiredOffsetLogVersion(sparkSessionForStream)
    math.max(configuredVersion, minRequiredVersion)
  }

  /**
   * Minimum offset log format version required by the features enabled on this session. Streaming
   * source evolution relies on the OffsetMap (sourceId -> offset) format, which is only available
   * in [[OffsetSeqLog.VERSION_2]].
   */
  private def getMinRequiredOffsetLogVersion(sparkSessionForStream: SparkSession): Int = {
    if (sparkSessionForStream.sessionState.conf.enableStreamingSourceEvolution) {
      OffsetSeqLog.VERSION_2
    } else {
      OffsetSeqLog.VERSION_1
    }
  }
}
