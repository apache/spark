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

package org.apache.spark.sql.streaming

import java.io.Serializable

import org.apache.spark.annotation.{Evolving, Experimental}

/**
 * Class used to provide access to timer values for processing and event time populated before
 * method invocations using the arbitrary state API v2.
 */
@Experimental
@Evolving
private[sql] trait TimerValues extends Serializable {

  /**
   * Get the current processing time as milliseconds in epoch time.
   * @note
   *   This will return a constant value throughout the duration of a streaming query trigger,
   *   even if the trigger is re-executed.
   */
  def getCurrentProcessingTimeInMs(): Long

  /**
   * Get the current event time watermark as milliseconds in epoch time.
   *
   * @note
   *   This can be called only when watermark is set before calling `transformWithState`.
   * @note
   *   The watermark gets propagated at the end of each query. As a result, this method will
   *   return 0 (1970-01-01T00:00:00) for the first micro-batch.
   */
  def getCurrentWatermarkInMs(): Long
}
