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
package org.apache.spark.sql.catalyst.plans.logical

import java.util.Locale

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.streaming.TimeMode

/** TimeMode types used in transformWithState operator */
case object NoTime extends TimeMode

case object ProcessingTime extends TimeMode

case object EventTime extends TimeMode

object TimeModes {
  def apply(timeMode: String): TimeMode = {
    timeMode.toLowerCase(Locale.ROOT) match {
      case "none" =>
        NoTime
      case "processingtime" =>
        ProcessingTime
      case "eventtime" =>
        EventTime
      case _ => throw new SparkIllegalArgumentException(
        errorClass = "STATEFUL_PROCESSOR_UNKNOWN_TIME_MODE",
        messageParameters = Map("timeMode" -> timeMode))
    }
  }
}
