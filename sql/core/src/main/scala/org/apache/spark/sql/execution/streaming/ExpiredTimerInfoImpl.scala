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

import org.apache.spark.sql.streaming.{ExpiredTimerInfo, TimeoutMode}

/**
 * Class that provides a concrete implementation that can be used to provide access to expired
 * timer's expiry time. These values are only relevant if the ExpiredTimerInfo
 * is valid.
 * @param isValid - boolean to check if the provided ExpiredTimerInfo is valid
 * @param expiryTimeInMsOpt - option to expired timer's expiry time as milliseconds in epoch time
 */
class ExpiredTimerInfoImpl(
    isValid: Boolean,
    expiryTimeInMsOpt: Option[Long] = None,
    timeoutMode: TimeoutMode = TimeoutMode.NoTimeouts()) extends ExpiredTimerInfo {

  override def isValid(): Boolean = isValid

  override def getExpiryTimeInMs(): Long = expiryTimeInMsOpt.getOrElse(-1L)
}
