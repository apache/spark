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
package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

private[spark] trait HasPollingPeriod {
  def properties: Properties

  private val POLL_UNIT = TimeUnit.SECONDS
  private val MINIMAL_POLL_PERIOD = 1

  private def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }

  private val DEFAULT_PERIOD = 10
  private val DEFAULT_UNIT = "SECONDS"

  private val PERIOD_KEY = "period"
  private val UNIT_KEY = "unit"

  protected val pollPeriod = Option(properties.getProperty(PERIOD_KEY)) match {
    case Some(s) => s.toInt
    case None => DEFAULT_PERIOD
  }

  protected val pollUnit = Option(properties.getProperty(UNIT_KEY)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase)
    case None => TimeUnit.valueOf(DEFAULT_UNIT)
  }

  checkMinimalPollingPeriod(pollUnit, pollPeriod)
}
