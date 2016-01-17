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

  protected val (pollPeriod, pollUnit) = {
    val PERIOD_KEY = "period"
    val DEFAULT_PERIOD = 10

    val UNIT_KEY = "unit"
    val DEFAULT_UNIT = TimeUnit.SECONDS

    val pp = Option(properties.getProperty(PERIOD_KEY)) match {
      case Some(s) => s.toInt
      case None => DEFAULT_PERIOD
    }
    val pu = Option(properties.getProperty(UNIT_KEY)) match {
      case Some(s) => TimeUnit.valueOf(s.toUpperCase)
      case None => DEFAULT_UNIT
    }

    // perform validation against the minimal 1 second period
    val MINIMAL_PERIOD = 1
    val period = DEFAULT_UNIT.convert(pp, pu)
    require(period > MINIMAL_PERIOD, s"Given polling period $pp $pu below the " +
        s"minimal polling period ($MINIMAL_PERIOD $DEFAULT_UNIT)")
    (pp, pu)
  }
}
