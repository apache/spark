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

package org.apache.spark.deploy.history

import com.codahale.metrics.{Gauge, MetricRegistry, Timer}

import org.apache.spark.metrics.source.Source

private[spark] class HistoryServerSource(val history: ApplicationHistoryProvider) extends Source {
  override val sourceName: String = "historyServer"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name("uncompleted"), new Gauge[Int] {
    override def getValue: Int = history.getUncompleted()
  })

  metricRegistry.register(MetricRegistry.name("applications"), new Gauge[Int] {
    override def getValue: Int = history.getListing().size
  })

  metricRegistry.register(MetricRegistry.name("underProcess"), new Gauge[Int] {
    override def getValue: Int = history.getEventLogsUnderProcess()
  })

  private val checkForLogsTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("checkForLogsTime"))

  private val cleanLogsTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("cleanLogsTime"))

  private val cleanDriverLogsTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("cleanDriverLogsTime"))

  private val compactTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("compactTime"))

  private val loadStoreTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("loadStoreTime"))

  def timeOfCheckForLogs[T](f: => T): T = {
    timeOfProcess(checkForLogsTimer)(f)
  }

  def timeOfCleanLogs[T](f: => T): T = {
    timeOfProcess(cleanLogsTimer)(f)
  }

  def timeOfCleanDriverLogs[T](f: => T): T = {
    timeOfProcess(cleanDriverLogsTimer)(f)
  }

  def timeOfCompact[T](f: => T): T = {
    timeOfProcess(compactTimer)(f)
  }

  def timeOfLoadStore[T](f: => T): T = {
    timeOfProcess(loadStoreTimer)(f)
  }

  private def timeOfProcess[T](t: Timer)(f: => T): T = {
    val timeCtx = t.time()
    try {
      f
    } finally {
      timeCtx.close()
    }
  }
}
