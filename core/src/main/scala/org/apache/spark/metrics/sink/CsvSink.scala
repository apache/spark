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

import java.io.File
import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{CsvReporter, MetricRegistry}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.metrics.MetricsSystem

private[spark] class CsvSink(val property: Properties, val registry: MetricRegistry,
    securityMgr: SecurityManager, conf: SparkConf) extends Sink with Logging {
  val CSV_KEY_PERIOD = "period"
  val CSV_KEY_UNIT = "unit"
  val CSV_KEY_DIR = "directory"

  val CSV_DEFAULT_PERIOD = 10
  val CSV_DEFAULT_UNIT = "SECONDS"
  val CSV_DEFAULT_DIR = "/tmp/"

  val pollPeriod = Option(property.getProperty(CSV_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => CSV_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(CSV_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(CSV_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val pollDir = Option(property.getProperty(CSV_KEY_DIR)).getOrElse(CSV_DEFAULT_DIR) +
    conf.get("spark.app.uniqueName")

  val file= new File(pollDir)
  file.mkdirs

  logInfo("Dumping csv metrics to " + pollDir)

  val reporter: CsvReporter = CsvReporter.forRegistry(registry)
      .formatFor(Locale.US)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(file)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }
}

