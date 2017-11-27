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

import org.apache.spark.SecurityManager

/**
 * A metrics [[Sink]] which will write registered metrics to the specified directory with CSV
 * format.
 *
 * @param property [[CsvSink]] specific properties
 * @param registry A [[MetricRegistry]] can this sink to register
 * @param securityMgr A [[SecurityManager]] to check security related stuffs.
 */
private[spark] class CsvSink(
    property: Properties,
    registry: MetricRegistry,
    securityMgr: SecurityManager) extends Sink(property, registry) {
  val CSV_KEY_DIR = "directory"
  val CSV_DEFAULT_DIR = "/tmp/"

  private val pollDir = Option(property.getProperty(CSV_KEY_DIR)) match {
    case Some(s) => s
    case None => CSV_DEFAULT_DIR
  }

  private val reporter: CsvReporter = CsvReporter.forRegistry(registry)
    .formatFor(Locale.US)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build(new File(pollDir))

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}

