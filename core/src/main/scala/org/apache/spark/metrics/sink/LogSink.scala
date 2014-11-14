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

import com.codahale.metrics.MetricRegistry
import com.shopify.metrics.reporting.LogReporter

import org.apache.spark.SecurityManager
import org.apache.spark.metrics.MetricsSystem

private[spark] class LogSink(val property: Properties, val registry: MetricRegistry,
  securityMgr: SecurityManager) extends Sink {
  
    val LOG_KEY_PERIOD = "period"
    val LOG_KEY_UNIT = "unit"
    val LOG_KEY_FILE  = "file"
    val LOG_KEY_MAX_FILE_SIZE = "maxFileSize"
    val LOG_KEY_MAX_BACKUP_INDEX = "maxFileIndex"
    
    val LOG_DEFAULT_PERIOD = 10
    val LOG_DEFAULT_UNIT = "SECONDS"
    val LOG_DEFAULT_FILE = "/tmp/metrics"
    val LOG_DEFAULT_MAX_FILE_SIZE = "50mb"
    val LOG_DEFAULT_BACKUP_INDEX = 10

    val pollPeriod = Option(property.getProperty(LOG_KEY_PERIOD)) match {
      case Some(s) => s.toInt
      case None => LOG_DEFAULT_PERIOD
    }

    val pollUnit: TimeUnit = Option(property.getProperty(LOG_KEY_UNIT)) match {
      case Some(s) => TimeUnit.valueOf(s.toUpperCase())
      case None => TimeUnit.valueOf(LOG_DEFAULT_UNIT)
    }

    MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

    val pollFile = Option(property.getProperty(LOG_KEY_FILE)) match {
      case Some(s) => s
      case None => LOG_DEFAULT_FILE
    }
    
    val maxFileSize = Option(property.getProperty(LOG_KEY_MAX_FILE_SIZE)) match {
      case Some(s) => s.toString
      case None => LOG_DEFAULT_MAX_FILE_SIZE
    }
    
    val maxBackupIndex = Option(property.getProperty(LOG_KEY_MAX_BACKUP_INDEX)) match {
      case Some(s) => s.toInt 
      case None => LOG_DEFAULT_BACKUP_INDEX
    }
    
    val reporter: LogReporter = LogReporter.forRegistry(registry)
        .formatFor(Locale.US)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build(pollFile, maxFileSize, maxBackupIndex)

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
