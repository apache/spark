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

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.scheduler.SparkListenerEvent

/**
 * This class calculates the rate of events being accepted, via two phases reading:
 *
 * 1) Initialize available [[EventFilterBuilder]] instances, and replay the event log files with
 * builders, so that these builders can gather the information to create [[EventFilter]] instances.
 * 2) Initialize [[EventFilter]] instances from [[EventFilterBuilder]] instances, and replay the
 * event log files with filters. Counts the number of events vs accepted events and calculate
 * the rate.
 */
class EventLogFilterRateCalculator(fs: FileSystem) {
  def calculate(eventLogPaths: Seq[Path]): Double = {
    val builders = EventFilterBuilder.initializeBuilders(fs, eventLogPaths)
    doCalculate(eventLogPaths, builders.map(_.createFilter()))
  }

  /**
   * Exposed for tests - enable UTs to simply inject EventFilters instead of loading from
   * ServiceLoader which is subject to change on which modules are available on classloader.
   */
  private[spark] def doCalculate(eventLogPaths: Seq[Path], filters: Seq[EventFilter]): Double = {
    val calc = new Calculator(fs, filters)
    calc.calculate(eventLogPaths)
  }

  private class Calculator(fs: FileSystem, filters: Seq[EventFilter]) {
    def calculate(eventLogPaths: Seq[Path]): Double = {
      var allEvents = 0L
      var filteredInEvents = 0L

      eventLogPaths.foreach { path =>
        EventFilter.applyFilterToFile(fs, filters, path,
          onAccepted = (_, _) => {
            allEvents += 1
            filteredInEvents += 1
          },
          onRejected = (_, _) => allEvents += 1,
          onUnidentified = _ => {
            allEvents += 1
            filteredInEvents += 1
          }
        )
      }

      filteredInEvents.toDouble / allEvents
    }
  }
}
