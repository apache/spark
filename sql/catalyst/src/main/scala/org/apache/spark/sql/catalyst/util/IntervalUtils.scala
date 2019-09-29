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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.unsafe.types.CalendarInterval

object IntervalUtils {
  val MONTHS_PER_YEAR: Int = 12
  val YEARS_PER_MILLENNIUM: Int = 1000
  val YEARS_PER_CENTURY: Int = 100
  val YEARS_PER_DECADE: Int = 10

  def getYear(interval: CalendarInterval): Int = {
    interval.months / MONTHS_PER_YEAR
  }

  def getMillennium(interval: CalendarInterval): Int = {
    getYear(interval) / YEARS_PER_MILLENNIUM
  }

  def getCentury(interval: CalendarInterval): Int = {
    getYear(interval) / YEARS_PER_CENTURY
  }

  def getDecade(interval: CalendarInterval): Int = {
    getYear(interval) / YEARS_PER_DECADE
  }
}
