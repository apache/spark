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

package org.apache.spark.sql.types

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * The data type representing day-time intervals. Currently, the day-time interval is concreted
 * internally using `CalendarInterval` which has three components: months, days, milliseconds, in
 * which we use:
 *    the integer value representing the number of days in this interval,
 *    the long value representing the number of microseconds in this interval.
 *
 * Please use the singleton `DataTypes.DayTimeIntervalType` to refer the type.
 *
 * @since 3.0.0
 */
@DeveloperApi
class DayTimeIntervalType extends DataType {

  // TODO: can be optimized to int + long size (12)
  override def defaultSize: Int = 16

  override def typeName: String = "interval day to second"

  val ordering: Ordering[CalendarInterval] = Ordering[CalendarInterval]

  override private[spark] def asNullable: DayTimeIntervalType = this
}

case object DayTimeIntervalType extends DayTimeIntervalType
