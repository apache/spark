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

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.Unstable

/**
 * The type represents day-time intervals of the SQL standard. A day-time interval is made up
 * of a contiguous subset of the following fields:
 *   - SECOND, seconds within minutes and possibly fractions of a second [0..59.999999],
 *   - MINUTE, minutes within hours [0..59],
 *   - HOUR, hours within days [0..23],
 *   - DAY, days in the range [0..106751991].
 *
 * `DayTimeIntervalType` represents positive as well as negative day-time intervals.
 *
 * Please use the singleton `DataTypes.DayTimeIntervalType` to refer the type.
 *
 * @since 3.2.0
 */
@Unstable
class DayTimeIntervalType private() extends AtomicType {
  /**
   * Internally, values of day-time intervals are stored in `Long` values as amount of time in terms
   * of microseconds that are calculated by the formula:
   *   -/+ (24*60*60 * DAY + 60*60 * HOUR + 60 * MINUTE + SECOND) * 1000000
   */
  private[sql] type InternalType = Long

  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * The day-time interval type has constant precision. A value of the type always occupies 8 bytes.
   * The DAY field is constrained by the upper bound 106751991 to fit to `Long`.
   */
  override def defaultSize: Int = 8

  private[spark] override def asNullable: DayTimeIntervalType = this

  override def typeName: String = "interval day to second"
}

/**
 * The companion case object and its class is separated so the companion object also subclasses
 * the DayTimeIntervalType class. Otherwise, the companion object would be of type
 * "DayTimeIntervalType$" in byte code. Defined with a private constructor so the companion object
 * is the only possible instantiation.
 *
 * @since 3.2.0
 */
@Unstable
case object DayTimeIntervalType extends DayTimeIntervalType
