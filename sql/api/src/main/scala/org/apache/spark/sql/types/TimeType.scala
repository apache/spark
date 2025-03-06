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

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.errors.DataTypeErrors

/**
 * The time type represents a time value with fields hour, minute, second, up to microseconds. The
 * range of times supported is 00:00:00.000000 to 23:59:59.999999.
 *
 * @param precision
 *   The time fractional seconds precision which indicates the number of decimal digits maintained
 *   following the decimal point in the seconds value. The supported range is [0, 6].
 *
 * @since 4.1.0
 */
@Unstable
case class TimeType(precision: Int) extends DatetimeType {

  if (precision < TimeType.MIN_PRECISION || precision > TimeType.MAX_PRECISION) {
    throw DataTypeErrors.unsupportedTimePrecisionError(precision)
  }

  /**
   * The default size of a value of the TimeType is 8 bytes.
   */
  override def defaultSize: Int = 8

  override def typeName: String = s"time($precision)"

  private[spark] override def asNullable: TimeType = this
}

object TimeType {
  val MIN_PRECISION: Int = 0
  val MICROS_PRECISION: Int = 6
  val MAX_PRECISION: Int = MICROS_PRECISION

  def apply(): TimeType = new TimeType(MICROS_PRECISION)
}
