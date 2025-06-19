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

import org.apache.spark.annotation.Stable

/**
 * The data type representing calendar intervals. The calendar interval is stored internally in
 * three components: an integer value representing the number of `months` in this interval, an
 * integer value representing the number of `days` in this interval, a long value representing the
 * number of `microseconds` in this interval.
 *
 * Please use the singleton `DataTypes.CalendarIntervalType` to refer the type.
 *
 * @note
 *   Calendar intervals are not comparable.
 *
 * @since 1.5.0
 */
@Stable
class CalendarIntervalType private () extends DataType {

  override def defaultSize: Int = 16

  override def typeName: String = "interval"

  private[spark] override def asNullable: CalendarIntervalType = this
}

/**
 * @since 1.5.0
 */
@Stable
case object CalendarIntervalType extends CalendarIntervalType
