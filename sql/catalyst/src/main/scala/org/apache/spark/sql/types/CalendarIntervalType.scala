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
 * The data type representing calendar time intervals. The calendar time interval is stored
 * internally in two components: number of months the number of microseconds.
 *
 * Please use the singleton `DataTypes.CalendarIntervalType`.
 *
 * @note Calendar intervals are not comparable.
 *
 * @since 1.5.0
 */
@Stable
class CalendarIntervalType private() extends DataType {

  override def defaultSize: Int = 16

  private[spark] override def asNullable: CalendarIntervalType = this
}

/**
 * @since 1.5.0
 */
@Stable
case object CalendarIntervalType extends CalendarIntervalType
