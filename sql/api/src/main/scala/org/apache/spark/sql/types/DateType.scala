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
 * The date type represents a valid date in the proleptic Gregorian calendar. Valid range is
 * [0001-01-01, 9999-12-31].
 *
 * Please use the singleton `DataTypes.DateType` to refer the type.
 * @since 1.3.0
 */
@Stable
class DateType private () extends DatetimeType {

  /**
   * The default size of a value of the DateType is 4 bytes.
   */
  override def defaultSize: Int = 4

  private[spark] override def asNullable: DateType = this
}

/**
 * The companion case object and the DateType class is separated so the companion object also
 * subclasses the class. Otherwise, the companion object would be of type "DateType$" in byte
 * code. The DateType class is defined with a private constructor so its companion object is the
 * only possible instantiation.
 *
 * @since 1.3.0
 */
@Stable
case object DateType extends DateType
