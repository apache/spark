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

/**
 * The timestamp type represents a time instant in nanosecond precision. Valid range is
 * [1677-09-21T00:12:43.145224192, 2262-04-11T23:47:16.854775807] where the left/right-bound is a
 * date and time of the proleptic Gregorian calendar in UTC+00:00.
 *
 * Internally stored as a Long representing nanoseconds since the Unix epoch
 * (1970-01-01T00:00:00Z).
 *
 * Please use the singleton `DataTypes.TimestampNSType` to refer the type.
 * @since 4.2.0
 */
@Unstable
class TimestampNSType private () extends DatetimeType {

  /**
   * The default size of a value of the TimestampNSType is 8 bytes.
   */
  override def defaultSize: Int = 8

  override def typeName: String = "timestamp_ns"

  override def equals(obj: Any): Boolean = obj.isInstanceOf[TimestampNSType]

  override def hashCode(): Int = classOf[TimestampNSType].getSimpleName.hashCode

  private[spark] override def asNullable: TimestampNSType = this
}

/**
 * The companion case object and its class is separated so the companion object also subclasses
 * the TimestampNSType class. Otherwise, the companion object would be of type "TimestampNSType$"
 * in byte code. Defined with a private constructor so the companion object is the only possible
 * instantiation.
 *
 * @since 4.2.0
 */
@Unstable
case object TimestampNSType extends TimestampNSType
