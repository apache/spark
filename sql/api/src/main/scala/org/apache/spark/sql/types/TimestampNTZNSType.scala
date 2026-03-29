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
 * The timestamp without time zone type represents a local time in nanosecond precision, which is
 * independent of time zone. Valid range is [1677-09-21T00:12:43.145224192,
 * 2262-04-11T23:47:16.854775807]. To represent an absolute point in time, use `TimestampNSType`
 * instead.
 *
 * Internally stored as a Long representing nanoseconds since the Unix epoch
 * (1970-01-01T00:00:00).
 *
 * Please use the singleton `DataTypes.TimestampNTZNSType` to refer the type.
 * @since 4.2.0
 */
@Unstable
class TimestampNTZNSType private () extends DatetimeType {

  /**
   * The default size of a value of the TimestampNTZNSType is 8 bytes.
   */
  override def defaultSize: Int = 8

  override def typeName: String = "timestamp_ntz_ns"

  override def equals(obj: Any): Boolean = obj.isInstanceOf[TimestampNTZNSType]

  override def hashCode(): Int = classOf[TimestampNTZNSType].getSimpleName.hashCode

  private[spark] override def asNullable: TimestampNTZNSType = this
}

/**
 * The companion case object and its class is separated so the companion object also subclasses
 * the TimestampNTZNSType class. Otherwise, the companion object would be of type
 * "TimestampNTZNSType$" in byte code. Defined with a private constructor so the companion object
 * is the only possible instantiation.
 *
 * @since 4.2.0
 */
@Unstable
case object TimestampNTZNSType extends TimestampNTZNSType
