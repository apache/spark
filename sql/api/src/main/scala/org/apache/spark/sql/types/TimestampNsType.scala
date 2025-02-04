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
 * The timestamp_ns type represents a time instant in nanosecond precision. Valid range is
 * [0001-01-01T00:00:00.000000000Z, 9999-12-31T23:59:59.999999999Z] where the left/right-bound is a
 * date and time of the proleptic Gregorian calendar in UTC+00:00.
 *
 * Please use the singleton `DataTypes.TimestampNsType` to refer the type.
 */

@Unstable
class TimestampNsType private () extends DatetimeType {
  override def defaultSize: Int = 10

  override def typeName: String = "timestamp_ns"

  private[spark] override def asNullable: TimestampType = this
}

@UnStable
case object TimestampNsType extends TimestampNsType
