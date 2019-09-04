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
 * The time type represents local time in microsecond precision.
 * Valid range is [00:00:00.000000, 23:59:59.999999].
 *
 * Please use the singleton `DataTypes.TimeType` to refer the type.
 * @since 3.0.0
 */
@Unstable
class TimeType private () extends AtomicType {

  /**
   * Internally, time is stored as the number of microseconds since 00:00:00.000000.
   */
  private[sql] type InternalType = Long

  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * The default size of a value of the TimeType is 8 bytes.
   */
  override def defaultSize: Int = 8

  private[spark] override def asNullable: TimeType = this
}

/**
 * The companion case object and its class is separated so the companion object also subclasses
 * the TimeType class. Otherwise, the companion object would be of type "TimeType$"
 * in byte code. Defined with a private constructor so the companion object is the only possible
 * instantiation.
 *
 * @since 3.0.0
 */
@Unstable
case object TimeType extends TimeType
