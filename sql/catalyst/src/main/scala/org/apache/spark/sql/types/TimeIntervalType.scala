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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflectionLock


/**
 * :: DeveloperApi ::
 * The data type representing time intervals. The time interval is stored internally as Long
 * in the number of microseconds.
 *
 * Note that time intervals are not comparable.
 *
 * Please use the singleton [[DataTypes.TimeIntervalType]].
 */
@DeveloperApi
class TimeIntervalType private() extends AtomicType {
  private[sql] type InternalType = Long

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  override def defaultSize: Int = 8

  private[spark] override def asNullable: TimeIntervalType = this
}

case object TimeIntervalType extends TimeIntervalType
