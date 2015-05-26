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

import java.sql.Timestamp

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflectionLock


/**
 * :: DeveloperApi ::
 * The data type representing `java.sql.Timestamp` values.
 * Please use the singleton [[DataTypes.TimestampType]].
 *
 * @group dataType
 */
@DeveloperApi
class TimestampType private() extends AtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "TimestampType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Timestamp

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }

  private[sql] val ordering = new Ordering[InternalType] {
    def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }

  /**
   * The default size of a value of the TimestampType is 12 bytes.
   */
  override def defaultSize: Int = 12

  private[spark] override def asNullable: TimestampType = this
}

case object TimestampType extends TimestampType
