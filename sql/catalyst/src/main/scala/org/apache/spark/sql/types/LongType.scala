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

import scala.math.{Integral, Numeric, Ordering}
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.ScalaReflectionLock

/**
 * The data type representing `Long` values. Please use the singleton `DataTypes.LongType`.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
class LongType private() extends IntegralType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "LongType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Long
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val numeric = implicitly[Numeric[Long]]
  private[sql] val integral = implicitly[Integral[Long]]
  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * The default size of a value of the LongType is 8 bytes.
   */
  override def defaultSize: Int = 8

  override def simpleString: String = "bigint"

  private[spark] override def asNullable: LongType = this
}

/**
 * @since 1.3.0
 */
@InterfaceStability.Stable
case object LongType extends LongType
