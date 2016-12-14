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

import scala.math.{Fractional, Numeric, Ordering}
import scala.math.Numeric.DoubleAsIfIntegral
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.util.Utils

/**
 * The data type representing `Double` values. Please use the singleton `DataTypes.DoubleType`.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
class DoubleType private() extends FractionalType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DoubleType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Double
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val numeric = implicitly[Numeric[Double]]
  private[sql] val fractional = implicitly[Fractional[Double]]
  private[sql] val ordering = new Ordering[Double] {
    override def compare(x: Double, y: Double): Int = Utils.nanSafeCompareDoubles(x, y)
  }
  private[sql] val asIntegral = DoubleAsIfIntegral

  /**
   * The default size of a value of the DoubleType is 8 bytes.
   */
  override def defaultSize: Int = 8

  private[spark] override def asNullable: DoubleType = this
}

/**
 * @since 1.3.0
 */
@InterfaceStability.Stable
case object DoubleType extends DoubleType
