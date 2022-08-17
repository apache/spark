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
 * The data type representing `Int128` values. Please use the singleton `DataTypes.Int128Type`.
 *
 * @since 3.4.0
 */
@Unstable
class Int128Type private() extends IntegralType {
  private[sql] type InternalType = Int128
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = Int128.Int128IsIntegral
  private[sql] val integral = Int128.Int128IsIntegral
  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * The default size of a value of the Int128Type is 16 bytes.
   */
  override def defaultSize: Int = 16

  override def simpleString: String = "int128"

  private[spark] override def asNullable: Int128Type = this
}

@Unstable
case object Int128Type extends Int128Type
