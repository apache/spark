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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalStringType}
import org.apache.spark.unsafe.types.UTF8String

@Experimental
case class CharType(length: Int) extends AtomicType {
  require(length >= 0, "The length of char type cannot be negative.")

  private[sql] type InternalType = UTF8String
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering = implicitly[Ordering[InternalType]]

  override def defaultSize: Int = length
  override def physicalDataType: PhysicalDataType = PhysicalStringType
  override def typeName: String = s"char($length)"
  override def toString: String = s"CharType($length)"
  private[spark] override def asNullable: CharType = this
}
