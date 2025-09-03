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

package org.apache.spark.sql.catalyst.types.ops

import org.apache.spark.sql.catalyst.expressions.MutableValue
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types.{DataType, TimeType}

// Base operations over Catalyst's types.
trait PhyTypeOps extends TypeOps {
  // Gets the underlying physical type
  def getPhysicalType: PhysicalDataType

  // Gets the Java class of the physical type
  def getJavaClass: Class[_]

  // Gets a mutable container for the physical type
  def getMutableValue: MutableValue
}

object PhyTypeOps {
  private val supportedDataTypes: Set[DataType] =
    Set(TimeType.MIN_PRECISION to TimeType.MAX_PRECISION map TimeType.apply: _*)

  def supports(dt: DataType): Boolean = supportedDataTypes.contains(dt)

  def apply(dt: DataType): PhyTypeOps = TypeOps(dt).asInstanceOf[PhyTypeOps]
}
