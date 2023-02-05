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

package org.apache.spark.sql.catalyst.types

import org.apache.spark.sql.types._

sealed abstract class PhysicalDataType

case class PhysicalArrayType(elementType: DataType, containsNull: Boolean) extends PhysicalDataType

class PhysicalBinaryType() extends PhysicalDataType
case object PhysicalBinaryType extends PhysicalBinaryType

class PhysicalBooleanType() extends PhysicalDataType
case object PhysicalBooleanType extends PhysicalBooleanType

class PhysicalByteType() extends PhysicalDataType
case object PhysicalByteType extends PhysicalByteType

class PhysicalCalendarIntervalType() extends PhysicalDataType
case object PhysicalCalendarIntervalType extends PhysicalCalendarIntervalType

case class PhysicalDecimalType(precision: Int, scale: Int) extends PhysicalDataType

class PhysicalDoubleType() extends PhysicalDataType
case object PhysicalDoubleType extends PhysicalDoubleType

class PhysicalFloatType() extends PhysicalDataType
case object PhysicalFloatType extends PhysicalFloatType

class PhysicalIntegerType() extends PhysicalDataType
case object PhysicalIntegerType extends PhysicalIntegerType

class PhysicalLongType() extends PhysicalDataType
case object PhysicalLongType extends PhysicalLongType

case class PhysicalMapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean)
    extends PhysicalDataType

class PhysicalNullType() extends PhysicalDataType
case object PhysicalNullType extends PhysicalNullType

class PhysicalShortType() extends PhysicalDataType
case object PhysicalShortType extends PhysicalShortType

class PhysicalStringType() extends PhysicalDataType
case object PhysicalStringType extends PhysicalStringType

case class PhysicalStructType(fields: Array[StructField]) extends PhysicalDataType

object UninitializedPhysicalType extends PhysicalDataType
