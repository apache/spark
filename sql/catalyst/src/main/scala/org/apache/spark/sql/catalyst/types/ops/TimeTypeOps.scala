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

import java.time.LocalTime

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, MutableLong, MutableValue}
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalLongType}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.TimeType
import org.apache.spark.sql.types.ops.TimeTypeApiOps

case class TimeTypeOps(t: TimeType)
  extends TimeTypeApiOps(t)
  with TypeOps
  with PhyTypeOps
  with LiteralTypeOps
  with ExternalTypeOps[LocalTime, LocalTime, Any] {

  override def getPhysicalType: PhysicalDataType = PhysicalLongType
  override def getJavaClass: Class[_] = classOf[PhysicalLongType.InternalType]
  override def getMutableValue: MutableValue = new MutableLong

  override def getDefaultLiteral: Literal = Literal.create(0L, t)
  override def getJavaLiteral(v: Any): String = s"${v}L"

  override def toCatalystImpl(scalaValue: LocalTime): Long = {
    DateTimeUtils.localTimeToNanos(scalaValue)
  }
  override def toScala(catalystValue: Any): LocalTime = {
    if (catalystValue == null) null
    else DateTimeUtils.nanosToLocalTime(catalystValue.asInstanceOf[Long])
  }
  override def toScalaImpl(row: InternalRow, column: Int): LocalTime =
    DateTimeUtils.nanosToLocalTime(row.getLong(column))
}
