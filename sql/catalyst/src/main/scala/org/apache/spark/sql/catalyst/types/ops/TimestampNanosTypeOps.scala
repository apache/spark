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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, MutableTimestampNanos, MutableValue}
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalTimestampLTZNanosType, PhysicalTimestampNTZNanosType}
import org.apache.spark.sql.types.{TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.sql.types.ops.{TimestampLTZNanosTypeApiOps, TimestampNTZNanosTypeApiOps}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Server-side (catalyst) operations shared by the nanosecond timestamp types
 * (TimestampNTZNanosType and TimestampLTZNanosType).
 *
 * Internal values are [[TimestampNanosVal]] (epoch micros + nanos within the micro), stored in
 * [[org.apache.spark.sql.catalyst.expressions.UnsafeRow]] via a 16-byte variable-length payload;
 * see [[org.apache.spark.sql.catalyst.expressions.TimestampNanosRowValues]].
 *
 * SCOPE (SPARK-57207): this issue covers physical representation, literals, row accessors, and
 * codegen class selection. java.time conversion is out of scope (SPARK-57033), so the external
 * conversion methods are identity over [[TimestampNanosVal]], matching the legacy identity
 * converter behavior. Concrete subclasses supply the NTZ/LTZ-specific physical type and row
 * accessors.
 *
 * @since 4.3.0
 */
trait TimestampNanosTypeOps extends TypeOps {

  // ==================== Physical Type Representation ====================

  override def getJavaClass: Class[_] = classOf[TimestampNanosVal]

  override def getMutableValue: MutableValue = new MutableTimestampNanos

  // ==================== Literal Creation ====================

  override def getDefaultLiteral: Literal = Literal.create(TimestampNanosVal.ZERO, dataType)

  override def getJavaLiteral(v: Any): String = {
    val tn = v.asInstanceOf[TimestampNanosVal]
    "org.apache.spark.unsafe.types.TimestampNanosVal.fromParts(" +
      s"${tn.epochMicros}L, (short) ${tn.nanosWithinMicro})"
  }

  // ==================== External Type Conversion ====================

  // java.time conversion is handled in a follow-up issue (SPARK-57033); until then values pass
  // through unchanged as TimestampNanosVal, matching the legacy identity converter.
  override def toCatalystImpl(scalaValue: Any): Any = scalaValue

  override def toScala(catalystValue: Any): Any = catalystValue
}

/**
 * Server-side operations for [[TimestampNTZNanosType]].
 *
 * @param t
 *   The TimestampNTZNanosType with precision information
 * @since 4.3.0
 */
case class TimestampNTZNanosTypeOps(override val t: TimestampNTZNanosType)
  extends TimestampNTZNanosTypeApiOps(t) with TimestampNanosTypeOps {

  override def getPhysicalType: PhysicalDataType = PhysicalTimestampNTZNanosType

  override def getRowWriter(ordinal: Int): (InternalRow, Any) => Unit =
    (input, v) => input.setTimestampNTZNanos(ordinal, v.asInstanceOf[TimestampNanosVal])

  override def toScalaImpl(row: InternalRow, column: Int): Any =
    row.getTimestampNTZNanos(column)
}

/**
 * Server-side operations for [[TimestampLTZNanosType]].
 *
 * @param t
 *   The TimestampLTZNanosType with precision information
 * @since 4.3.0
 */
case class TimestampLTZNanosTypeOps(override val t: TimestampLTZNanosType)
  extends TimestampLTZNanosTypeApiOps(t) with TimestampNanosTypeOps {

  override def getPhysicalType: PhysicalDataType = PhysicalTimestampLTZNanosType

  override def getRowWriter(ordinal: Int): (InternalRow, Any) => Unit =
    (input, v) => input.setTimestampLTZNanos(ordinal, v.asInstanceOf[TimestampNanosVal])

  override def toScalaImpl(row: InternalRow, column: Int): Any =
    row.getTimestampLTZNanos(column)
}
