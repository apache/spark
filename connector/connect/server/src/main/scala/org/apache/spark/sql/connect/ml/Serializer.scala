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

package org.apache.spark.sql.connect.ml

import org.apache.spark.connect.proto
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.planner.LiteralValueProtoConverter
import org.apache.spark.sql.types


object Serializer {

  def serialize(data: Any): proto.MlCommandResponse = {
    data match {
      case v: Vector => serializeVector(v)
      case v: Matrix => serializeMatrix(v)
      case v: Array[_] =>
        // TODO: Make `LiteralValueProtoConverter.toConnectProtoValue` support it.
        val arrayProto = proto.Expression.Literal.Array.newBuilder()
        for (e <- v) {
          arrayProto.addElements(LiteralValueProtoConverter.toConnectProtoValue(e))
        }
        arrayProto.setElementType(
          DataTypeProtoConverter.toConnectProtoType(v(0) match {
            case _: Byte => types.ByteType
            case _: Short => types.ShortType
            case _: Int => types.IntegerType
            case _: Long => types.LongType
            case _: Float => types.FloatType
            case _: Double => types.DoubleType
            case _ => throw new UnsupportedOperationException()
          })
        )
        proto.MlCommandResponse.newBuilder().setLiteral(
          proto.Expression.Literal.newBuilder()
            .setArray(arrayProto)
        ).build()
      case _: Byte | _: Short | _: Int | _: Long | _: Float | _: Double |_: Boolean | _: String =>
        proto.MlCommandResponse.newBuilder().setLiteral(
          LiteralValueProtoConverter.toConnectProtoValue(data)
        ).build()
    }
  }

  def serializeVector(data: Vector): proto.MlCommandResponse = {
    // TODO: Support sparse
    val values = data.toArray
    val denseBuilder = proto.Vector.Dense.newBuilder()
    for (i <- 0 until values.length) {
      denseBuilder.addValue(values(i))
    }

    proto.MlCommandResponse.newBuilder().setVector(
      proto.Vector.newBuilder().setDense(denseBuilder)
    ).build()
  }

  def serializeMatrix(data: Matrix): proto.MlCommandResponse = {
    // TODO: Support sparse
    // TODO: optimize transposed case
    val denseBuilder = proto.Matrix.Dense.newBuilder()
    val values = data.toArray
    for (i <- 0 until values.length) {
      denseBuilder.addValue(values(i))
    }
    denseBuilder.setNumCols(data.numCols)
    denseBuilder.setNumRows(data.numRows)
    denseBuilder.setIsTransposed(false)
    proto.MlCommandResponse.newBuilder().setMatrix(
      proto.Matrix.newBuilder().setDense(denseBuilder)
    ).build()
  }
}
