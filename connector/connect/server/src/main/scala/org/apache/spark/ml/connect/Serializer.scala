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

package org.apache.spark.ml.connect

import org.apache.spark.connect.proto
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter

object Serializer {

  def serializeResponseValue(data: Any): proto.MlCommandResponse = {
    data match {
      case v: Vector =>
        val vectorProto = serializeVector(v)
        proto.MlCommandResponse
          .newBuilder()
          .setVector(vectorProto)
          .build()
      case v: Matrix =>
        val matrixProto = serializeMatrix(v)
        proto.MlCommandResponse
          .newBuilder()
          .setMatrix(matrixProto)
          .build()
      case _: Byte | _: Short | _: Int | _: Long | _: Float | _: Double | _: Boolean | _: String |
          _: Array[_] =>
        proto.MlCommandResponse
          .newBuilder()
          .setLiteral(LiteralValueProtoConverter.toLiteralProto(data))
          .build()
    }
  }

  def serializeVector(data: Vector): proto.Vector = {
    // TODO: Support sparse
    val values = data.toArray
    val denseBuilder = proto.Vector.Dense.newBuilder()
    for (i <- 0 until values.length) {
      denseBuilder.addValue(values(i))
    }
    proto.Vector.newBuilder().setDense(denseBuilder).build()
  }

  def serializeMatrix(data: Matrix): proto.Matrix = {
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
    proto.Matrix.newBuilder().setDense(denseBuilder).build()
  }

  def deserializeVector(protoValue: proto.Vector): Vector = {
    // TODO: Support sparse
    Vectors.dense(
      protoValue.getDense.getValueList.stream().mapToDouble(_.doubleValue()).toArray
    )
  }

  def deserializeMatrix(protoValue: proto.Matrix): Matrix = {
    // TODO: Support sparse
    val denseProto = protoValue.getDense
    Matrices.dense(
      denseProto.getNumRows,
      denseProto.getNumCols,
      denseProto.getValueList.stream().mapToDouble(_.doubleValue()).toArray
    )
  }
}
