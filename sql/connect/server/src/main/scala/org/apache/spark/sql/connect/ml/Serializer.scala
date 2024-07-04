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
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrix, SparseMatrix, SparseVector, Vector}
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.connect.service.SessionHolder

object Serializer {

  def serializeParam(data: Any): proto.Param = {
    data match {
      case v: Vector =>
        // TODO: Support sparse
        val values = v.toArray
        val denseBuilder = proto.Vector.Dense.newBuilder()
        for (i <- 0 until values.length) {
          denseBuilder.addValue(values(i))
        }
        proto.Param
          .newBuilder()
          .setVector(proto.Vector.newBuilder().setDense(denseBuilder))
          .build()
      case v: Matrix =>
        // TODO: Support sparse
        // TODO: optimize transposed case
        val denseBuilder = proto.Matrix.Dense.newBuilder()
        val values = v.toArray
        for (i <- 0 until values.length) {
          denseBuilder.addValue(values(i))
        }
        denseBuilder.setNumCols(v.numCols)
        denseBuilder.setNumRows(v.numRows)
        denseBuilder.setIsTransposed(false)
        proto.Param
          .newBuilder()
          .setMatrix(proto.Matrix.newBuilder().setDense(denseBuilder))
          .build()
      case _: Byte | _: Short | _: Int | _: Long | _: Float | _: Double | _: Boolean | _: String |
          _: Array[_] =>
        proto.Param
          .newBuilder()
          .setLiteral(LiteralValueProtoConverter.toLiteralProto(data))
          .build()

      case _ => throw new RuntimeException("Unsupported parameter type")
    }
  }

  def deserializeMethodArguments(
      args: Array[proto.FetchModelAttr.Args],
      sessionHolder: SessionHolder): Array[(Object, Class[_])] = {
    args.map { arg =>
      if (arg.hasParam) {
        val param = arg.getParam
        if (param.hasLiteral) {
          param.getLiteral.getLiteralTypeCase match {
            case proto.Expression.Literal.LiteralTypeCase.INTEGER =>
              (param.getLiteral.getInteger.asInstanceOf[Object], classOf[Int])
            case proto.Expression.Literal.LiteralTypeCase.FLOAT =>
              (param.getLiteral.getFloat.toDouble.asInstanceOf[Object], classOf[Double])
            case proto.Expression.Literal.LiteralTypeCase.STRING =>
              (param.getLiteral.getString, classOf[String])
            case proto.Expression.Literal.LiteralTypeCase.DOUBLE =>
              (param.getLiteral.getDouble.asInstanceOf[Object], classOf[Double])
            case proto.Expression.Literal.LiteralTypeCase.BOOLEAN =>
              (param.getLiteral.getBoolean.asInstanceOf[Object], classOf[Boolean])
            case _ =>
              throw new UnsupportedOperationException(param.getLiteral.getLiteralTypeCase.name())
          }
        } else if (param.hasVector) {
          val vector = MLUtils.deserializeVector(param.getVector)
          val vectorType = if (param.getVector.hasDense) {
            classOf[DenseVector]
          } else {
            classOf[SparseVector]
          }
          (vector, vectorType)
        } else if (param.hasMatrix) {
          val matrix = MLUtils.deserializeMatrix(param.getMatrix)
          val matrixType = if (param.getMatrix.hasDense) {
            classOf[DenseMatrix]
          } else {
            classOf[SparseMatrix]
          }
          (matrix, matrixType)
        } else {
          throw new UnsupportedOperationException("unsupported parameter type")
        }
      } else if (arg.hasInput) {
        (MLUtils.parseRelationProto(arg.getInput, sessionHolder), classOf[Dataset[_]])
      } else {
        throw new UnsupportedOperationException("deserializeMethodArguments")
      }
    }
  }

  def serializeParams(instance: Params): proto.MlParams = {
    val builder = proto.MlParams.newBuilder()
    instance.params.foreach { param =>
      if (instance.isSet(param)) {
        val v = serializeParam(instance.get(param).get)
        builder.putParams(param.name, v)
      }
    }
    builder.build()
  }
}
