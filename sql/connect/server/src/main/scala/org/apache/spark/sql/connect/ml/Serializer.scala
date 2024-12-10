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
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, SparseMatrix, SparseVector}
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.connect.service.SessionHolder

private[ml] object Serializer {

  /**
   * Serialize the ML parameters, currently support Vector/Matrix and literals
   * @param data
   *   the value of parameter
   * @return
   *   proto.Param
   */
  def serializeParam(data: Any): proto.Param = {
    data match {
      case v: DenseVector =>
        val denseBuilder = proto.Vector.Dense.newBuilder()
        v.values.foreach(denseBuilder.addValue)
        proto.Param
          .newBuilder()
          .setVector(proto.Vector.newBuilder().setDense(denseBuilder))
          .build()
      case v: SparseVector =>
        val sparseBuilder = proto.Vector.Sparse.newBuilder().setSize(v.size)
        v.indices.foreach(sparseBuilder.addIndex)
        v.values.foreach(sparseBuilder.addValue)
        proto.Param
          .newBuilder()
          .setVector(proto.Vector.newBuilder().setSparse(sparseBuilder))
          .build()
      case v: DenseMatrix =>
        val denseBuilder = proto.Matrix.Dense.newBuilder()
        v.values.foreach(denseBuilder.addValue)
        denseBuilder.setNumCols(v.numCols)
        denseBuilder.setNumRows(v.numRows)
        denseBuilder.setIsTransposed(v.isTransposed)
        proto.Param
          .newBuilder()
          .setMatrix(proto.Matrix.newBuilder().setDense(denseBuilder))
          .build()
      case v: SparseMatrix =>
        val sparseBuilder = proto.Matrix.Sparse
          .newBuilder()
          .setNumCols(v.numCols)
          .setNumRows(v.numRows)
        v.values.foreach(sparseBuilder.addValue)
        v.colPtrs.foreach(sparseBuilder.addColptr)
        v.rowIndices.foreach(sparseBuilder.addRowIndex)
        proto.Param
          .newBuilder()
          .setMatrix(proto.Matrix.newBuilder().setSparse(sparseBuilder))
          .build()
      case _: Byte | _: Short | _: Int | _: Long | _: Float | _: Double | _: Boolean | _: String |
          _: Array[_] =>
        proto.Param
          .newBuilder()
          .setLiteral(LiteralValueProtoConverter.toLiteralProto(data))
          .build()

      case other => throw MlUnsupportedException(s"$other not supported")
    }
  }

  def deserializeMethodArguments(
      args: Array[proto.Fetch.Args],
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
            case other =>
              throw MlUnsupportedException(s"$other not supported")
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
          throw MlUnsupportedException(s"$param not supported")
        }
      } else if (arg.hasInput) {
        (MLUtils.parseRelationProto(arg.getInput, sessionHolder), classOf[Dataset[_]])
      } else {
        throw MlUnsupportedException(s"$arg not supported")
      }
    }
  }

  /**
   * Serialize an instance of "Params" which could be estimator/model/evaluator ...
   * @param instance
   *   of Params
   * @return
   *   proto.MlParams
   */
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
