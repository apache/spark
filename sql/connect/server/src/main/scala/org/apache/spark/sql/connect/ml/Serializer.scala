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
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, LiteralValueProtoConverter, ProtoDataTypes}
import org.apache.spark.sql.connect.service.SessionHolder

private[ml] object Serializer {

  /**
   * Serialize the ML parameters, currently support Vector/Matrix and literals
   * @param data
   *   the value of parameter
   * @return
   *   proto.Expression
   */
  def serializeParam(data: Any): proto.Expression = {
    val literal = data match {
      case v: SparseVector =>
        val builder = proto.Expression.Literal.Struct.newBuilder()
        builder.setStructType(DataTypeProtoConverter.toConnectProtoType(VectorUDT.sqlType))
        // type = 0
        builder.addElements(proto.Expression.Literal.newBuilder().setByte(0))
        // size
        builder.addElements(proto.Expression.Literal.newBuilder().setInteger(v.size))
        // indices
        builder.addElements(buildIntArray(v.indices))
        // values
        builder.addElements(buildDoubleArray(v.values))
        proto.Expression.Literal.newBuilder().setStruct(builder).build()

      case v: DenseVector =>
        val builder = proto.Expression.Literal.Struct.newBuilder()
        builder.setStructType(DataTypeProtoConverter.toConnectProtoType(VectorUDT.sqlType))
        // type = 1
        builder.addElements(proto.Expression.Literal.newBuilder().setByte(1))
        // size = null
        builder.addElements(
          proto.Expression.Literal.newBuilder().setNull(ProtoDataTypes.NullType))
        // indices = null
        builder.addElements(
          proto.Expression.Literal.newBuilder().setNull(ProtoDataTypes.NullType))
        // values
        builder.addElements(buildDoubleArray(v.values))
        proto.Expression.Literal.newBuilder().setStruct(builder).build()

      case m: SparseMatrix =>
        val builder = proto.Expression.Literal.Struct.newBuilder()
        builder.setStructType(DataTypeProtoConverter.toConnectProtoType(MatrixUDT.sqlType))
        // type = 0
        builder.addElements(proto.Expression.Literal.newBuilder().setByte(0))
        // numRows
        builder.addElements(proto.Expression.Literal.newBuilder().setInteger(m.numRows))
        // numCols
        builder.addElements(proto.Expression.Literal.newBuilder().setInteger(m.numCols))
        // colPtrs
        builder.addElements(buildIntArray(m.colPtrs))
        // rowIndices
        builder.addElements(buildIntArray(m.rowIndices))
        // values
        builder.addElements(buildDoubleArray(m.values))
        // isTransposed
        builder.addElements(proto.Expression.Literal.newBuilder().setBoolean(m.isTransposed))
        proto.Expression.Literal.newBuilder().setStruct(builder).build()

      case m: DenseMatrix =>
        val builder = proto.Expression.Literal.Struct.newBuilder()
        builder.setStructType(DataTypeProtoConverter.toConnectProtoType(MatrixUDT.sqlType))
        // type = 1
        builder.addElements(proto.Expression.Literal.newBuilder().setByte(1))
        // numRows
        builder.addElements(proto.Expression.Literal.newBuilder().setInteger(m.numRows))
        // numCols
        builder.addElements(proto.Expression.Literal.newBuilder().setInteger(m.numCols))
        // colPtrs = null
        builder.addElements(
          proto.Expression.Literal.newBuilder().setNull(ProtoDataTypes.NullType))
        // rowIndices = null
        builder.addElements(
          proto.Expression.Literal.newBuilder().setNull(ProtoDataTypes.NullType))
        // values
        builder.addElements(buildDoubleArray(m.values))
        // isTransposed
        builder.addElements(proto.Expression.Literal.newBuilder().setBoolean(m.isTransposed))
        proto.Expression.Literal.newBuilder().setStruct(builder).build()

      case _: Byte | _: Short | _: Int | _: Long | _: Float | _: Double | _: Boolean | _: String |
          _: Array[_] =>
        LiteralValueProtoConverter.toLiteralProto(data)

      case other => throw MlUnsupportedException(s"$other not supported")
    }
    proto.Expression.newBuilder().setLiteral(literal).build()
  }

  private def buildIntArray(values: Array[Int]): proto.Expression.Literal = {
    val builder = proto.Expression.Literal.SpecializedArray.newBuilder()
    builder.setElementType(ProtoDataTypes.IntegerType)
    values.foreach(builder.addIntegers)
    proto.Expression.Literal.newBuilder().setSpecializedArray(builder.build()).build()
  }

  private def buildDoubleArray(values: Array[Double]): proto.Expression.Literal = {
    val builder = proto.Expression.Literal.SpecializedArray.newBuilder()
    builder.setElementType(ProtoDataTypes.DoubleType)
    values.foreach(builder.addDoubles)
    proto.Expression.Literal.newBuilder().setSpecializedArray(builder.build()).build()
  }

  def deserializeMethodArguments(
      args: Array[proto.Fetch.Method.Args],
      sessionHolder: SessionHolder): Array[(Object, Class[_])] = {
    args.map { arg =>
      if (arg.hasParam) {
        val param = arg.getParam
        if (param.hasLiteral) {
          param.getLiteral.getLiteralTypeCase match {
            case proto.Expression.Literal.LiteralTypeCase.STRUCT =>
              val struct = param.getLiteral.getStruct
              val schema = DataTypeProtoConverter.toCatalystType(struct.getStructType)
              if (schema == VectorUDT.sqlType) {
                (MLUtils.deserializeVector(struct), classOf[Vector])
              } else if (schema == MatrixUDT.sqlType) {
                (MLUtils.deserializeMatrix(struct), classOf[Matrix])
              } else {
                throw MlUnsupportedException(s"$schema not supported")
              }
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
