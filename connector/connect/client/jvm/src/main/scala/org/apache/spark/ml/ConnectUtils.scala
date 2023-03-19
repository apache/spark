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

package org.apache.spark.ml

import org.apache.spark.connect.proto
import org.apache.spark.ml.linalg.{Matrix, Vector, Matrices, Vectors}
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.connect.common.{InvalidPlanInput, LiteralValueProtoConverter}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

import scala.collection.mutable
import scala.reflect.ClassTag

object ConnectUtils {

  def getInstanceParamsProto(instance: Params): proto.MlParams = {
    val builder = proto.MlParams.newBuilder()

    for (param <- instance.params) {
      instance.get(param).map { value =>
        builder.putParams(
          param.name,
          LiteralValueProtoConverter.toLiteralProto(value)
        )
      }
      instance.getDefault(param).map { value =>
        builder.putParams(
          param.name,
          LiteralValueProtoConverter.toLiteralProto(value)
        )
      }
    }
    builder.build()
  }

  def serializeResponseValue(data: Any): proto.MlCommandResponse = {
    data match {
      case v: Vector => serializeVector(v)
      case v: Matrix => serializeMatrix(v)
      case _: Byte | _: Short | _: Int | _: Long | _: Float | _: Double | _: Boolean | _: String |
           _: Array[_] =>
        proto.MlCommandResponse
          .newBuilder()
          .setLiteral(LiteralValueProtoConverter.toLiteralProto(data))
          .build()
      case _ =>
        throw new IllegalArgumentException()
    }
  }

  def serializeVector(data: Vector): proto.MlCommandResponse = {
    // TODO: Support sparse
    val values = data.toArray
    val denseBuilder = proto.Vector.Dense.newBuilder()
    for (i <- 0 until values.length) {
      denseBuilder.addValue(values(i))
    }

    proto.MlCommandResponse
      .newBuilder()
      .setVector(proto.Vector.newBuilder().setDense(denseBuilder))
      .build()
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
    proto.MlCommandResponse
      .newBuilder()
      .setMatrix(proto.Matrix.newBuilder().setDense(denseBuilder))
      .build()
  }

  def deserializeResponseValue(protoValue: proto.MlCommandResponse): Any = {
    protoValue.getMlCommandResponseTypeCase match {
      case proto.MlCommandResponse.MlCommandResponseTypeCase.LITERAL =>
        deserializeLiteral(protoValue.getLiteral)
      case proto.MlCommandResponse.MlCommandResponseTypeCase.VECTOR =>
        deserializeVector(protoValue.getVector)
      case proto.MlCommandResponse.MlCommandResponseTypeCase.MATRIX =>
        deserializeMatrix(protoValue.getMatrix)
      case proto.MlCommandResponse.MlCommandResponseTypeCase.MODEL_REF =>
        ModelRef.fromProto(protoValue.getModelRef)
      case _ =>
        throw new IllegalArgumentException()
    }
  }

  def deserializeLiteral(protoValue: proto.Expression.Literal): Any = {
    protoValue.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.INTEGER =>
        protoValue.getInteger
      case proto.Expression.Literal.LiteralTypeCase.LONG =>
        protoValue.getLong
      case proto.Expression.Literal.LiteralTypeCase.FLOAT =>
        protoValue.getFloat
      case proto.Expression.Literal.LiteralTypeCase.DOUBLE =>
        protoValue.getDouble
      case proto.Expression.Literal.LiteralTypeCase.STRING =>
        protoValue.getString
      case proto.Expression.Literal.LiteralTypeCase.BOOLEAN =>
        protoValue.getInteger
      case proto.Expression.Literal.LiteralTypeCase.ARRAY =>
        toArrayData(protoValue.getArray)
      case _ =>
        throw new IllegalArgumentException()
    }
  }

  private def toArrayData(array: proto.Expression.Literal.Array): Any = {
    def makeArrayData[T](converter: proto.Expression.Literal => T)(implicit
                                                                   tag: ClassTag[T]): Array[T] = {
      val builder = mutable.ArrayBuilder.make[T]
      val elementList = array.getElementsList
      builder.sizeHint(elementList.size())
      val iter = elementList.iterator()
      while (iter.hasNext) {
        builder += converter(iter.next())
      }
      builder.result()
    }

    val elementType = array.getElementType
    if (elementType.hasShort) {
      makeArrayData(v => v.getShort.toShort)
    } else if (elementType.hasInteger) {
      makeArrayData(v => v.getInteger)
    } else if (elementType.hasLong) {
      makeArrayData(v => v.getLong)
    } else if (elementType.hasDouble) {
      makeArrayData(v => v.getDouble)
    } else if (elementType.hasByte) {
      makeArrayData(v => v.getByte.toByte)
    } else if (elementType.hasFloat) {
      makeArrayData(v => v.getFloat)
    } else if (elementType.hasBoolean) {
      makeArrayData(v => v.getBoolean)
    } else if (elementType.hasString) {
      makeArrayData(v => v.getString)
    } else if (elementType.hasBinary) {
      makeArrayData(v => v.getBinary.toByteArray)
    } else if (elementType.hasDate) {
      makeArrayData(v => DateTimeUtils.toJavaDate(v.getDate))
    } else if (elementType.hasTimestamp) {
      makeArrayData(v => DateTimeUtils.toJavaTimestamp(v.getTimestamp))
    } else if (elementType.hasTimestampNtz) {
      makeArrayData(v => DateTimeUtils.microsToLocalDateTime(v.getTimestampNtz))
    } else if (elementType.hasDayTimeInterval) {
      makeArrayData(v => IntervalUtils.microsToDuration(v.getDayTimeInterval))
    } else if (elementType.hasYearMonthInterval) {
      makeArrayData(v => IntervalUtils.monthsToPeriod(v.getYearMonthInterval))
    } else if (elementType.hasDecimal) {
      makeArrayData(v => Decimal(v.getDecimal.getValue))
    } else if (elementType.hasCalendarInterval) {
      makeArrayData(v => {
        val interval = v.getCalendarInterval
        new CalendarInterval(interval.getMonths, interval.getDays, interval.getMicroseconds)
      })
    } else if (elementType.hasArray) {
      makeArrayData(v => toArrayData(v.getArray))
    } else {
      throw InvalidPlanInput(s"Unsupported Literal Type: $elementType)")
    }
  }

}