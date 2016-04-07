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

package org.apache.spark.sql.execution.aggregate

import scala.language.existentials

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedDeserializer, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._

object TypedAggregateExpression {
  def apply[BUF : Encoder, OUT : Encoder](
      aggregator: Aggregator[_, BUF, OUT]): TypedAggregateExpression = {
    val bufferEncoder = encoderFor[BUF]
    val bufferSerializer = bufferEncoder.namedExpressions

    // To avoid re-calculating the deserializer expression and function call expression while
    // evaluating each buffer serializer expression, we serialize the buffer object to a single
    // struct field, not multiply fields, no matter whether the encoder is flat or not. So for
    // buffer deserializer, we should add one extra level at bottom, to use the buffer attribute of
    // struct type as input.
    // TODO: remove this trick after we have  better integration of subexpression elimination and
    // whole stage codegen.
    val bufferAttr = if (bufferEncoder.flat) {
      AttributeReference("buffer", bufferEncoder.schema.head.dataType, nullable = false)()
    } else {
      AttributeReference("buffer", bufferEncoder.schema, nullable = false)()
    }
    val bufferDeserializer = if (bufferEncoder.flat) {
      bufferEncoder.deserializer
    } else {
      bufferEncoder.deserializer transformUp {
        case UnresolvedAttribute(nameParts) =>
          assert(nameParts.length == 1)
          UnresolvedExtractValue(bufferAttr, Literal(nameParts.head))
        case BoundReference(ordinal, dt, _) => GetStructField(bufferAttr, ordinal)
      }
    }

    val outputEncoder = encoderFor[OUT]
    val outputType = if (outputEncoder.flat) {
      outputEncoder.schema.head.dataType
    } else {
      outputEncoder.schema
    }

    new TypedAggregateExpression(
      aggregator.asInstanceOf[Aggregator[Any, Any, Any]],
      None,
      bufferAttr,
      bufferSerializer,
      UnresolvedDeserializer(bufferDeserializer, bufferAttr :: Nil),
      outputEncoder.serializer,
      outputEncoder.deserializer.dataType,
      outputType)
  }
}

/**
 * A helper class to hook [[Aggregator]] into the aggregation system.
 */
case class TypedAggregateExpression(
    aggregator: Aggregator[Any, Any, Any],
    inputDeserializer: Option[Expression],
    bufferAttr: AttributeReference,
    bufferSerializer: Seq[NamedExpression],
    bufferDeserializer: Expression,
    outputSerializer: Seq[Expression],
    outputExternalType: DataType,
    dataType: DataType) extends DeclarativeAggregate with NonSQLExpression {

  override def nullable: Boolean = true

  override def deterministic: Boolean = true

  override def children: Seq[Expression] = inputDeserializer.toSeq :+ bufferDeserializer

  override lazy val resolved: Boolean = inputDeserializer.isDefined && childrenResolved

  override def references: AttributeSet = AttributeSet(inputDeserializer.toSeq)

  override def inputTypes: Seq[AbstractDataType] = Nil

  private def aggregatorLiteral =
    Literal.create(aggregator, ObjectType(classOf[Aggregator[Any, Any, Any]]))

  private def bufferExternalType = bufferDeserializer.dataType

  override lazy val aggBufferAttributes: Seq[AttributeReference] = bufferAttr :: Nil

  private def generateBuffer(inputObj: Expression): Seq[Expression] = {
    if (bufferSerializer.length > 1) {
      EvaluateOnce(bufferSerializer, inputObj, bufferAttr.dataType) :: Nil
    } else {
      bufferSerializer.head.transform {
        case b: BoundReference => inputObj
      } :: Nil
    }
  }

  override lazy val initialValues: Seq[Expression] = {
    val zero = Literal.fromObject(aggregator.zero, bufferExternalType)
    generateBuffer(zero)
  }

  override lazy val updateExpressions: Seq[Expression] = {
    val reduced = Invoke(
      aggregatorLiteral,
      "reduce",
      bufferExternalType,
      bufferDeserializer :: inputDeserializer.get :: Nil)

    generateBuffer(reduced)
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    val leftBuffer = bufferDeserializer transform {
      case a: AttributeReference => a.left
    }
    val rightBuffer = bufferDeserializer transform {
      case a: AttributeReference => a.right
    }
    val merged = Invoke(
      aggregatorLiteral,
      "merge",
      bufferExternalType,
      leftBuffer :: rightBuffer :: Nil)

    generateBuffer(merged)
  }

  override lazy val evaluateExpression: Expression = {
    val resultObj = Invoke(
      aggregatorLiteral,
      "finish",
      outputExternalType,
      bufferDeserializer :: Nil)

    dataType match {
      case s: StructType => EvaluateOnce(outputSerializer, resultObj, s)
      case _ =>
        assert(outputSerializer.length == 1)
        outputSerializer.head transform {
          case b: BoundReference => resultObj
        }
    }
  }

  override def toString: String = {
    val input = inputDeserializer match {
      case Some(UnresolvedDeserializer(deserializer, _)) => deserializer.dataType.simpleString
      case Some(deserializer) => deserializer.dataType.simpleString
      case _ => "unknown"
    }

    s"$nodeName($input)"
  }

  override def nodeName: String = aggregator.getClass.getSimpleName.stripSuffix("$")
}

/**
 * Combines serializer expressions into one single expression that outputs a struct, evaluate the
 * object expression only once and use the result as input for all serializer expressions.
 */
case class EvaluateOnce(serializer: Seq[Expression], obj: Expression, dataType: DataType)
  extends UnaryExpression with NonSQLExpression {

  override def nullable: Boolean = false
  override def child: Expression = obj

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override protected def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val evalObj = obj.gen(ctx)
    val objRef = LambdaVariable(evalObj.value, evalObj.isNull, obj.dataType)

    val result = CreateStruct(serializer.map(_ transform {
      case b: BoundReference => objRef
    }))

    val evalResult = result.gen(ctx)
    ev.value = evalResult.value
    ev.isNull = evalResult.isNull

    evalObj.code + "\n" + evalResult.code
  }
}
