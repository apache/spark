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
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedDeserializer, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._

object TypedAggregateExpression {
  def apply[IN: Encoder, BUF : Encoder, OUT : Encoder](
      aggregator: Aggregator[IN, BUF, OUT],
      inputColumnNames: Option[Seq[String]] = None): TypedAggregateExpression = {
    val inputEncoder = encoderFor[IN]
    val inputDeserializer = UnresolvedDeserializer(inputColumnNames.map{ names =>
      inputEncoder.deserializer.transform {
        case BoundReference(ord, dataType, nullable) =>
          UnresolvedAttribute(names(ord))
      }
    }.getOrElse(inputEncoder.deserializer))
    val bufferEncoder = encoderFor[BUF]
    val bufferSerializer = bufferEncoder.namedExpressions
    val bufferDeserializer = bufferEncoder.deserializer.transform {
      case b: BoundReference => bufferSerializer(b.ordinal).toAttribute
    }

    val outputEncoder = encoderFor[OUT]
    val outputType = if (outputEncoder.flat) {
      outputEncoder.schema.head.dataType
    } else {
      outputEncoder.schema
    }

    new TypedAggregateExpression(
      aggregator.asInstanceOf[Aggregator[Any, Any, Any]],
      inputDeserializer,
      bufferSerializer,
      bufferDeserializer,
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
    inputDeserializer: Expression,
    bufferSerializer: Seq[NamedExpression],
    bufferDeserializer: Expression,
    outputSerializer: Seq[Expression],
    outputExternalType: DataType,
    dataType: DataType) extends DeclarativeAggregate with NonSQLExpression {

  override def nullable: Boolean = true

  override def deterministic: Boolean = true

  override def children: Seq[Expression] = inputDeserializer :: bufferDeserializer :: Nil

  override lazy val resolved: Boolean = childrenResolved

  override def references: AttributeSet = AttributeSet(Seq(inputDeserializer))

  override def inputTypes: Seq[AbstractDataType] = Nil

  private def aggregatorLiteral =
    Literal.create(aggregator, ObjectType(classOf[Aggregator[Any, Any, Any]]))

  private def bufferExternalType = bufferDeserializer.dataType

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    bufferSerializer.map(_.toAttribute.asInstanceOf[AttributeReference])

  override lazy val initialValues: Seq[Expression] = {
    val zero = Literal.fromObject(aggregator.zero, bufferExternalType)
    bufferSerializer.map(ReferenceToExpressions(_, zero :: Nil))
  }

  override lazy val updateExpressions: Seq[Expression] = {
    val reduced = Invoke(
      aggregatorLiteral,
      "reduce",
      bufferExternalType,
      bufferDeserializer :: inputDeserializer :: Nil)

    bufferSerializer.map(ReferenceToExpressions(_, reduced :: Nil))
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

    bufferSerializer.map(ReferenceToExpressions(_, merged :: Nil))
  }

  override lazy val evaluateExpression: Expression = {
    val resultObj = Invoke(
      aggregatorLiteral,
      "finish",
      outputExternalType,
      bufferDeserializer :: Nil)

    dataType match {
      case s: StructType =>
        ReferenceToExpressions(CreateStruct(outputSerializer), resultObj :: Nil)
      case _ =>
        assert(outputSerializer.length == 1)
        outputSerializer.head transform {
          case b: BoundReference => resultObj
        }
    }
  }

  override def toString: String = {
    val input = inputDeserializer match {
      case UnresolvedDeserializer(deserializer, _) => deserializer.dataType.simpleString
      case deserializer => deserializer.dataType.simpleString
    }

    s"$nodeName($input)"
  }

  override def nodeName: String = aggregator.getClass.getSimpleName.stripSuffix("$")
}
