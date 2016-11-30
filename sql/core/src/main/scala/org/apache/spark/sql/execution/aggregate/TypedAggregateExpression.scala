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
  def apply[BUF : Encoder, OUT : Encoder](
      aggregator: Aggregator[_, BUF, OUT]): TypedAggregateExpression = {
    val bufferEncoder = encoderFor[BUF]
    val bufferSerializer = bufferEncoder.namedExpressions
    val bufferDeserializer = UnresolvedDeserializer(
      bufferEncoder.deserializer,
      bufferSerializer.map(_.toAttribute))

    val outputEncoder = encoderFor[OUT]
    val outputType = if (outputEncoder.flat) {
      outputEncoder.schema.head.dataType
    } else {
      outputEncoder.schema
    }

    new TypedAggregateExpression(
      aggregator.asInstanceOf[Aggregator[Any, Any, Any]],
      None,
      None,
      None,
      bufferSerializer,
      bufferDeserializer,
      outputEncoder.serializer,
      outputEncoder.deserializer.dataType,
      outputType,
      !outputEncoder.flat || outputEncoder.schema.head.nullable)
  }
}

/**
 * A helper class to hook [[Aggregator]] into the aggregation system.
 */
case class TypedAggregateExpression(
    aggregator: Aggregator[Any, Any, Any],
    inputDeserializer: Option[Expression],
    inputClass: Option[Class[_]],
    inputSchema: Option[StructType],
    bufferSerializer: Seq[NamedExpression],
    bufferDeserializer: Expression,
    outputSerializer: Seq[Expression],
    outputExternalType: DataType,
    dataType: DataType,
    nullable: Boolean) extends DeclarativeAggregate with NonSQLExpression {

  override def deterministic: Boolean = true

  override def children: Seq[Expression] = inputDeserializer.toSeq :+ bufferDeserializer

  override lazy val resolved: Boolean = inputDeserializer.isDefined && childrenResolved

  override def references: AttributeSet = AttributeSet(inputDeserializer.toSeq)

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
      bufferDeserializer :: inputDeserializer.get :: Nil)

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
        val objRef = outputSerializer.head.find(_.isInstanceOf[BoundReference]).get
        val struct = If(
          IsNull(objRef),
          Literal.create(null, dataType),
          CreateStruct(outputSerializer))
        ReferenceToExpressions(struct, resultObj :: Nil)
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
