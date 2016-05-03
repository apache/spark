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
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._

object TypedAggregateExpression {
  def apply[BUF : Encoder, OUT : Encoder](
      aggregator: Aggregator[_, BUF, OUT]): TypedAggregateExpression = {
    val bufferEncoder = encoderFor[BUF]
    // We will insert the deserializer and function call expression at the bottom of each serializer
    // expression while executing `TypedAggregateExpression`, which means multiply serializer
    // expressions will all evaluate the same sub-expression at bottom.  To avoid the re-evaluating,
    // here we always use one single serializer expression to serialize the buffer object into a
    // single-field row, no matter whether the encoder is flat or not.  We also need to update the
    // deserializer to read in all fields from that single-field row.
    // TODO: remove this trick after we have  better integration of subexpression elimination and
    // whole stage codegen.
    val bufferSerializer = if (bufferEncoder.flat) {
      bufferEncoder.namedExpressions.head
    } else {
      Alias(CreateStruct(bufferEncoder.serializer), "buffer")()
    }

    val bufferDeserializer = if (bufferEncoder.flat) {
      bufferEncoder.deserializer transformUp {
        case b: BoundReference => bufferSerializer.toAttribute
      }
    } else {
      bufferEncoder.deserializer transformUp {
        case UnresolvedAttribute(nameParts) =>
          assert(nameParts.length == 1)
          UnresolvedExtractValue(bufferSerializer.toAttribute, Literal(nameParts.head))
        case BoundReference(ordinal, dt, _) => GetStructField(bufferSerializer.toAttribute, ordinal)
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
    inputDeserializer: Option[Expression],
    bufferSerializer: NamedExpression,
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

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    bufferSerializer.toAttribute.asInstanceOf[AttributeReference] :: Nil

  override lazy val initialValues: Seq[Expression] = {
    val zero = Literal.fromObject(aggregator.zero, bufferExternalType)
    ReferenceToExpressions(bufferSerializer, zero :: Nil) :: Nil
  }

  override lazy val updateExpressions: Seq[Expression] = {
    val reduced = Invoke(
      aggregatorLiteral,
      "reduce",
      bufferExternalType,
      bufferDeserializer :: inputDeserializer.get :: Nil)

    ReferenceToExpressions(bufferSerializer, reduced :: Nil) :: Nil
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

    ReferenceToExpressions(bufferSerializer, merged :: Nil) :: Nil
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
      case Some(UnresolvedDeserializer(deserializer, _)) => deserializer.dataType.simpleString
      case Some(deserializer) => deserializer.dataType.simpleString
      case _ => "unknown"
    }

    s"$nodeName($input)"
  }

  override def nodeName: String = aggregator.getClass.getSimpleName.stripSuffix("$")
}
