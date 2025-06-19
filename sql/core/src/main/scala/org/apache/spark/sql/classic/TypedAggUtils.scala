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

package org.apache.spark.sql.classic

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis.UnresolvedDeserializer
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.agnosticEncoderFor
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression
import org.apache.spark.sql.internal.SQLConf

private[sql] object TypedAggUtils {

  def aggKeyColumn[A](
      encoder: Encoder[A],
      groupingAttributes: Seq[Attribute]): NamedExpression = {
    val agnosticEncoder = agnosticEncoderFor(encoder)
    if (!agnosticEncoder.isStruct) {
      if (SQLConf.get.nameNonStructGroupingKeyAsValue) {
        groupingAttributes.head
      } else {
        Alias(groupingAttributes.head, "key")()
      }
    } else {
      Alias(CreateStruct(groupingAttributes), "key")()
    }
  }

  /**
   * Insert inputs into typed aggregate expressions. For untyped aggregate expressions,
   * the resolving is handled in the analyzer directly.
   */
  private[sql] def withInputType[T <: Expression](
      expr: T,
      inputEncoder: ExpressionEncoder[_],
      inputAttributes: Seq[Attribute]): T = {
    val unresolvedDeserializer = UnresolvedDeserializer(inputEncoder.deserializer, inputAttributes)
    val transformed = expr transform {
      case ta: TypedAggregateExpression if ta.inputDeserializer.isEmpty =>
        ta.withInputInfo(
          deser = unresolvedDeserializer,
          cls = inputEncoder.clsTag.runtimeClass,
          schema = inputEncoder.schema
        )
    }
    transformed.asInstanceOf[T]
  }
}

