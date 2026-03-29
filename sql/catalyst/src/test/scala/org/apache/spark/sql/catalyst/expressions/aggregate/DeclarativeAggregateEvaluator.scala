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
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, JoinedRow, MutableProjection}

/**
 * Evaluator for a [[DeclarativeAggregate]].
 */
case class DeclarativeAggregateEvaluator(function: DeclarativeAggregate, input: Seq[Attribute]) {

  lazy val initializer = MutableProjection.create(function.initialValues)

  lazy val updater = MutableProjection.create(
    function.updateExpressions,
    function.aggBufferAttributes ++ input)

  lazy val merger = MutableProjection.create(
    function.mergeExpressions,
    function.aggBufferAttributes ++ function.inputAggBufferAttributes)

  lazy val evaluator = MutableProjection.create(
    function.evaluateExpression :: Nil,
    function.aggBufferAttributes)

  def initialize(): InternalRow = initializer.apply(InternalRow.empty).copy()

  def update(values: InternalRow*): InternalRow = {
    val joiner = new JoinedRow
    val buffer = values.foldLeft(initialize()) { (buffer, input) =>
      updater(joiner(buffer, input))
    }
    buffer.copy()
  }

  def merge(buffers: InternalRow*): InternalRow = {
    val joiner = new JoinedRow
    val buffer = buffers.foldLeft(initialize()) { (left, right) =>
      merger(joiner(left, right))
    }
    buffer.copy()
  }

  def eval(buffer: InternalRow): InternalRow = evaluator(buffer).copy()
}
