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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode

/**
 * A base class for aggregate implementation.
 */
abstract class AggregateExec extends UnaryExecNode {

  def requiredChildDistributionExpressions: Option[Seq[Expression]]
  def groupingExpressions: Seq[NamedExpression]
  def aggregateExpressions: Seq[AggregateExpression]
  def aggregateAttributes: Seq[Attribute]
  def initialInputBufferOffset: Int
  def resultExpressions: Seq[NamedExpression]
  def child: SparkPlan

  // all the mode of aggregate expressions
  protected val modes = aggregateExpressions.map(_.mode).distinct

  protected val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }
}
