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

import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.{ExplainUtils, UnaryExecNode}

/**
 * Holds common logic for aggregate operators
 */
trait BaseAggregateExec extends UnaryExecNode {
  def groupingExpressions: Seq[NamedExpression]
  def aggregateExpressions: Seq[AggregateExpression]
  def aggregateAttributes: Seq[Attribute]
  def resultExpressions: Seq[NamedExpression]

  override def verboseStringWithOperatorId(): String = {
    s"""
       |(${ExplainUtils.getOpId(this)}) $nodeName ${ExplainUtils.getCodegenId(this)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("Keys", groupingExpressions)}
       |${ExplainUtils.generateFieldString("Functions", aggregateExpressions)}
       |${ExplainUtils.generateFieldString("Aggregate Attributes", aggregateAttributes)}
       |${ExplainUtils.generateFieldString("Results", resultExpressions)}
     """.stripMargin
  }
}
