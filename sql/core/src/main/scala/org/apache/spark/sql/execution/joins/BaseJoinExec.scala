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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.{BinaryExecNode, ExplainUtils}

/**
 * Holds common logic for join operators
 */
trait BaseJoinExec extends BinaryExecNode {
  def joinType: JoinType
  def condition: Option[Expression]
  def leftKeys: Seq[Expression]
  def rightKeys: Seq[Expression]

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType ($opId)".trim
  }

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) {
      s"${condition.get}"
    } else "None"
    if (leftKeys.nonEmpty || rightKeys.nonEmpty) {
      s"""
         |$formattedNodeName
         |${ExplainUtils.generateFieldString("Left keys", leftKeys)}
         |${ExplainUtils.generateFieldString("Right keys", rightKeys)}
         |${ExplainUtils.generateFieldString("Join type", joinType.toString)}
         |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |${ExplainUtils.generateFieldString("Join type", joinType.toString)}
         |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
         |""".stripMargin
    }
  }
}
