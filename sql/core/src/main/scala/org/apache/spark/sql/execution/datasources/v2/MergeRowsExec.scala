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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Instruction, ROW_ID}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class MergeRowsExec(
    isSourceRowPresent: Expression,
    isTargetRowPresent: Expression,
    matchedInstructions: Seq[Instruction],
    notMatchedInstructions: Seq[Instruction],
    notMatchedBySourceInstructions: Seq[Instruction],
    checkCardinality: Boolean,
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {

  @transient override lazy val producedAttributes: AttributeSet = {
    AttributeSet(output.filterNot(attr => inputSet.contains(attr)))
  }

  @transient
  override lazy val references: AttributeSet = {
    val usedExprs = if (checkCardinality) {
      val rowIdAttr = child.output.find(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdAttr.isDefined, "Cannot find row ID attr")
      rowIdAttr.get +: expressions
    } else {
      expressions
    }
    AttributeSet.fromAttributeSets(usedExprs.map(_.references)) -- producedAttributes
  }

  override def simpleString(maxFields: Int): String = {
    s"MergeRowsExec${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val inpputRDD = child.execute()
    val evaluatorFactory = new MergeRowsEvaluatorFactory(
      child.output,
      isSourceRowPresent,
      isTargetRowPresent,
      matchedInstructions,
      notMatchedInstructions,
      notMatchedBySourceInstructions,
      checkCardinality,
      conf.caseSensitiveAnalysis)

    if (conf.usePartitionEvaluator) {
      inpputRDD.mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      inpputRDD.mapPartitionsWithIndex { (index, iter) =>
        evaluatorFactory.createEvaluator().eval(index, iter)
      }
    }
  }
}
