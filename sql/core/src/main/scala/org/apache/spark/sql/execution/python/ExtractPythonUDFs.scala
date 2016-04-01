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

package org.apache.spark.sql.execution.python

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Extracts PythonUDFs from operators, rewriting the query plan so that the UDF can be evaluated
 * alone in a batch.
 *
 * Only extracts the PythonUDFs that could be evaluated in Python (the single child is PythonUDFs
 * or all the children could be evaluated in JVM).
 *
 * This has the limitation that the input to the Python UDF is not allowed include attributes from
 * multiple child operators.
 */
private[spark] object ExtractPythonUDFs extends Rule[LogicalPlan] {

  private def hasPythonUDF(e: Expression): Boolean = {
    e.find(_.isInstanceOf[PythonUDF]).isDefined
  }

  private def canEvaluateInPython(e: PythonUDF): Boolean = {
    e.children match {
      // single PythonUDF child could be chained and evaluated in Python
      case Seq(u: PythonUDF) => canEvaluateInPython(u)
      // Python UDF can't be evaluated directly in JVM
      case children => !children.exists(hasPythonUDF)
    }
  }

  private def collectEvaluatableUDF(expr: Expression): Seq[PythonUDF] = expr match {
    case udf: PythonUDF if canEvaluateInPython(udf) => Seq(udf)
    case e => e.children.flatMap(collectEvaluatableUDF)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // Skip EvaluatePython nodes.
    case plan: EvaluatePython => plan

    case plan: LogicalPlan if plan.resolved =>
      // Extract any PythonUDFs from the current operator.
      val udfs = plan.expressions.flatMap(collectEvaluatableUDF).filter(_.resolved)
      if (udfs.isEmpty) {
        // If there aren't any, we are done.
        plan
      } else {
        val attributeMap = mutable.HashMap[PythonUDF, Expression]()
        // Rewrite the child that has the input required for the UDF
        val newChildren = plan.children.map { child =>
          // Pick the UDF we are going to evaluate
          val validUdfs = udfs.filter { case udf =>
            // Check to make sure that the UDF can be evaluated with only the input of this child.
            udf.references.subsetOf(child.outputSet)
          }
          if (validUdfs.nonEmpty) {
            val evaluation = EvaluatePython(validUdfs, child)
            attributeMap ++= validUdfs.zip(evaluation.resultAttribute)
            evaluation
          } else {
            child
          }
        }
        // Other cases are disallowed as they are ambiguous or would require a cartesian
        // product.
        udfs.filterNot(attributeMap.contains).foreach { udf =>
          if (udf.references.subsetOf(plan.inputSet)) {
            sys.error(s"Invalid PythonUDF $udf, requires attributes from more than one child.")
          } else {
            sys.error(s"Unable to evaluate PythonUDF $udf. Missing input attributes.")
          }
        }

        // Trim away the new UDF value if it was only used for filtering or something.
        logical.Project(
          plan.output,
          plan.transformExpressions {
            case p: PythonUDF if attributeMap.contains(p) => attributeMap(p)
          }.withNewChildren(newChildren))
      }
  }
}
