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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.SparkPlan

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
private[spark] object ExtractPythonUDFs extends Rule[SparkPlan] {

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

  def apply(plan: SparkPlan): SparkPlan = plan transformUp {
    case plan: SparkPlan => extract(plan)
  }

  /**
   * Extract all the PythonUDFs from the current operator.
   */
  def extract(plan: SparkPlan): SparkPlan = {
    val udfs = plan.expressions.flatMap(collectEvaluatableUDF)
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
          val resultAttrs = udfs.zipWithIndex.map { case (u, i) =>
            AttributeReference(s"pythonUDF$i", u.dataType)()
          }
          val evaluation = BatchEvalPythonExec(validUdfs, child.output ++ resultAttrs, child)
          attributeMap ++= validUdfs.zip(resultAttrs)
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

      val rewritten = plan.transformExpressions {
        case p: PythonUDF if attributeMap.contains(p) =>
          attributeMap(p)
      }.withNewChildren(newChildren)

      // extract remaining python UDFs recursively
      val newPlan = extract(rewritten)
      if (newPlan.output != plan.output) {
        // Trim away the new UDF value if it was only used for filtering or something.
        execution.ProjectExec(plan.output, newPlan)
      } else {
        newPlan
      }
    }
  }
}
