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

import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Extracts PythonUDFs from operators, rewriting the query plan so that the UDF can be evaluated
 * alone in a batch.
 *
 * This has the limitation that the input to the Python UDF is not allowed include attributes from
 * multiple child operators.
 */
private[spark] object ExtractPythonUDFs extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // Skip EvaluatePython nodes.
    case plan: EvaluatePython => plan

    case plan: LogicalPlan if plan.resolved =>
      // Extract any PythonUDFs from the current operator.
      val udfs = plan.expressions.flatMap(_.collect { case udf: PythonUDF => udf })
      if (udfs.isEmpty) {
        // If there aren't any, we are done.
        plan
      } else {
        // Pick the UDF we are going to evaluate (TODO: Support evaluating multiple UDFs at a time)
        // If there is more than one, we will add another evaluation operator in a subsequent pass.
        udfs.find(_.resolved) match {
          case Some(udf) =>
            var evaluation: EvaluatePython = null

            // Rewrite the child that has the input required for the UDF
            val newChildren = plan.children.map { child =>
              // Check to make sure that the UDF can be evaluated with only the input of this child.
              // Other cases are disallowed as they are ambiguous or would require a cartesian
              // product.
              if (udf.references.subsetOf(child.outputSet)) {
                evaluation = EvaluatePython(udf, child)
                evaluation
              } else if (udf.references.intersect(child.outputSet).nonEmpty) {
                sys.error(s"Invalid PythonUDF $udf, requires attributes from more than one child.")
              } else {
                child
              }
            }

            assert(evaluation != null, "Unable to evaluate PythonUDF.  Missing input attributes.")

            // Trim away the new UDF value if it was only used for filtering or something.
            logical.Project(
              plan.output,
              plan.transformExpressions {
                case p: PythonUDF if p.fastEquals(udf) => evaluation.resultAttribute
              }.withNewChildren(newChildren))

          case None =>
            // If there is no Python UDF that is resolved, skip this round.
            plan
        }
      }
  }
}
