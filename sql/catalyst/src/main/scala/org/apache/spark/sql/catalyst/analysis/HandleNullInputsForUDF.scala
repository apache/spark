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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Expression, If, IsNull, KnownNotNull, Literal, Or, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.SCALA_UDF

/**
 * Correctly handle null primitive inputs for UDF by adding extra [[If]] expression to do the
 * null check.  When user defines a UDF with primitive parameters, there is no way to tell if the
 * primitive parameter is null or not, so here we assume the primitive input is null-propagatable
 * and we should return null if the input is null.
 */
object HandleNullInputsForUDF extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(SCALA_UDF)) {
    case p if !p.resolved => p // Skip unresolved nodes.

    case p => p.transformExpressionsUpWithPruning(_.containsPattern(SCALA_UDF)) {

      case udf: ScalaUDF if udf.inputPrimitives.contains(true) =>
        // Otherwise, add special handling of null for fields that can't accept null.
        // The result of operations like this, when passed null, is generally to return null.
        assert(udf.inputPrimitives.length == udf.children.length)

        val inputPrimitivesPair = udf.inputPrimitives.zip(udf.children)
        val inputNullCheck = inputPrimitivesPair.collect {
          case (isPrimitive, input) if isPrimitive && input.nullable =>
            IsNull(input)
        }.reduceLeftOption[Expression](Or)

        if (inputNullCheck.isDefined) {
          // Once we add an `If` check above the udf, it is safe to mark those checked inputs
          // as null-safe (i.e., wrap with `KnownNotNull`), because the null-returning
          // branch of `If` will be called if any of these checked inputs is null. Thus we can
          // prevent this rule from being applied repeatedly.
          val newInputs = inputPrimitivesPair.map {
            case (isPrimitive, input) =>
              if (isPrimitive && input.nullable) {
                KnownNotNull(input)
              } else {
                input
              }
          }
          val newUDF = udf.copy(children = newInputs)
          If(inputNullCheck.get, Literal.create(null, udf.dataType), newUDF)
        } else {
          udf
        }
    }
  }
}
