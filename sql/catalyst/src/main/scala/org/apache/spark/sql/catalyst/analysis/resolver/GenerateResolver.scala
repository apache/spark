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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.GeneratorResolution
import org.apache.spark.sql.catalyst.expressions.{Attribute, Generator}
import org.apache.spark.sql.catalyst.plans.logical.Generate

/**
 * Resolves [[Generate]] operator and its generator expression.
 * [[Generate]] is a unique operator that introduces new [[AttributeReference]]s into the plan
 * directly (not via [[Alias]]) and is not a [[LeafNode]]. Because of this, it requires special
 * expression ID handling. See [[resolveGeneratorOutput]] for details.
 */
class GenerateResolver(resolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Generate, Generate] {
  private val scopes: NameScopeStack = resolver.getNameScopes
  private val expressionIdAssigner: ExpressionIdAssigner =
    expressionResolver.getExpressionIdAssigner

  /**
   * Resolution of [[Generate]] operator. The main steps are:
   *  - Resolve the child operator first.
   *  - Resolve inner [[Generator]] as an expression.
   *  - Resolve generator attributes and assign expression IDs. See [[resolveGeneratorOutput]].
   *  - Update the current scope with new generator attributes.
   *
   * @throws SparkException INTERNAL_ERROR if `unresolvedGenerate.unrequiredChildIndex` isn't empty.
   *   It must always be empty during analysis and gets filled only during the optimisation phase.
   *   The only known method to break this invariant is by using
   *   [[org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan.generate]].
   */
  override def resolve(unresolvedGenerate: Generate): Generate = {
    if (unresolvedGenerate.unrequiredChildIndex.nonEmpty) {
      throw SparkException.internalError(
        s"unresolvedGenerate.unrequiredChildIndex must be empty during analysis, " +
          s"but got ${unresolvedGenerate.unrequiredChildIndex}"
      )
    }
    val resolvedChild = resolver.resolve(unresolvedGenerate.child)

    val resolvedGenerator = expressionResolver
      .resolveExpressionTreeInOperator(
        unresolvedExpression = unresolvedGenerate.generator,
        parentOperator = unresolvedGenerate
      )
      .asInstanceOf[Generator]

    val resolvedOutput = resolveGeneratorOutput(unresolvedGenerate, resolvedGenerator)

    val resolvedGenerate = unresolvedGenerate.copy(
      generator = resolvedGenerator,
      generatorOutput = resolvedOutput,
      child = resolvedChild
    )

    scopes.overwriteCurrent(
      output = Some(scopes.current.output ++ resolvedGenerate.qualifiedGeneratorOutput)
    )

    resolvedGenerate
  }

  /**
   * Resolve the generator output attributes and reassign expression IDs if needed.
   *
   * The most important part is expression ID assignment, because [[Generate]] is a unique operator
   * that introduces new [[AttributeReference]]s into the plan directly (not via [[Alias]] as
   * [[Project]] does).
   * Therefore, we should consider 2 cases:
   * - First resolution:
   *   Expression ID generation happens in [[GeneratorResolution.makeGeneratorOutput]] and at that
   *   time they are guaranteed to be unique.
   * - Subsequent resolution with resolved plan reuse (possible in dataframe API):
   *   We should preserve the original expression IDs for left subtree attributes, but re-assign new
   *   IDs for the rest. That's exactly what [[ExpressionIdAssigner.mapExpression]] with
   *   `allowUpdatesForAttributeReferences = true` does.
   *
   *   For example consider self-join:
   *   {{{
   *     val df1 = spark.sql("SELECT col FROM explode(array(1, 2, 3))")
   *     Project [col#0]
   *     +- Generate explode(array(1, 2, 3)), false, [col#0]
   *        +- OneRowRelation
   *
   *     val df2 = df1.select(col("col").as("col2"))
   *     Project [col#0 AS col2#1]
   *     +- Project [col#0]
   *        +- Generate explode(array(1, 2, 3)), false, [col#0]
   *           +- OneRowRelation
   *
   *     df1.join(df2, df1("col") === df2("col2"))
   *     Join Inner, (col#0 = col2#1)* :- Project [col#0]
   *     :  +- Generate explode(array(1, 2, 3)), false, [col#0]
   *     :     +- OneRowRelation
   *     +- Project [col#2 AS col2#1]
   *        +- Project [col#2]
   *           +- Generate explode(array(1, 2, 3)), false, [col#2]
   *              +- OneRowRelation
   *   }}}
   *   It could be seen that we have to reassign expression IDs (col#0 -> col#2) for the right side
   *   of the join. Otherwise, optimizer would consider that col#0 = col2#1 => col#0 = col#0 => true
   *   and it's cartesian product.
   *
   * @param unresolvedGenerate the unresolved [[Generate]] operator
   * @param resolvedGenerator the resolved [[Generator]] expression
   * @return the resolved generator output attributes with assigned expression IDs
   * @see [[ExpressionIdAssigner]] doc for more details on expression ID assignment
   */
  private def resolveGeneratorOutput(
      unresolvedGenerate: Generate,
      resolvedGenerator: Generator): Seq[Attribute] = {
    val generatorOutput = if (unresolvedGenerate.generatorOutput.isEmpty) {
      GeneratorResolution.makeGeneratorOutput(resolvedGenerator, Nil)
    } else {
      unresolvedGenerate.generatorOutput
    }
    generatorOutput.map(
      expressionIdAssigner.mapExpression(_, allowUpdatesForAttributeReferences = true)
    )
  }
}
