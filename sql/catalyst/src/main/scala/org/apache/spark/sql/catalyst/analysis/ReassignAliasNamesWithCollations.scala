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

import java.util.HashMap

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  Cast,
  Expression,
  ExprId,
  Literal
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{ALIAS, ATTRIBUTE_REFERENCE}
import org.apache.spark.sql.catalyst.util.{toPrettySQL, AUTO_GENERATED_ALIAS}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

/**
 * Reassign [[Alias]] names for expression trees with collations. We need this rule because
 * [[AliasResolution]] cannot easily detect if the expression nodes are properly casted to collated
 * types or not, and sometimes assigns alias names before [[CollationTypeCoercion]] is run.
 *
 * For example, if we didn't have this rule:
 *
 * {{{
 * -- The output alias name is "(collate('a', UTF8_LCASE) < 'A' collate UTF8_LCASE)"
 * SELECT 'a' COLLATE UTF8_LCASE < 'A';
 *
 * -- The output alias name is "concat_ws(a, col1, col1)"
 * SELECT CONCAT_WS('a', col1, col1) FROM VALUES ('a' COLLATE UTF8_LCASE);
 * }}}
 *
 * In the second case literal 'a' does not have "collate" information after it because
 * [[ResolveAliases]] runs before [[AnsiCombinedTypeCoercionRule]].
 *
 * After refenerating [[Alias]] names we have to reassign all the relevant [[AttributeReference]]s
 * in the tree, because those still have the old alias names:
 *
 * {{{
 * -- Reference after the star expansion needs to get new name as well
 * SELECT * FROM (
 *   SELECT CONCAT_WS('a', col1, col1) FROM VALUES ('a' COLLATE UTF8_LCASE);
 * );
 * }}}
 */
object ReassignAliasNamesWithCollations extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.REASSIGN_ALIAS_NAMES_WITH_COLLATIONS)) {
      plan
    } else {
      reassignNames(plan)
    }
  }

  private def reassignNames(plan: LogicalPlan): LogicalPlan = {
    val regeneratedAliasNames = new HashMap[ExprId, String]

    val planWithRegeneratedAliases = plan
      .resolveExpressionsWithPruning(_.containsPattern(ALIAS)) {
        case a: Alias
            if a.resolved &&
            a.metadata.contains(AUTO_GENERATED_ALIAS) &&
            hasNonDefaultCollationInTheSubtree(a.child) =>
          val newName = toPrettySQL(a.child)
          if (newName != a.name) {
            regeneratedAliasNames.put(a.exprId, newName)
            a.withName(newName)
          } else {
            a
          }
      }

    planWithRegeneratedAliases
      .resolveExpressionsWithPruning(_.containsPattern(ATTRIBUTE_REFERENCE)) {
        case attributeReference: AttributeReference =>
          regeneratedAliasNames.get(attributeReference.exprId) match {
            case null => attributeReference
            case newName => attributeReference.withName(newName)
          }
      }
  }

  /**
   * Detect if we have a non-default collation in the subtree under [[Alias]]. We only need to check
   * [[Cast]] and [[Literal]], because only those expressions are affected by
   * [[CollationTypeCoercion]].
   */
  private def hasNonDefaultCollationInTheSubtree(rootExpression: Expression) = {
    rootExpression.exists { expression =>
      expression match {
        case _: Cast | _: Literal =>
          expression.dataType match {
            case stringType: StringType => !stringType.isUTF8BinaryCollation
            case _ => false
          }
        case _ => false
      }
    }
  }
}
