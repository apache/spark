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

import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.ALIAS
import org.apache.spark.sql.catalyst.util.{toPrettySQL, AUTO_GENERATED_ALIAS}
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
 */
object ReassignAliasNamesWithCollations extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveExpressionsWithPruning(_.containsPattern(ALIAS)) {
      case a: Alias
          if a.resolved &&
          a.metadata.contains(AUTO_GENERATED_ALIAS) &&
          hasNonDefaultCollationInTheSubtree(a.child) =>
        val newName = toPrettySQL(a.child)
        if (newName != a.name) {
          a.withName(newName)
        } else {
          a
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
