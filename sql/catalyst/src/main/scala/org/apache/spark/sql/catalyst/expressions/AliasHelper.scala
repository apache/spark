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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project}

/**
 * Helper methods for collecting and replacing aliases.
 */
trait AliasHelper {

  protected def getAliasMap(plan: Project): AttributeMap[Expression] = {
    // Create a map of Aliases to their values from the child projection.
    // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> a + b).
    AttributeMap(plan.projectList.collect { case a: Alias => (a.toAttribute, a.child) })
  }

  protected def getAliasMap(plan: Aggregate): AttributeMap[Expression] = {
    // Find all the aliased expressions in the aggregate list that don't include any actual
    // AggregateExpression or PythonUDF, and create a map from the alias to the expression
    val aliasMap = plan.aggregateExpressions.collect {
      case a: Alias if a.child.find(e => e.isInstanceOf[AggregateExpression] ||
        PythonUDF.isGroupedAggPandasUDF(e)).isEmpty =>
        (a.toAttribute, a.child)
    }
    AttributeMap(aliasMap)
  }

  // Substitute any known alias from a map.
  protected def replaceAlias(
    condition: Expression,
    aliases: AttributeMap[Expression]): Expression = {
    // Use transformUp to prevent infinite recursion when the replacement expression
    // redefines the same ExprId,
    condition.transformUp {
      case a: Attribute =>
        aliases.getOrElse(a, a)
    }
  }
}
