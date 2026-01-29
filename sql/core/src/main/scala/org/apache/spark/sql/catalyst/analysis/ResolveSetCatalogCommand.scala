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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.SetCatalogCommand

/**
 * Replaces unresolved catalog name attributes in SetCatalogCommand
 * with a string literal.
 */
object ResolveSetCatalogCommand extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case cmd @ SetCatalogCommand(expr) =>
      val resolvedExpr = expr match {
        case UnresolvedAttribute(nameParts) =>
          // Convert `SET CATALOG foo` into Literal("foo").
          Literal(nameParts.mkString("."))

        case other =>
          // Other expressions (identifier(), CAST, CONCAT, etc.) are resolved
          // by earlier analysis rules and will be evaluated at runtime.
          other
      }

      cmd.copy(catalogNameExpr = resolvedExpr)
  }
}
